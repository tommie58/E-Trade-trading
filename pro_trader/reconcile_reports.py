"""Read-only reconciliation of the three close reports.

GET /pro-trader/report, GET /trades, and GET /learning/report have never
counted the same set. Some of that is the definition of each report. Some of
it was a bug: the pro-trader report scanned a shorter raw tail and summed
unpriced closes. This module states both, and it is the inclusion rule the
report now uses.

Nothing here writes the ledger, a counter, or an order.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Booked fields a historical sign-correction record is allowed to replace.
# Mirrors main_bot.PNL_SIGN_FIELDS. The chain itself is never rewritten.
SIGN_FIELDS = ("realized_pnl_pct", "account_pnl_pct", "realized_pnl_usd")

REPORT_INCLUSION_RULE = (
    "Newest EDGE_LOOKBACK ledger records, sign-corrected in memory (the chain "
    "is not rewritten). Event position_closed only. Excludes price_unknown, "
    "exit_unpriced, and needs_reconcile_trueup with no exit. Excludes closes "
    "with no finite realized_pnl_pct, pnl_pct, or realized_pnl (unmeasured, "
    "not zero). An alias-only percent is summed here and is not a learning "
    "close. Collapses a "
    "repeated identity seq|ts|ticker to one row; seq alone is not unique "
    "across a chain restart. Sums realized_pnl_pct. Does not filter by "
    "calendar day. Does not include shadow_closed, partial_stop_fill, or "
    "flatten_partial_unpriced. A live fill taken while the desk was in shadow "
    "is included, because it is a real position_closed."
)

LEARNING_INCLUSION_RULE = (
    "Same newest EDGE_LOOKBACK sign-corrected tail as the report. Live "
    "position_closed rows with a known price, a finite realized_pnl_pct, and "
    "a parseable record timestamp. price_unknown, exit_unpriced, "
    "needs_reconcile_trueup with no exit, flatten_partial_unpriced, "
    "partial_stop_fill, and shadow_closed are excluded. A repeated "
    "seq|ts|ticker is counted again: the learning replay that sizes entries "
    "walks every qualifying event, and this report does not change that "
    "replay. The published figure is live_closes (a count). This block also "
    "sums realized_pnl_pct of those closes. Learning does not publish "
    "account_pnl_pct."
)

TRADES_HISTORY_INCLUSION_RULE = (
    "Redis or memory review list trades:history, newest first, capped at "
    "TRADES_HISTORY_MAX. Each row is the close snapshot stored when the "
    "position closed. Includes unpriced and unmeasured closes. The push "
    "dedupes by ledger_seq only; the read collapses a repeated "
    "seq|ts|ticker and keeps two rows that reused seq with different "
    "timestamps. Sign corrections appended later are not rewritten into the "
    "list. GET /trades then slices to the count query (default 50, max 500). "
    "The account total is the sum of finite account_pnl_pct. realized_pnl_pct "
    "is summed separately and is not that total."
)

TRADES_LEDGER_INCLUSION_RULE = (
    "Used when trades:history is empty. Newest-first position_closed rows "
    "inside the newest EDGE_LOOKBACK ledger lines, including unpriced and "
    "unmeasured closes. Identity seq|ledger_ts|ticker collapses a repeated "
    "line and keeps a chain restart that reused seq with a different "
    "timestamp. GET /trades stops after the count query (default 50, max 500). "
    "This block reports the full set before that slice. The account total is "
    "the sum of finite account_pnl_pct."
)


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or abs(number) == float("inf"):
        return None
    return number


def _epoch(value: Any) -> Optional[float]:
    """Same instant parse the learning replay uses for a ledger timestamp."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if number == number and abs(number) != float("inf") else None
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def price_unknown(data: Any) -> bool:
    """True when a close has no broker price yet.

    A missing premium is not a zero fill. Those rows stay out of P&L until
    the reconciler trues them up. The close reason is never a lookup key.
    """
    if not isinstance(data, dict):
        return False
    if data.get("price_unknown") is True or data.get("exit_unpriced") is True:
        return True
    if data.get("needs_reconcile_trueup") is True and data.get("exit") is None:
        return True
    return False


def close_identity(seq: Any, ts: Any, ticker: Any) -> Optional[Tuple[Any, str, str]]:
    """Identity of one close. seq alone is not unique: a chain restart reuses it."""
    if seq is None or isinstance(seq, (dict, list, bool)):
        return None
    return (seq, str(ts), str(ticker or "").upper())


def review_identity(row: Any) -> Optional[Tuple[Any, str, str]]:
    """Identity of a GET /trades row (history snapshot or ledger fallback)."""
    if not isinstance(row, dict):
        return None
    seq = row.get("ledger_seq")
    if seq is None:
        seq = row.get("seq")
    if row.get("ledger_ts") is not None:
        ts = row.get("ledger_ts")
    elif row.get("ts") is not None and "closed_at" not in row:
        ts = row.get("ts")
    else:
        ts = row.get("closed_at")
        if ts is None:
            ts = row.get("ts")
    return close_identity(seq, ts, row.get("ticker"))


def dedupe_review_rows(rows: Iterable[Any]) -> List[Dict[str, Any]]:
    """Keep the first copy of each seq|ts|ticker. Rows with no seq stay."""
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ident = review_identity(row)
        if ident is not None:
            if ident in seen:
                continue
            seen.add(ident)
        out.append(row)
    return out


def _correction_key(seq: Any, ts: Any, ticker: Any) -> str:
    return f"{seq}|{ts}|{str(ticker or '').upper()}"


def apply_sign_corrections(records: Sequence[Any]) -> List[dict]:
    """Overlay pnl_sign_corrected targets. Does not mutate the input records.

    A second pass over an already-corrected tail is a no-op: those correction
    events were removed and the fields already carry the corrected numbers.
    """
    fixes: Dict[str, Dict[str, Any]] = {}
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        if str(rec.get("event") or "") != "pnl_sign_corrected":
            continue
        data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
        for target in data.get("targets") or []:
            if isinstance(target, dict) and isinstance(target.get("now"), dict):
                fixes[_correction_key(target.get("seq"), target.get("ts"), target.get("ticker"))] = target
    out: List[dict] = []
    for rec in records or []:
        if not isinstance(rec, dict):
            out.append(rec)  # type: ignore[arg-type]
            continue
        event = str(rec.get("event") or "")
        if event == "pnl_sign_corrected":
            continue
        if event == "position_closed" and fixes:
            data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
            fix = fixes.get(_correction_key(rec.get("seq"), rec.get("ts"), data.get("ticker")))
            if fix is not None:
                patched = dict(data)
                for field, value in fix["now"].items():
                    if field in SIGN_FIELDS:
                        patched[field] = value
                if fix.get("entry_action") in {"BUY", "SELL"}:
                    patched["entry_action"] = fix["entry_action"]
                patched["sign_corrected"] = True
                out.append({**rec, "data": patched})
                continue
        out.append(rec)
    return out


def _realized(row: Dict[str, Any]) -> Optional[float]:
    for key in ("realized_pnl_pct", "pnl_pct", "realized_pnl"):
        number = _num(row.get(key))
        if number is not None:
            return number
    return None


def _totals(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    realized = 0.0
    account = 0.0
    realized_n = 0
    account_n = 0
    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        count += 1
        pnl = _realized(row)
        if pnl is not None:
            realized += pnl
            realized_n += 1
        acct = _num(row.get("account_pnl_pct"))
        if acct is not None:
            account += acct
            account_n += 1
    return {
        "count": count,
        "realized_pnl_pct_sum": round(realized, 4),
        "realized_measured": realized_n,
        "account_pnl_pct_sum": round(account, 4),
        "account_measured": account_n,
    }


def _source(rows: List[Dict[str, Any]], rule: str, **extra: Any) -> Dict[str, Any]:
    body = _totals(rows)
    body["inclusion_rule"] = rule
    body.update(extra)
    return body


def _cause(
    cause: str,
    rows: List[Dict[str, Any]],
    detail: str,
    counts: Dict[str, int],
) -> Dict[str, Any]:
    totals = _totals(rows)
    return {
        "cause": cause,
        "count": len(rows),
        "counts": counts,
        "realized_pnl_pct_sum": totals["realized_pnl_pct_sum"],
        "account_pnl_pct_sum": totals["account_pnl_pct_sum"],
        "detail": detail,
    }


def report_closes(records: Sequence[Any]) -> List[Dict[str, Any]]:
    """Close payloads that enter the pro-trader report's realized sum."""
    return _split(records)["report_rows"]


def _split(records: Sequence[Any]) -> Dict[str, Any]:
    corrected = apply_sign_corrections(records)
    report_rows: List[Dict[str, Any]] = []
    learning_rows: List[Dict[str, Any]] = []
    ledger_rows: List[Dict[str, Any]] = []
    seen = set()
    buckets: Dict[str, List[Dict[str, Any]]] = {
        "price_unknown": [],
        "unmeasured_realized_pnl": [],
        "alias_pnl_field": [],
        "missing_timestamp": [],
        "duplicate_identity": [],
        "partial_stop_fill": [],
        "shadow_closed": [],
        "flatten_partial_unpriced": [],
    }
    sign_corrected = 0
    for rec in corrected:
        if not isinstance(rec, dict):
            continue
        event = str(rec.get("event") or "")
        data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
        if event == "partial_stop_fill":
            buckets["partial_stop_fill"].append(data)
            continue
        if event == "shadow_closed":
            buckets["shadow_closed"].append(data)
            continue
        if event == "flatten_partial_unpriced":
            buckets["flatten_partial_unpriced"].append(data)
            continue
        if event != "position_closed":
            continue
        if data.get("sign_corrected") is True:
            sign_corrected += 1
        ident = close_identity(rec.get("seq"), rec.get("ts"), data.get("ticker"))
        is_dup = ident is not None and ident in seen
        if ident is not None:
            seen.add(ident)
        snapshot = dict(data)
        snapshot["ledger_seq"] = rec.get("seq")
        snapshot["ledger_ts"] = rec.get("ts")
        unknown = price_unknown(data)
        primary = _num(data.get("realized_pnl_pct"))
        fallback = _realized(data)
        has_ts = _epoch(rec.get("ts")) is not None
        # Learning reads realized_pnl_pct only. The report also accepts the
        # pnl_pct / realized_pnl aliases build_report already summed.
        if is_dup:
            buckets["duplicate_identity"].append(snapshot)
            if (not unknown) and primary is not None and has_ts:
                learning_rows.append(snapshot)
            continue
        ledger_rows.append(snapshot)
        if unknown:
            buckets["price_unknown"].append(snapshot)
            continue
        if fallback is None:
            buckets["unmeasured_realized_pnl"].append(snapshot)
            continue
        if primary is None:
            buckets["alias_pnl_field"].append(snapshot)
            report_rows.append(snapshot)
            continue
        report_rows.append(snapshot)
        if has_ts:
            learning_rows.append(snapshot)
        else:
            buckets["missing_timestamp"].append(snapshot)
    return {
        "report_rows": report_rows,
        "learning_rows": learning_rows,
        "ledger_rows": ledger_rows,
        "buckets": buckets,
        "sign_corrected": sign_corrected,
    }


def reconcile_report_sources(
    records: Sequence[Any],
    history_rows: Optional[Sequence[Any]] = None,
    *,
    history_read: bool = False,
    window: int = 3000,
) -> Dict[str, Any]:
    """Counts, totals, inclusion rules, and the difference by cause.

    `records` is the ledger tail the report and learning share (already
    limited to the window). Sign corrections inside that tail are applied
    here. `history_rows` is the review list GET /trades serves when it is
    non-empty. Pass history_read=True when that list was actually read,
    including when the read returned nothing.
    """
    split = _split(records)
    report_rows: List[Dict[str, Any]] = split["report_rows"]
    learning_rows: List[Dict[str, Any]] = split["learning_rows"]
    ledger_rows: List[Dict[str, Any]] = split["ledger_rows"]
    buckets: Dict[str, List[Dict[str, Any]]] = split["buckets"]
    history = dedupe_review_rows(history_rows or [])
    served_from = "history" if history_read and history else "ledger"
    served_rows = history if served_from == "history" else ledger_rows
    served_rule = TRADES_HISTORY_INCLUSION_RULE if served_from == "history" else TRADES_LEDGER_INCLUSION_RULE

    report = _source(report_rows, REPORT_INCLUSION_RULE, window=int(window))
    learning = _source(
        learning_rows,
        LEARNING_INCLUSION_RULE,
        window=int(window),
        published_field="live_closes",
    )
    # Learning does not publish an account total. Keep the sum so the block
    # can show it, and say it is not a published figure.
    learning["account_pnl_published"] = False
    trades = _source(
        served_rows,
        served_rule,
        served_from=served_from,
        http_count_default=50,
        http_count_max=500,
        history=_source(history, TRADES_HISTORY_INCLUSION_RULE) if history_read else None,
        ledger=_source(ledger_rows, TRADES_LEDGER_INCLUSION_RULE, window=int(window)),
    )

    dup_in_learning = sum(
        1 for row in buckets["duplicate_identity"]
        if not price_unknown(row)
        and _num(row.get("realized_pnl_pct")) is not None
        and _epoch(row.get("ledger_ts")) is not None
    )
    missing_ts = len(buckets["missing_timestamp"])
    alias_only = len(buckets["alias_pnl_field"])
    learning_minus_report = learning["count"] - report["count"]
    difference = learning_minus_report - (dup_in_learning - missing_ts - alias_only)

    causes = [
        _cause(
            "price_unknown",
            buckets["price_unknown"],
            "Unpriced closes (price_unknown, exit_unpriced, or needs_reconcile_trueup "
            "with no exit) stay in the trades list and out of the report and learning "
            "P&L until a fill price exists. Counting one as a realized loss was a bug.",
            {"pro_trader_report": 0, "learning": 0, "trades": len(buckets["price_unknown"])},
        ),
        _cause(
            "unmeasured_realized_pnl",
            buckets["unmeasured_realized_pnl"],
            "position_closed with no finite realized_pnl_pct. The report and learning "
            "omit it. It is not treated as zero. GET /trades still returns the row.",
            {"pro_trader_report": 0, "learning": 0, "trades": len(buckets["unmeasured_realized_pnl"])},
        ),
        _cause(
            "alias_pnl_field",
            buckets["alias_pnl_field"],
            "Close has pnl_pct or realized_pnl and no realized_pnl_pct. The report "
            "sums it, because that is what build_report already did. Learning reads "
            "only realized_pnl_pct, so the row is not a live close.",
            {"pro_trader_report": alias_only, "learning": 0, "trades": alias_only},
        ),
        _cause(
            "missing_timestamp",
            buckets["missing_timestamp"],
            "A priced close whose ledger timestamp does not parse. The report still "
            "sums it. Learning drops it because the replay needs a clock. This is a "
            "definition, not a dropped fill.",
            {
                "pro_trader_report": missing_ts,
                "learning": 0,
                "trades": missing_ts,
            },
        ),
        _cause(
            "duplicate_identity",
            buckets["duplicate_identity"],
            "A repeated seq|ts|ticker. The report and the trades ledger fallback keep "
            "one copy. Learning counts every qualifying copy, because that replay also "
            "sizes entries and this change does not touch it.",
            {
                "pro_trader_report": 0,
                "learning": dup_in_learning,
                "trades": 0,
            },
        ),
        _cause(
            "partial_stop_fill",
            buckets["partial_stop_fill"],
            "A partial protective-stop fill books account percent onto the daily "
            "counter, but it is not a position_closed. None of the three close "
            "counts include it.",
            {"pro_trader_report": 0, "learning": 0, "trades": 0},
        ),
        _cause(
            "shadow_closed",
            buckets["shadow_closed"],
            "Paper shadow_closed rows are not live closes. None of the three "
            "endpoints count them. A real order placed while the desk was in "
            "shadow is a position_closed and is counted with the live fills.",
            {"pro_trader_report": 0, "learning": 0, "trades": 0},
        ),
        _cause(
            "flatten_partial_unpriced",
            buckets["flatten_partial_unpriced"],
            "An unpriced flatten slice is its own event, not a position_closed. "
            "It is excluded from all three counts until it is trued up.",
            {"pro_trader_report": 0, "learning": 0, "trades": 0},
        ),
        {
            "cause": "account_vs_realized",
            "count": 0,
            "counts": {
                "pro_trader_report": report["count"],
                "learning": learning["count"],
                "trades": trades["count"],
            },
            "realized_pnl_pct_sum": report["realized_pnl_pct_sum"],
            "account_pnl_pct_sum": trades["account_pnl_pct_sum"],
            "delta_account_minus_realized": round(
                float(trades["account_pnl_pct_sum"]) - float(report["realized_pnl_pct_sum"]), 4,
            ),
            "detail": (
                "The report total is the sum of realized_pnl_pct, the per-trade "
                "move on the close's own basis (premium or underlying). The trades "
                "total the audit compared is the sum of account_pnl_pct, the "
                "account-equity percent booked to the daily loss counter. They are "
                "different units, and the sets behind them differ by the causes "
                "above, so the delta is not a pure basis conversion."
            ),
        },
        {
            "cause": "window",
            "count": 0,
            "counts": {
                "pro_trader_report": report["count"],
                "learning": learning["count"],
                "trades_ledger": len(ledger_rows),
                "trades_history": len(history) if history_read else None,
            },
            "detail": (
                f"The report and learning both read the newest {int(window)} "
                "sign-corrected ledger records (EDGE_LOOKBACK), not a calendar day. "
                "GET /trades serves the review list when it is non-empty and "
                "otherwise that same tail, then slices to the count query "
                "(default 50, max 500). The report previously scanned only the "
                "newest 1200 raw records and ignored sign corrections. That shorter "
                "raw window was a bug. It is closed."
            ),
        },
        {
            "cause": "trades_history_vs_ledger",
            "count": abs(len(history) - len(ledger_rows)) if history_read else 0,
            "counts": {
                "history": len(history) if history_read else None,
                "ledger": len(ledger_rows),
                "served_from": served_from,
            },
            "detail": (
                "The review list is the snapshots pushed at close time, capped "
                "and not rewritten when a sign correction lands. The ledger rule "
                "is every position_closed in the shared window. GET /trades "
                f"served this read from {served_from}. "
                + (
                    "The history list was not readable, so the ledger rule is the trades source."
                    if not history_read
                    else "Both counts are before the HTTP count slice."
                )
            ),
        },
        {
            "cause": "trades_count_query",
            "count": 0,
            "counts": {"http_count_default": 50, "http_count_max": 500},
            "detail": (
                "GET /trades returns the newest min(count, source) rows. The "
                "default count is 50 and the maximum is 500. Counts in this "
                "block are the full source before that slice."
            ),
        },
        {
            "cause": "sign_correction",
            "count": int(split["sign_corrected"]),
            "counts": {"sign_corrected_closes": int(split["sign_corrected"])},
            "detail": (
                "pnl_sign_corrected records overlay realized, account, and dollar "
                "fields in memory. The report and learning both apply them. The "
                "review list keeps the numbers stored at close time. Recorded "
                "ledger lines are not modified."
            ),
        },
    ]
    return {
        "sources": {
            "pro_trader_report": report,
            "trades": trades,
            "learning": learning,
        },
        "difference_by_cause": causes,
        "reconciled_difference": {
            "learning_minus_report": learning_minus_report,
            "duplicate_identity_in_learning": dup_in_learning,
            "missing_timestamp_in_report_only": missing_ts,
            "alias_pnl_field_in_report_only": alias_only,
            "checks_out": difference == 0,
            "trades_ledger_minus_report": len(ledger_rows) - report["count"],
            "trades_ledger_explained_by_unpriced_or_unmeasured": (
                len(buckets["price_unknown"]) + len(buckets["unmeasured_realized_pnl"])
            ),
        },
    }
