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
    "list. GET /trades unions that list with realized close events in the "
    "ledger tail (take_profit_closed and other *_closed exits, not "
    "shadow_closed) that the list missed. One close id keeps a single row, "
    "and the history snapshot wins over the sibling exit event. Then it "
    "slices to the count query (default 50, max 500). The account total is "
    "the sum of finite account_pnl_pct. realized_pnl_pct is summed "
    "separately and is not that total."
)

TRADES_LEDGER_INCLUSION_RULE = (
    "Newest-first realized closes inside the newest EDGE_LOOKBACK ledger "
    "lines, including unpriced and unmeasured closes. position_closed is the "
    "booked row. take_profit_closed and other *_closed exit events are "
    "included when no position_closed shares their close id. shadow_closed "
    "is not a live close. The close id is the exit order id, otherwise the "
    "open's signal or open seq, otherwise seq|ts|ticker. A repeated line "
    "collapses; a chain restart that reused seq with a different timestamp "
    "stays. GET /trades uses this set when trades:history is empty, and uses "
    "it to fill gaps when the list is not. It stops after the count query "
    "(default 50, max 500). This block reports the full set before that "
    "slice. The account total is the sum of finite account_pnl_pct."
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


# Exit events that mean a live position was closed. shadow_closed is a paper
# row. partial_stop_fill and flatten_partial_unpriced are slices, not the
# close of the position, and they do not end in _closed.
_OPEN_CLEARED_EVENTS = frozenset({
    "entry_abandoned",
    "stop_timeout_entry_cancelled",
})


def is_realized_close_event(event: Any) -> bool:
    """True for a ledger event that records a live position close.

    position_closed is the booked row. take_profit_closed and the scheduled
    exits (eod_flatten_closed, hold_guard_closed, and the other `{reason}_closed`
    lines) are written even when the fill could not be priced and the booked
    row was skipped. shadow_closed is not a live close.
    """
    name = str(event or "")
    if name == "shadow_closed":
        return False
    if name == "position_closed":
        return True
    return name.endswith("_closed")


def _text_id(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    text = str(value).strip()
    return text or None


def _entry_key(data: Any) -> Optional[float]:
    """Premium or underlying entry, rounded to the ledger's 4 decimals."""
    if not isinstance(data, dict):
        return None
    for key in ("entry_premium", "entry"):
        number = _num(data.get(key))
        if number is not None and number > 0:
            return round(number, 4)
    return None


def _exit_premium(data: Dict[str, Any]) -> Optional[float]:
    """Fill price only. The decision mark is not an exit."""
    for key in ("exit_premium", "exit"):
        number = _num(data.get(key))
        if number is not None and number > 0:
            return number
    return None


def _option_close_pnl(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Premium P&L for an exit event that never reached `_record_close`.

    Same arithmetic as a long option close: (exit - entry) / entry, and
    dollars times the 100 multiplier. A SELL entry flips the sign. A missing
    exit stays missing — the mark and the target are not fills.
    """
    entry = _entry_key(data)
    exit_px = _exit_premium(data)
    try:
        qty = int(data.get("qty"))
    except (TypeError, ValueError):
        qty = 0
    direction = -1.0 if str(data.get("entry_action") or "BUY").upper() == "SELL" else 1.0
    pct = usd = None
    if entry is not None and exit_px is not None:
        pct = round(direction * ((exit_px - entry) / entry) * 100.0, 4)
        if qty > 0:
            usd = round(direction * (exit_px - entry) * 100.0 * qty, 2)
    return {"entry": entry, "exit": exit_px, "pct": pct, "usd": usd}


def _line_close_id(rec: Dict[str, Any], ticker: str) -> str:
    data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
    return f"line:{rec.get('seq')}|{rec.get('ts')}|{ticker}|{data.get('reason') or ''}"


def _same_position_close(pending: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """True when this position_closed books the exit event still pending.

    Matching entries are the same position. Matching reasons are the same
    decision (take_profit, eod_flatten). An exit with no entry links to the
    next booked close of that ticker only until another open arrives — that
    is the reconciler catching the deferred fill. Two different entries do
    not collapse.
    """
    pending_entry = pending.get("entry")
    entry = _entry_key(data)
    if pending_entry is not None and entry is not None:
        return pending_entry == entry
    reason = str(data.get("reason") or "")
    if reason and reason == str(pending.get("reason") or ""):
        return True
    return pending_entry is None


def _row_from_close_event(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Review-row shape for one realized close event.

    A position_closed row is the booked payload, unchanged apart from
    ledger_seq / ledger_ts. An exit event that was never booked contributes
    its own fields and a premium P&L when both premiums are on the line.
    """
    event = str(rec.get("event") or "")
    data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
    row = dict(data)
    row["ledger_seq"] = rec.get("seq")
    row["ledger_ts"] = rec.get("ts")
    row["close_event"] = event
    if event == "position_closed":
        return row
    pnl = _option_close_pnl(data)
    if row.get("entry") is None and pnl["entry"] is not None:
        row["entry"] = pnl["entry"]
    if _num(row.get("exit")) is None and _num(data.get("exit_premium")) is not None:
        row["exit"] = pnl["exit"]
    if _num(row.get("realized_pnl_pct")) is None:
        row["realized_pnl_pct"] = pnl["pct"]
    if _num(row.get("realized_pnl_usd")) is None:
        row["realized_pnl_usd"] = pnl["usd"]
    if not row.get("reason"):
        row["reason"] = event[: -len("_closed")] if event.endswith("_closed") else event
    if not row.get("basis") and (
        _num(data.get("entry_premium")) is not None or _num(data.get("exit_premium")) is not None
    ):
        row["basis"] = data.get("basis") or "premium"
    if not row.get("closed_at"):
        row["closed_at"] = rec.get("ts")
    return row


def build_realized_close_rows(records: Sequence[Any]) -> List[Dict[str, Any]]:
    """One newest-first row per realized close in `records`.

    A take_profit_closed (or other *_closed exit) and the position_closed
    that books the same position share one close id. The later line wins, and
    the booked row is written second, so the position_closed payload is the
    one that stays. An exit with no booked row stays, with P&L only when the
    line carries both premiums.
    """
    cycle: Dict[str, str] = {}
    pending: Dict[str, Dict[str, Any]] = {}
    assigned: List[Tuple[Dict[str, Any], Optional[str]]] = []
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        event = str(rec.get("event") or "")
        data = rec.get("data") if isinstance(rec.get("data"), dict) else {}
        ticker = str(data.get("ticker") or "").upper()
        if event == "position_opened" and ticker:
            signal_id = _text_id(data.get("signal_id"))
            cycle[ticker] = f"signal:{signal_id}" if signal_id else f"open:{rec.get('seq')}|{ticker}"
            pending.pop(ticker, None)
            continue
        if event in _OPEN_CLEARED_EVENTS and ticker:
            cycle.pop(ticker, None)
            pending.pop(ticker, None)
            continue
        if not is_realized_close_event(event) or not ticker:
            continue
        order_id = _text_id(data.get("order_id"))
        if event != "position_closed":
            if order_id:
                close_id = f"order:{order_id}"
            elif ticker in cycle:
                close_id = cycle[ticker]
            else:
                close_id = _line_close_id(rec, ticker)
            reason = event[: -len("_closed")] if event.endswith("_closed") else event
            pending[ticker] = {"id": close_id, "entry": _entry_key(data), "reason": reason}
            cycle.pop(ticker, None)
            assigned.append((rec, close_id))
            continue
        prev = pending.get(ticker)
        close_id = None
        if prev is not None and _same_position_close(prev, data):
            close_id = str(prev["id"])
            pending.pop(ticker, None)
        elif ticker in cycle:
            close_id = cycle[ticker]
        else:
            close_id = _line_close_id(rec, ticker)
        cycle.pop(ticker, None)
        assigned.append((rec, close_id))

    out: List[Dict[str, Any]] = []
    seen = set()
    for rec, close_id in reversed(assigned):
        if close_id is None or close_id in seen:
            continue
        seen.add(close_id)
        row = _row_from_close_event(rec)
        row["close_id"] = close_id
        out.append(row)
    return out


def _newest_key(row: Dict[str, Any]) -> Tuple[float, int]:
    ts = row.get("ledger_ts")
    if ts is None:
        ts = row.get("closed_at")
    epoch = _epoch(ts)
    try:
        seq = int(row.get("ledger_seq"))
    except (TypeError, ValueError):
        seq = -1
    when = epoch if epoch is not None else float("-inf")
    return (-when, -seq)


def _history_close_id(row: Dict[str, Any], ledger_rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    """Close id of a history snapshot, from the ledger row it already shows."""
    seq = row.get("ledger_seq")
    if seq is None:
        seq = row.get("seq")
    matches = [item for item in ledger_rows if item.get("ledger_seq") == seq] if seq is not None else []
    want = row.get("ledger_ts")
    if want is None:
        want = row.get("closed_at")
    if len(matches) == 1 and matches[0].get("close_id"):
        # seq alone is not an identity across a chain restart. A timestamp
        # that disagrees belongs to the other close that reused the number.
        ledger_ts = matches[0].get("ledger_ts")
        same_instant = (
            want is None
            or ledger_ts is None
            or _epoch(want) is None
            or _epoch(want) == _epoch(ledger_ts)
        )
        if same_instant:
            return str(matches[0]["close_id"])
    if matches and want is not None:
        want_text = str(want)
        timed = [
            item for item in matches
            if str(item.get("ledger_ts")) == want_text or str(item.get("closed_at")) == want_text
        ]
        if len(timed) == 1 and timed[0].get("close_id"):
            return str(timed[0]["close_id"])
    order_id = _text_id(row.get("order_id"))
    if order_id:
        for item in ledger_rows:
            if _text_id(item.get("order_id")) == order_id and item.get("close_id"):
                return str(item["close_id"])
        return f"order:{order_id}"
    # signal_id is the open's id. Use it only for a snapshot that has no
    # ledger seq — a seq that is already on the list is that close, and a
    # later trade on the same signal must not be dropped.
    if seq is None:
        signal_id = _text_id(row.get("signal_id"))
        if signal_id:
            wanted = f"signal:{signal_id}"
            for item in ledger_rows:
                if item.get("close_id") == wanted:
                    return wanted
            return wanted
    ident = review_identity(row)
    if ident is None:
        return None
    return f"review:{ident[0]}|{ident[1]}|{ident[2]}"


def merge_trade_review(
    history: Sequence[Any],
    ledger_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """History snapshots first, then ledger closes the list does not already show.

    The history order is kept when every ledger close is already in the list.
    A ledger close that is new is inserted and the union is newest-first.
    """
    ledger = [row for row in ledger_rows or [] if isinstance(row, dict)]
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in dedupe_review_rows(history or []):
        close_id = _history_close_id(row, ledger)
        if close_id is not None:
            if close_id in seen:
                continue
            seen.add(close_id)
        stamped = dict(row)
        if close_id is not None and stamped.get("close_id") is None:
            stamped["close_id"] = close_id
        out.append(stamped)
    added = False
    for row in ledger:
        close_id = row.get("close_id")
        if close_id is not None and close_id in seen:
            continue
        if close_id is not None:
            seen.add(close_id)
        out.append(dict(row))
        added = True
    if added:
        out.sort(key=_newest_key)
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
    buckets: Dict[str, List[Dict[str, Any]]] = split["buckets"]
    # GET /trades reads every realized close, not only position_closed. The
    # report and learning stay on position_closed: an unbooked exit never
    # reached the daily counter.
    ledger_rows = build_realized_close_rows(apply_sign_corrections(records))
    orphan_rows = [
        row for row in ledger_rows
        if str(row.get("close_event") or "") not in {"", "position_closed"}
    ]
    history = dedupe_review_rows(history_rows or [])
    served_from = "history" if history_read and history else "ledger"
    if served_from == "history":
        served_rows = merge_trade_review(history, ledger_rows)
        served_rule = TRADES_HISTORY_INCLUSION_RULE
    else:
        served_rows = ledger_rows
        served_rule = TRADES_LEDGER_INCLUSION_RULE

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
        _cause(
            "exit_event_without_position_closed",
            orphan_rows,
            "take_profit_closed or another *_closed exit with no position_closed "
            "for the same close id. GET /trades includes it. The report and "
            "learning do not: the deferred path never booked realized P&L.",
            {"pro_trader_report": 0, "learning": 0, "trades": len(orphan_rows)},
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
                "GET /trades unions the review list with realized closes in "
                "that same tail, then slices to the count query "
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
                "is every realized close in the shared window, one row per close "
                "id. GET /trades "
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
                len(buckets["price_unknown"])
                + len(buckets["unmeasured_realized_pnl"])
                + len(orphan_rows)
            ),
        },
    }
