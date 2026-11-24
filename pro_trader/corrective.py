"""Machine-readable corrective actions for the Pro Day Trader assistant.

Ranked proposals only. Nothing here places an order or edits a rail.
The assistant reads `GET /pro-trader/corrective-actions` and may open a
Cursor PR against this repo. Each item names the evidence and the file
or setting it would touch.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

DISPATCH = {
    "consumer": "pro_day_trader",
    "repo": "tommie58/E-Trade-trading",
    "action": "cursor_corrective_pr",
    "authority": "corrective-action",
}

_SEVERITY = {"critical": 0, "high": 1, "medium": 2, "low": 3}

DO_NOT_TOUCH = [
    "RISK_PER_TRADE_PCT above 8",
    "DAILY_LOSS_LIMIT_PCT (do not raise)",
    "APP_MAX_OPEN_RISK_PCT 10",
    "STOP until breakeven_locked",
    "TRAILING_STOP_CNST promotion",
    "had_native_trail",
    "15:40 ET flatten",
    "DEGRADED_DATA fail-safe",
    "spread ceiling",
]


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and abs(out) != float("inf") else None


def build_corrective_actions(
    decisions: Iterable[Dict[str, Any]],
    closes: Iterable[Dict[str, Any]] = (),
    *,
    pacing: Optional[Dict[str, Any]] = None,
    feed: Optional[Dict[str, Any]] = None,
    min_trades: int = 4,
) -> List[Dict[str, Any]]:
    rows = [d for d in decisions if isinstance(d, dict)]
    close_rows = [c for c in closes if isinstance(c, dict)]
    pace = pacing or {}
    done = int(pace.get("trades_completed") if pace.get("trades_completed") is not None else _counted_fills(rows))
    target = int(pace.get("min_trades_per_day") or min_trades or 4)
    actions: List[Dict[str, Any]] = []

    if done < target and (pace.get("behind") or pace.get("state") == "behind" or _late(pace)):
        actions.append(_action(
            "trades-below-min-per-day",
            "high",
            ["trades_below_min_per_day", "pacing_" + str(pace.get("state") or "short")],
            {
                "trades_completed": done,
                "min_trades_per_day": target,
                "shortfall": max(0, target - done),
                "checkpoint": pace.get("checkpoint"),
                "session_progress": pace.get("session_progress"),
            },
            "config",
            "MIN_TRADES_PER_DAY",
            (
                f"Session is at {done}/{target} trades"
                + (f" by the {pace.get('checkpoint')} checkpoint" if pace.get("checkpoint") else "")
                + ". Pacing already lowers the score floor to verified B+ (75) inside the "
                "regime rotation. Do not lower it further and do not add a morning shutoff. "
                "If this repeats, check whether setups are arriving outside the rotation "
                "tokens in pro_trader/playbook.py."
            ),
        ))

    feed_codes = _feed_codes(rows, feed)
    if feed_codes:
        actions.append(_action(
            "feed-health-failure",
            "critical",
            feed_codes,
            {"feed": feed, "decisions": sum(1 for d in rows if d.get("regime") == "DEGRADED_DATA")},
            "config",
            "MASSIVE_API_KEY",
            (
                "api.massive.com snapshot feed returned a failure"
                + (" (HTTP 403)" if "massive_http_403" in feed_codes else "")
                + ". DEGRADED_DATA must keep blocking new entries. Fix the key or plan; "
                "do not disable the fail-safe to manufacture the trade quota."
            ),
        ))

    spread_blocks = [d for d in rows if "spread_too_wide" in (d.get("reason_codes") or [])]
    if len(spread_blocks) >= 2:
        actions.append(_action(
            "spread-slippage-leak",
            "high",
            ["spread_too_wide"],
            {"count": len(spread_blocks), "sample": [d.get("ticker") for d in spread_blocks[:5]]},
            "code",
            "pro_trader/playbook.py REGIME_META max_spread_pct",
            (
                f"{len(spread_blocks)} candidates died on the spread ceiling. "
                "That ceiling is a hard rail — tighten contract selection (tighter markets, "
                "higher premium) rather than raising the cap."
            ),
        ))

    for leak in _mae_mfe_leaks(close_rows):
        actions.append(leak)

    by_regime_blocks: Dict[str, int] = {}
    for row in rows:
        if row.get("verdict") == "BLOCK":
            regime = str(row.get("regime") or "UNKNOWN")
            by_regime_blocks[regime] = by_regime_blocks.get(regime, 0) + 1
    for regime, count in sorted(by_regime_blocks.items(), key=lambda kv: -kv[1]):
        if count < 3 or regime in {"DEGRADED_DATA", "EVENT_RISK"}:
            continue
        actions.append(_action(
            f"regime-block-concentration-{regime.lower()}",
            "medium",
            ["regime_block_concentration", regime],
            {"regime": regime, "blocks": count},
            "code",
            "pro_trader/playbook.py ROTATION",
            (
                f"{regime} blocked {count} candidates. Confirm the rotation still names the "
                "setups the scanner actually emits. Pacing may only admit B+ names already "
                "in that rotation."
            ),
        ))

    if not actions:
        actions.append(_action(
            "no-corrective-action",
            "low",
            ["within_rails"],
            {"trades_completed": done, "min_trades_per_day": target},
            "none",
            None,
            "No leak cleared the bar for a corrective PR. Keep shadow on until the quota and the rails both look right.",
        ))

    actions.sort(key=lambda item: _SEVERITY.get(str(item.get("severity")), 9))
    for index, item in enumerate(actions, start=1):
        item["rank"] = index
    return actions


def _action(ident, severity, reason_codes, evidence, kind, target, summary) -> Dict[str, Any]:
    return {
        "id": ident,
        "severity": severity,
        "reason_codes": reason_codes,
        "evidence": evidence,
        "proposal": {
            "kind": kind,
            "target": target,
            "summary": summary,
            "do_not_touch": list(DO_NOT_TOUCH),
        },
        "dispatch": dict(DISPATCH),
    }


def _counted_fills(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("verdict") in {"ALLOW", "REDUCE"} and row.get("accepted_by_filters") is not False)


def _late(pace: Dict[str, Any]) -> bool:
    progress = _num(pace.get("session_progress"))
    return progress is not None and progress >= 0.75


def _feed_codes(rows: List[Dict[str, Any]], feed: Optional[Dict[str, Any]]) -> List[str]:
    codes: List[str] = []
    if isinstance(feed, dict):
        status = feed.get("http_status")
        err = str(feed.get("last_error") or "")
        if status == 403 or "403" in err:
            codes.append("massive_http_403")
        elif status == 401 or "401" in err:
            codes.append("massive_http_401")
        elif feed.get("ok") is False:
            codes.append("feed_unhealthy")
    if any(row.get("regime") == "DEGRADED_DATA" for row in rows) and "feed_unhealthy" not in codes and "massive_http_403" not in codes:
        codes.append("degraded_data")
    return codes


def _mae_mfe_leaks(closes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Regime buckets where the worst drop ate a real favorable excursion."""
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for close in closes:
        meta = close.get("pro_trader") if isinstance(close.get("pro_trader"), dict) else {}
        regime = str(meta.get("regime") or "UNKNOWN")
        mae = _num(close.get("trough_mae_pct"))
        mfe = _num(close.get("peak_mfe_pct"))
        if mae is None or mfe is None:
            continue
        if mfe >= 0.08 and mae >= mfe:
            buckets.setdefault(regime, []).append(close)
    out = []
    for regime, group in buckets.items():
        if len(group) < 2:
            continue
        out.append(_action(
            f"mae-mfe-leak-{regime.lower()}",
            "high",
            ["mae_exceeds_mfe", regime],
            {
                "regime": regime,
                "trades": len(group),
                "tickers": [c.get("ticker") for c in group[:5]],
            },
            "code",
            "pro_trader/risk.py exit_alignment",
            (
                f"{len(group)} {regime} closes gave back the whole favorable excursion "
                "(MAE >= MFE after a +8% run). The stop sequence stays "
                "STOP → breakeven_locked → TRAILING_STOP_CNST. A corrective PR may only "
                "lock sooner inside exit_alignment, never loosen the hard stop or skip the native trail."
            ),
        ))
    return out
