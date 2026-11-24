"""Execution-quality guard: spread, quote age, VWAP chase, feed health.

A missing spread is not a zero spread. The filter-time check only blocks a
spread it can measure. The live-quote check runs once the chain is in hand.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

DEFAULT_MAX_SPREAD_PCT = 12.0
STALE_QUOTE_SECONDS = 180.0


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def spread_pct(bid: Any, ask: Any) -> Optional[float]:
    b, a = _num(bid), _num(ask)
    if b is None or a is None or b <= 0 or a <= 0 or a < b:
        return None
    mid = (a + b) / 2.0
    if mid <= 0:
        return None
    return (a - b) / mid * 100.0


def guard_live_quote(
    bid: Any,
    ask: Any,
    *,
    max_spread_pct: Any = DEFAULT_MAX_SPREAD_PCT,
    quote_age_sec: Any = None,
    feed_degraded: bool = False,
) -> Dict[str, Any]:
    """Refuse a live option ticket the book cannot actually fill.

    `ok False` is a block. Unmeasured bid/ask is a block here — this function
    is the last look, after the chain quote, not the early filter.
    """
    ceiling = _num(max_spread_pct)
    if ceiling is None or ceiling <= 0:
        ceiling = DEFAULT_MAX_SPREAD_PCT
    width = spread_pct(bid, ask)
    reasons = []
    if feed_degraded:
        reasons.append("feed_degraded")
    age = _num(quote_age_sec)
    if age is not None and age > STALE_QUOTE_SECONDS:
        reasons.append("quote_stale")
    if width is None:
        reasons.append("spread_unmeasured")
    elif width > ceiling + 1e-9:
        reasons.append("spread_too_wide")
    return {
        "ok": not reasons,
        "spread_pct": round(width, 4) if width is not None else None,
        "max_spread_pct": ceiling,
        "reason_codes": reasons,
        "reason": _reason(reasons, width, ceiling),
    }


def _reason(reasons: list, width: Optional[float], ceiling: float) -> str:
    if not reasons:
        shown = f"{width:.2f}%" if width is not None else "n/a"
        return f"spread {shown} inside {ceiling:g}%"
    if "spread_too_wide" in reasons and width is not None:
        return f"spread {width:.2f}% > {ceiling:g}% ceiling"
    if "spread_unmeasured" in reasons:
        return "option spread unmeasured — no blind entry"
    if "quote_stale" in reasons:
        return "quote is stale"
    if "feed_degraded" in reasons:
        return "market-data feed is degraded"
    return "execution guard"


def chase_blocked(candidate: Dict[str, Any], max_extension_pct: float) -> Optional[str]:
    """Block buying an extension that is already far through VWAP.

    Mean-reversion setups are allowed to be extended — that distance is the
    trade. Missing distance is not a chase.
    """
    setup = str(candidate.get("setup") or "").lower()
    if any(token in setup for token in ("mean", "reversion", "revert", "fade")):
        return None
    dist = _num(candidate.get("underlying_vs_vwap_pct"))
    if dist is None:
        return None
    side = str(candidate.get("exposure") or "long")
    limit = abs(float(max_extension_pct))
    if side == "long" and dist > limit:
        return "chase_extended_above_vwap"
    if side == "short" and dist < -limit:
        return "chase_extended_below_vwap"
    return None
