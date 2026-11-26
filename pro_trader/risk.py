"""Size and exit alignment that can only tighten Tommie's locked rails.

The multiplier lives in [0.35, 1.0] for any trade that is still allowed.
Block is a separate verdict and sends no order. Nothing here raises the
per-trade risk percent, the daily loss limit, or the open-risk ceiling, and
nothing changes the stop sequence:

    fixed STOP  →  breakeven_locked  →  one TRAILING_STOP_CNST
    fixed STOP stays if the native arm fails
    had_native_trail is sticky
    15:40 ET flatten
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

MIN_SIZE_MULTIPLIER = 0.35
MAX_SIZE_MULTIPLIER = 1.0
# Operator per-trade ceiling. 5.99.7 lowered this from 8 so it matches the
# policy ceiling. A policy value above this is haircut back to 5 before
# the regime multiplier.
PER_TRADE_RISK_CEILING_PCT = 5.0
# Echoed, never written. The bot's open-risk gate and the historical day-halt
# discussion both sit on 10. The engine does not move either number.
OPEN_RISK_CEILING_PCT = 10.0
DEFAULT_BREAKEVEN_TRIGGER_R = 0.75
DEFAULT_BREAKEVEN_TRIGGER_PCT = 0.08
# Faster lock in chop / pin. Still a real move, and never later than default.
FASTER_BREAKEVEN_TRIGGER_R = 0.45
FASTER_BREAKEVEN_TRIGGER_PCT = 0.05
TRAIL_SEQUENCE = ("STOP", "breakeven_locked", "TRAILING_STOP_CNST")
FLATTEN_ET = "15:40"


def flatten_et_label(close_minute: Any = None) -> str:
    """Display clock for today's flatten. A 16:00 close stays ``15:40``.

    A shorter session shows 20 minutes before that close (13:00 → ``12:40``).
    Unknown stays the full-day label. This does not move the flatten itself.
    """
    if isinstance(close_minute, bool) or close_minute is None:
        return FLATTEN_ET
    try:
        close = int(close_minute)
    except (TypeError, ValueError):
        return FLATTEN_ET
    if close >= 16 * 60:
        return FLATTEN_ET
    flat = close - 20
    if flat < 0:
        return FLATTEN_ET
    hour, minute = divmod(flat, 60)
    return f"{hour:02d}:{minute:02d}"

HARD_RAILS = {
    "max_size_multiplier": MAX_SIZE_MULTIPLIER,
    "min_size_multiplier": MIN_SIZE_MULTIPLIER,
    "per_trade_risk_ceiling_pct": PER_TRADE_RISK_CEILING_PCT,
    "open_risk_ceiling_pct": OPEN_RISK_CEILING_PCT,
    "engine_may_raise_risk": False,
    "engine_may_raise_daily_loss_limit": False,
    "trail_sequence": list(TRAIL_SEQUENCE),
    "native_trail_price_type": "TRAILING_STOP_CNST",
    "native_trail_fallback": "fixed STOP if the native arm fails",
    "had_native_trail_sticky": True,
    "flatten_et": FLATTEN_ET,
    "reconciler_adopts_covering_trails": True,
}


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def clamp_multiplier(value: Any) -> float:
    """Force a size scale into [0.35, 1.0]. Junk becomes 1.0 (no cut), never an increase."""
    mult = _num(value)
    if mult is None:
        return MAX_SIZE_MULTIPLIER
    if mult > MAX_SIZE_MULTIPLIER:
        return MAX_SIZE_MULTIPLIER
    if mult < MIN_SIZE_MULTIPLIER:
        return MIN_SIZE_MULTIPLIER
    return mult


def risk_scale(
    configured_risk_pct: Any,
    multiplier: Any,
    daily_loss_limit_pct: Any = None,
) -> Dict[str, Any]:
    """Effective per-trade risk after a down-only scale.

    `daily_loss_limit_pct` is returned unchanged so a caller can prove the
    engine did not edit the halt. Effective risk is never above the configured
    percent and never above the per-trade ceiling (5).
    """
    configured = _num(configured_risk_pct)
    if configured is None or configured <= 0:
        configured = PER_TRADE_RISK_CEILING_PCT
    cap_haircut = 1.0 if configured <= PER_TRADE_RISK_CEILING_PCT else (
        PER_TRADE_RISK_CEILING_PCT / configured
    )
    mult = min(MAX_SIZE_MULTIPLIER, clamp_multiplier(multiplier) * cap_haircut)
    effective = configured * mult
    ceiling = min(configured, PER_TRADE_RISK_CEILING_PCT)
    if effective > ceiling + 1e-9:
        mult = ceiling / configured
        effective = ceiling
    return {
        "configured_risk_pct": round(configured, 4),
        "size_multiplier": round(mult, 4),
        "effective_risk_pct": round(effective, 4),
        "per_trade_ceiling_pct": PER_TRADE_RISK_CEILING_PCT,
        "daily_loss_limit_pct": daily_loss_limit_pct,
        "daily_loss_limit_mutated": False,
        "within_rails": effective <= ceiling + 1e-9 and mult <= MAX_SIZE_MULTIPLIER + 1e-9,
    }


def exit_alignment(regime: str, *, breathe: bool) -> Dict[str, Any]:
    """Regime-aware profit lock. Trend days keep the default trigger.

    `breathe True` means the winner runs to the existing 0.75R / 8% breakeven
    and only then promotes to the native trail. Chop and pin lock sooner.
    The sequence and the 15:40 flatten do not change.
    """
    faster = not breathe and regime in {"CHOP_RANGE", "LOW_VOL_PIN", "HIGH_VOL_EXPANSION"}
    return {
        "breakeven_trigger_r": FASTER_BREAKEVEN_TRIGGER_R if faster else DEFAULT_BREAKEVEN_TRIGGER_R,
        "breakeven_trigger_pct": FASTER_BREAKEVEN_TRIGGER_PCT if faster else DEFAULT_BREAKEVEN_TRIGGER_PCT,
        "faster_profit_lock": faster,
        "breathe": not faster,
        "sequence": list(TRAIL_SEQUENCE),
        "flatten_et": FLATTEN_ET,
        "had_native_trail_sticky": True,
        "applied_to_broker_stop": False,
    }


def position_exit_fields(decision: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Fields to stamp on a position.

    The journal blob is always safe to store. The tighter breakeven trigger
    is stamped only in active mode, and only when it is strictly sooner than
    the default — a shadow decision must not move a stop.
    """
    if not isinstance(decision, dict) or decision.get("mode") == "off":
        return {}
    align = decision.get("exit_alignment") or {}
    out: Dict[str, Any] = {}
    if decision.get("mode") == "active" and decision.get("verdict") != "BLOCK":
        trigger_r = _num(align.get("breakeven_trigger_r"))
        trigger_pct = _num(align.get("breakeven_trigger_pct"))
        if trigger_r is not None and trigger_r + 1e-9 < DEFAULT_BREAKEVEN_TRIGGER_R:
            out["pro_trader_breakeven_trigger_r"] = trigger_r
        if trigger_pct is not None and trigger_pct + 1e-9 < DEFAULT_BREAKEVEN_TRIGGER_PCT:
            out["pro_trader_breakeven_trigger_pct"] = trigger_pct
        if align.get("faster_profit_lock") is True:
            out["pro_trader_faster_lock"] = True
    return out
