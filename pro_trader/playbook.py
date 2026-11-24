"""Multi-playbook rotation. Each regime keeps more than one way to get paid.

Trend days: continuation, VWAP pullback, opening-range drive.
Chop: mean reversion, range-extreme VWAP fade, relative-strength leaders.
High vol: a smaller impulse. Pin: a tight-target scalp or a quick lock.

Pacing may admit a verified B+ from that same rotation. It cannot admit a
setup the regime does not trade, a wide spread, a dead feed, a late 0DTE,
or a day that is already through the loss halt.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pro_trader.execution import chase_blocked, spread_pct
from pro_trader.pacing import BPLUS_RVOL, BPLUS_SCORE, paced_score_floor
from pro_trader.risk import exit_alignment

APLUS_SCORE = 90.0
APLUS_RVOL = 1.6

# Blocks pacing is not allowed to lift.
NON_OVERRIDABLE = frozenset({
    "degraded_data",
    "event_risk",
    "spread_too_wide",
    "quote_stale",
    "daily_loss_limit",
    "zero_dte_late_decay",
    "score_unmeasured",
    "rvol_unmeasured",
    "fade_needs_aplus",
    "setup_outside_rotation",
})

# role primary|secondary is descriptive. All of these are in the rotation
# pacing may draw from. `pace_eligible` false means a trend-fade only.
ROTATION = {
    "TREND_UP": (
        {"id": "trend_continuation", "setups": ("breakout", "momentum", "flag", "trend"),
         "min_score": 78.0, "size": 1.0, "chase": 1.25},
        {"id": "vwap_pullback", "setups": ("vwap pullback", "vwap reclaim", "pullback", "reclaim"),
         "min_score": 76.0, "size": 0.85, "chase": 0.80},
        {"id": "opening_range_drive", "setups": ("orb", "opening range", "opening-range"),
         "min_score": 76.0, "size": 0.90, "chase": 1.10,
         "phases": ("open_drive", "mid_morning")},
    ),
    "TREND_DOWN": (
        {"id": "trend_continuation", "setups": ("breakout", "momentum", "flag", "trend"),
         "min_score": 78.0, "size": 1.0, "chase": 1.25},
        {"id": "vwap_pullback", "setups": ("vwap pullback", "vwap reclaim", "pullback", "rejection"),
         "min_score": 76.0, "size": 0.85, "chase": 0.80},
        {"id": "opening_range_drive", "setups": ("orb", "opening range", "opening-range"),
         "min_score": 76.0, "size": 0.90, "chase": 1.10,
         "phases": ("open_drive", "mid_morning")},
    ),
    "CHOP_RANGE": (
        {"id": "mean_reversion", "setups": ("mean reversion", "mean-reversion", "reversion", "revert", "vwap reclaim", "reclaim"),
         "min_score": 80.0, "size": 0.70, "chase": 1.80},
        {"id": "range_extreme_vwap_fade", "setups": ("vwap fade", "vwap rejection", "range", "fade"),
         "min_score": 80.0, "size": 0.65, "chase": 1.80},
        {"id": "rs_leader_breakout", "setups": ("relative strength", "relative weakness", "rs ", "leader", "breakout", "momentum"),
         "min_score": 78.0, "size": 0.70, "chase": 0.90},
    ),
    "HIGH_VOL_EXPANSION": (
        {"id": "volatility_impulse", "setups": ("impulse", "expansion", "breakout", "momentum", "volatility"),
         "min_score": 78.0, "size": 0.45, "chase": 1.60},
    ),
    "LOW_VOL_PIN": (
        {"id": "rvol_scalp", "setups": ("scalp", "rvol", "momentum", "reclaim"),
         "min_score": 78.0, "size": 0.55, "chase": 0.45},
        {"id": "quick_lock", "setups": ("quick", "tight", "flag", "vwap"),
         "min_score": 76.0, "size": 0.50, "chase": 0.45},
    ),
}

REGIME_META = {
    "TREND_UP": {"max_spread_pct": 12.0, "breathe": True},
    "TREND_DOWN": {"max_spread_pct": 12.0, "breathe": True},
    "CHOP_RANGE": {"max_spread_pct": 10.0, "breathe": False},
    "HIGH_VOL_EXPANSION": {"max_spread_pct": 10.0, "breathe": False},
    "LOW_VOL_PIN": {"max_spread_pct": 8.0, "breathe": False},
    "EVENT_RISK": {"max_spread_pct": 8.0, "breathe": False, "name": "stand_aside"},
    "DEGRADED_DATA": {"max_spread_pct": 8.0, "breathe": False, "name": "feed_failsafe"},
}


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and abs(out) != float("inf") else None


def exposure_of(candidate: Dict[str, Any]) -> str:
    right = str(candidate.get("option_right") or candidate.get("right") or "").upper()
    if right.startswith("C"):
        return "long"
    if right.startswith("P"):
        return "short"
    action = str(candidate.get("action") or "BUY").upper()
    if action in {"SELL", "SELL_SHORT", "SHORT"}:
        return "short"
    return "long"


def _aligned(regime: str, bias: str, exposure: str) -> Optional[bool]:
    side = bias
    if regime == "TREND_UP":
        side = "up"
    elif regime == "TREND_DOWN":
        side = "down"
    if side == "up":
        return exposure == "long"
    if side == "down":
        return exposure == "short"
    return None


def match_playbook(regime: str, setup: Any, phase: str) -> Optional[Dict[str, Any]]:
    """First rotation entry whose setup tokens hit. Phase-gated books are skipped outside their window."""
    text = " " + str(setup or "").lower().strip() + " "
    if not str(setup or "").strip():
        return None
    for book in ROTATION.get(regime) or ():
        phases = book.get("phases")
        if phases and phase not in phases:
            continue
        if any(token in text for token in book["setups"]):
            return book
    return None


def rotation_ids(regime: str) -> List[str]:
    return [book["id"] for book in ROTATION.get(regime) or ()]


def adjudicate(
    classification: Dict[str, Any],
    candidate: Optional[Dict[str, Any]],
    pace: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cand = dict(candidate or {})
    regime = str(classification.get("regime") or "CHOP_RANGE")
    meta = REGIME_META.get(regime) or REGIME_META["CHOP_RANGE"]
    phase = str(classification.get("phase") or "unknown")
    bias = str(classification.get("bias") or "unknown")
    exposure = exposure_of(cand)
    cand["exposure"] = exposure
    score = _num(cand.get("score"))
    rvol = _num(cand.get("rvol"))
    dte = _num(cand.get("dte"))
    is_0dte = cand.get("is_0dte") is True or dte == 0
    instrument = str(cand.get("instrument") or "option").lower()
    width = _num(cand.get("spread_pct"))
    if width is None and cand.get("bid") is not None:
        width = spread_pct(cand.get("bid"), cand.get("ask"))
    age = _num(cand.get("quote_age_sec"))
    pnl = _num(cand.get("realized_pnl_today_pct"))
    loss_limit = _num(cand.get("daily_loss_limit_pct"))
    pace = pace or {}
    aligned = _aligned(regime, bias, exposure)

    reasons: List[str] = list(classification.get("reason_codes") or [])
    reasons.append("scanner_kept_running")
    hard: List[str] = []
    soft: List[str] = []

    if regime == "DEGRADED_DATA":
        hard.append("degraded_data")
    elif regime == "EVENT_RISK":
        hard.append("event_risk")
    if pnl is not None and loss_limit is not None and loss_limit > 0 and pnl <= -abs(loss_limit) + 1e-9:
        hard.append("daily_loss_limit")
    if is_0dte and phase == "late_decay":
        hard.append("zero_dte_late_decay")
    max_spread = float(meta.get("max_spread_pct") or 12)
    if instrument == "option" and width is not None and width > max_spread + 1e-9:
        hard.append("spread_too_wide")
    if age is not None and age > 180:
        hard.append("quote_stale")
    if score is None and instrument != "equity" and regime not in {"DEGRADED_DATA", "EVENT_RISK"}:
        hard.append("score_unmeasured")

    book = None if hard else match_playbook(regime, cand.get("setup"), phase)
    playbook_id = (book or {}).get("id") or meta.get("name") or "stand_aside"
    pace_lifted = False

    if not hard and regime not in {"DEGRADED_DATA", "EVENT_RISK"}:
        if book is None:
            soft.append("setup_outside_rotation")
        else:
            floor = float(book["min_score"])
            # Fading the index trend is not a secondary playbook. A+ only.
            if aligned is False and regime in {"TREND_UP", "TREND_DOWN"}:
                if score is None or score < APLUS_SCORE or rvol is None or rvol < APLUS_RVOL:
                    hard.append("fade_needs_aplus")
                else:
                    reasons.append("aplus_fade")
            elif score is not None and score + 1e-9 < floor:
                soft.append("below_playbook_score")
            if rvol is None and instrument != "equity":
                hard.append("rvol_unmeasured")
            chase_limit = book.get("chase")
            if chase_limit is not None:
                chase = chase_blocked(cand, float(chase_limit))
                if chase:
                    hard.append(chase)

    cause = hard + [code for code in soft if code not in hard]
    blocks = list(cause)
    multiplier = 0.0
    if any(code in NON_OVERRIDABLE or code.startswith("chase_") for code in hard):
        verdict = "BLOCK"
    elif blocks and _pace_can_lift(blocks, score, rvol, book, pace, aligned, regime):
        pace_lifted = True
        verdict = "REDUCE"
        base = float(book["size"]) if book else 0.55
        multiplier = min(base, float(pace.get("pace_size_multiplier") or 0.55))
        multiplier = max(0.40, min(0.75, multiplier))
        reasons.append("paced_bplus")
        blocks = [code for code in blocks if code in NON_OVERRIDABLE or code.startswith("chase_")]
    elif blocks:
        verdict = "BLOCK"
    else:
        multiplier = float(book["size"]) if book else 1.0
        if aligned is False:
            multiplier = min(multiplier, 0.70)
        if multiplier < 0.999:
            verdict = "REDUCE"
            reasons.append("size_cut")
        else:
            verdict = "ALLOW"
            multiplier = 1.0

    if pace_lifted:
        blocks = []

    summary = _summary(regime, playbook_id, verdict, hard if verdict == "BLOCK" else [], reasons)
    return {
        "playbook": playbook_id,
        "playbook_id": playbook_id,
        "rotation": rotation_ids(regime),
        "verdict": verdict,
        "size_multiplier": multiplier if verdict != "BLOCK" else 0.0,
        "reason_codes": _uniq(reasons + (cause if verdict == "BLOCK" else [])),
        "block_reasons": cause if verdict == "BLOCK" else [],
        "summary": summary,
        "max_spread_pct": max_spread,
        "aligned": aligned,
        "exposure": exposure,
        "spread_pct": round(width, 4) if width is not None else None,
        "exit_alignment": exit_alignment(regime, breathe=bool(meta.get("breathe"))),
        "scanner_policy": "84-A",
        "morning_loss_shutoff": False,
        "pacing": {
            "applied": pace_lifted,
            "state": pace.get("state"),
            "behind": bool(pace.get("behind")),
            "checkpoint": pace.get("checkpoint"),
            "shortfall": pace.get("shortfall"),
            "trades_completed": pace.get("trades_completed"),
            "min_trades_per_day": pace.get("min_trades_per_day"),
            "trades_remaining": pace.get("trades_remaining"),
            "score_floor": BPLUS_SCORE if pace_lifted else (float(book["min_score"]) if book else None),
            "overrides_hard_rails": False,
        },
    }


def _pace_can_lift(blocks, score, rvol, book, pace, aligned, regime) -> bool:
    if not pace.get("behind") or book is None:
        return False
    if any(code in NON_OVERRIDABLE or str(code).startswith("chase_") for code in blocks):
        return False
    if aligned is False and regime in {"TREND_UP", "TREND_DOWN"}:
        return False
    if score is None or score + 1e-9 < max(BPLUS_SCORE, paced_score_floor(float(book["min_score"]), pace)):
        return False
    if rvol is None or rvol + 1e-9 < BPLUS_RVOL:
        return False
    return "below_playbook_score" in blocks


def _uniq(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _summary(regime: str, playbook: str, verdict: str, blocks: List[str], reasons: List[str]) -> str:
    if blocks:
        return f"{verdict} {regime} / {playbook}: " + ", ".join(blocks)
    notable = [r for r in reasons if r not in {"scanner_kept_running", "size_cut"}]
    tail = (", ".join(notable[:4])) if notable else "within playbook"
    return f"{verdict} {regime} / {playbook}: {tail}"
