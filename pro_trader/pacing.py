"""Intraday pace toward MIN_TRADES_PER_DAY.

The desk counts fills against the session clock. Past a checkpoint, a
shortfall steps the score floor down toward a verified B+ (never junk) and
cuts size into the 0.40–0.75 band. It does not raise size, waive a spread
cap, trade a dead feed, or punch through the daily loss halt.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional

MIN_TRADES_DEFAULT = 4
MIN_TRADES_FLOOR = 1
MIN_TRADES_CAP = 12
# Verified B+. Pacing may relax a playbook floor to here and no further.
BPLUS_SCORE = 75.0
BPLUS_RVOL = 0.9
# Paced tickets only. Primary A setups keep their own (larger) multiplier.
PACE_SIZE_MAX = 0.75
PACE_SIZE_MIN = 0.40

# (name, minute of day ET, fills required by then). Linear would still be
# short at 2pm; these checkpoints ask earlier so the book does not hunt
# the quota in the last twenty minutes.
CHECKPOINTS = (
    ("mid_morning", 10 * 60 + 30, 1),
    ("midday", 12 * 60, 2),
    ("early_afternoon", 14 * 60, 3),
    ("late_session", 15 * 60, 4),
)


def clamp_min_trades(value: Any, default: int = MIN_TRADES_DEFAULT) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = default
    if parsed < MIN_TRADES_FLOOR:
        return MIN_TRADES_FLOOR
    if parsed > MIN_TRADES_CAP:
        return MIN_TRADES_CAP
    return parsed


def _minute(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def pace_state(
    minute_et: Any,
    trades_completed: Any,
    min_trades: Any = MIN_TRADES_DEFAULT,
    close_minute: Any = None,
) -> Dict[str, Any]:
    """Where the session sits against the daily trade target. Pure."""
    target = clamp_min_trades(min_trades)
    try:
        done = max(0, int(trades_completed or 0))
    except (TypeError, ValueError):
        done = 0
    minute = _minute(minute_et)
    open_m = 9 * 60 + 30
    close_m = 16 * 60
    parsed_close = _minute(close_minute)
    if parsed_close is not None and parsed_close > open_m:
        close_m = parsed_close
    if minute is None:
        progress = 0.0
    else:
        progress = (minute - open_m) / float(close_m - open_m)
        progress = max(0.0, min(1.0, progress))

    active = None
    required = 0
    if minute is not None:
        scaled = max(1, int(math.ceil(target * (CHECKPOINTS[-1][2] / float(MIN_TRADES_DEFAULT)))))
        # Scale the 4-trade checkpoint table when the operator sets a different target.
        for name, at, need in CHECKPOINTS:
            need_scaled = max(1, int(math.ceil(need * target / float(MIN_TRADES_DEFAULT))))
            if minute + 1e-9 >= at:
                active = name
                required = min(target, max(required, need_scaled))
            else:
                break
        if scaled:
            required = min(target, required)

    shortfall = max(0, required - done)
    behind = shortfall > 0 and active is not None
    if not behind:
        pace_size = 1.0
        relief = 0.0
    else:
        relief = min(15.0, 5.0 * shortfall)
        pace_size = PACE_SIZE_MAX - (PACE_SIZE_MAX - PACE_SIZE_MIN) * min(shortfall - 1, 2) / 2.0
        pace_size = max(PACE_SIZE_MIN, min(PACE_SIZE_MAX, pace_size))

    return {
        "min_trades_per_day": target,
        "trades_completed": done,
        "trades_remaining": max(0, target - done),
        "session_progress": round(progress, 4),
        "checkpoint": active,
        "checkpoint_required": required,
        "shortfall": shortfall,
        "behind": behind,
        "state": "behind" if behind else ("met" if done >= target else "on_pace"),
        "score_relief": relief,
        "bplus_score": BPLUS_SCORE,
        "bplus_rvol": BPLUS_RVOL,
        "pace_size_multiplier": round(pace_size, 4),
        "overrides_hard_rails": False,
    }


def paced_score_floor(playbook_floor: float, pace: Dict[str, Any]) -> float:
    """The floor after a behind-pace step-down. Never under B+."""
    if not pace.get("behind"):
        return float(playbook_floor)
    return max(BPLUS_SCORE, float(playbook_floor) - float(pace.get("score_relief") or 0))
