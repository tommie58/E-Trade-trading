"""Live intraday regime classifier with hysteresis.

Regimes: TREND_UP, TREND_DOWN, CHOP_RANGE, HIGH_VOL_EXPANSION, LOW_VOL_PIN,
EVENT_RISK, and DEGRADED_DATA when the quote feed is failed or stale.

Inputs are whatever the bot already measured. Missing fields stay missing —
they never become a flat tape or a healthy feed.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

REGIMES = (
    "TREND_UP",
    "TREND_DOWN",
    "CHOP_RANGE",
    "HIGH_VOL_EXPANSION",
    "LOW_VOL_PIN",
    "EVENT_RISK",
    "DEGRADED_DATA",
)

# Safety regimes enter on the first bad read. A noisy trend/chop label has to
# repeat so one bar cannot flip the book.
IMMEDIATE_REGIMES = frozenset({"DEGRADED_DATA", "EVENT_RISK"})
CONFIRM_BARS = 3
LEAVE_DEGRADED_BARS = 2

# A normal SPY session travels about 1% high-to-low. Used only as the
# denominator when the caller did not pass an expected range.
NORMAL_DAY_RANGE_PCT = 1.0

# Efficiency below this is a two-way tape. Above it, with an index on one
# side of VWAP, is a trend.
TREND_EFFICIENCY = 0.55
CHOP_EFFICIENCY = 0.38
TREND_VWAP_PCT = 0.08
CLEAR_VWAP_PCT = 0.35
PIN_VWAP_PCT = 0.20
HIGH_VOL_RATIO = 1.70
LOW_VOL_RATIO = 0.45
STALE_FEED_SECONDS = 180.0


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def session_phase(minute_et: Any, close_minute: Any = None) -> str:
    """Named slice of the regular session. Outside 9:30–close ET is `closed`.

    `close_minute` defaults to 16:00, so a full day is unchanged. A 13:00
    close reports `closed` from 13:00 on.
    """
    minute = _num(minute_et)
    if minute is None:
        return "unknown"
    close = 16 * 60
    parsed_close = _num(close_minute)
    if parsed_close is not None and parsed_close > 9 * 60 + 30:
        close = parsed_close
    if minute < 9 * 60 + 30 or minute >= close:
        return "closed"
    if minute < 10 * 60 + 30:
        return "open_drive"
    if minute < 11 * 60 + 30:
        return "mid_morning"
    if minute < 13 * 60 + 30:
        return "midday_chop"
    if minute < 15 * 60:
        return "afternoon_trend"
    return "late_decay"


def path_efficiency(prices: Any) -> Optional[float]:
    """|net move| / path length. 1 is a straight line, near 0 is chop.

    Needs four prints. Fewer is unmeasured, not "efficient".
    """
    if not isinstance(prices, (list, tuple)):
        return None
    vals: List[float] = []
    for raw in prices:
        price = _num(raw if not isinstance(raw, dict) else raw.get("last"))
        if price is not None and price > 0:
            vals.append(price)
    if len(vals) < 4:
        return None
    path = 0.0
    for i in range(1, len(vals)):
        path += abs(vals[i] - vals[i - 1])
    if path <= 0:
        return None
    return abs(vals[-1] - vals[0]) / path


def opening_range_state(high: Any, low: Any, last: Any) -> Optional[str]:
    hi, lo, px = _num(high), _num(low), _num(last)
    if hi is None or lo is None or px is None or hi <= lo or px <= 0:
        return None
    if px > hi:
        return "break_up"
    if px < lo:
        return "break_down"
    return "inside"


def _feed_degraded(feed: Any) -> Tuple[bool, List[str]]:
    """True when the upstream snapshot feed cannot be traded against."""
    if not isinstance(feed, dict):
        return False, []
    reasons: List[str] = []
    if feed.get("ok") is False:
        reasons.append("feed_unhealthy")
    status = _num(feed.get("http_status"))
    if status in (401, 403):
        reasons.append("massive_http_%d" % int(status))
    err = str(feed.get("last_error") or "")
    if "403" in err and "massive_http_403" not in reasons:
        reasons.append("massive_http_403")
    if "401" in err and "massive_http_401" not in reasons:
        reasons.append("massive_http_401")
    age = _num(feed.get("age_seconds"))
    if feed.get("stale") is True or (age is not None and age > STALE_FEED_SECONDS and feed.get("ok") is False):
        reasons.append("feed_stale")
    # An explicit healthy feed with no error is not degraded, even if an old
    # error string was left on a recovered client. `ok is True` wins.
    if feed.get("ok") is True and status not in (401, 403) and "403" not in err and "401" not in err:
        return False, []
    return bool(reasons), reasons


def _bias(spy: Optional[float], qqq: Optional[float], breadth: Optional[float]) -> str:
    vals = [v for v in (spy, qqq) if v is not None]
    if not vals and breadth is None:
        return "unknown"
    if len(vals) == 2 and vals[0] > TREND_VWAP_PCT and vals[1] < -TREND_VWAP_PCT:
        return "mixed"
    if len(vals) == 2 and vals[0] < -TREND_VWAP_PCT and vals[1] > TREND_VWAP_PCT:
        return "mixed"
    score = 0.0
    if vals:
        score += sum(vals) / len(vals)
    if breadth is not None:
        score += (breadth - 0.5) * 0.40
    if score >= TREND_VWAP_PCT:
        return "up"
    if score <= -TREND_VWAP_PCT:
        return "down"
    return "flat"


def classify_raw(observation: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """One-bar regime, before debounce. Pure."""
    obs = observation or {}
    spy = _num(obs.get("spy_vs_vwap_pct"))
    qqq = _num(obs.get("qqq_vs_vwap_pct"))
    efficiency = path_efficiency(obs.get("prices"))
    if efficiency is None:
        efficiency = _num(obs.get("efficiency"))
    chop = (1.0 - efficiency) if efficiency is not None else _num(obs.get("chop_ratio"))
    if efficiency is None and chop is not None:
        efficiency = max(0.0, min(1.0, 1.0 - chop))
    realized = _num(obs.get("realized_range_pct"))
    expected = _num(obs.get("expected_range_pct"))
    vol_ratio = _num(obs.get("vol_ratio"))
    if vol_ratio is None and realized is not None and realized >= 0:
        base = expected if expected is not None and expected > 0 else NORMAL_DAY_RANGE_PCT
        vol_ratio = realized / base
    breadth_count = _num(obs.get("breadth_count"))
    breadth_above = _num(obs.get("breadth_above_vwap"))
    breadth = None
    if breadth_count is not None and breadth_count >= 4 and breadth_above is not None:
        breadth = max(0.0, min(1.0, breadth_above / breadth_count))
    or_state = opening_range_state(
        obs.get("opening_range_high"), obs.get("opening_range_low"), obs.get("last"),
    )
    phase = session_phase(obs.get("minute_et"), obs.get("close_minute"))
    feed = obs.get("feed") if isinstance(obs.get("feed"), dict) else None
    degraded, feed_reasons = _feed_degraded(feed)
    event = obs.get("event_active") is True

    measured = 0
    confidence = 0.34
    if spy is not None and qqq is not None:
        confidence += 0.16
        measured += 2
    elif spy is not None or qqq is not None:
        confidence += 0.08
        measured += 1
    if efficiency is not None:
        confidence += 0.16
        measured += 1
    if vol_ratio is not None:
        confidence += 0.10
        measured += 1
    if breadth is not None:
        confidence += 0.08
        measured += 1
    if or_state is not None:
        confidence += 0.06
        measured += 1
    if phase not in {"unknown", "closed"}:
        confidence += 0.04

    bias = _bias(spy, qqq, breadth)
    if bias == "mixed":
        confidence -= 0.12
    confidence = max(0.15, min(0.95, confidence))

    reasons: List[str] = []
    if degraded:
        regime = "DEGRADED_DATA"
        reasons.extend(feed_reasons or ["feed_unhealthy"])
        confidence = max(confidence, 0.90)
    elif event:
        regime = "EVENT_RISK"
        reasons.append("event_window")
        confidence = max(confidence, 0.88)
    elif vol_ratio is not None and vol_ratio >= HIGH_VOL_RATIO:
        regime = "HIGH_VOL_EXPANSION"
        reasons.append("range_expanded")
        if bias == "up":
            reasons.append("bias_up")
        elif bias == "down":
            reasons.append("bias_down")
    elif (
        vol_ratio is not None
        and vol_ratio <= LOW_VOL_RATIO
        and bias in {"flat", "unknown"}
        and (efficiency is None or efficiency < TREND_EFFICIENCY)
        and (or_state in {None, "inside"})
    ):
        regime = "LOW_VOL_PIN"
        reasons.append("range_pinned")
    else:
        trend_up = bias == "up" and (
            (efficiency is not None and efficiency >= TREND_EFFICIENCY)
            or (efficiency is None and _clear(spy, qqq, "up"))
            or or_state == "break_up" and bias == "up"
        )
        trend_down = bias == "down" and (
            (efficiency is not None and efficiency >= TREND_EFFICIENCY)
            or (efficiency is None and _clear(spy, qqq, "down"))
            or or_state == "break_down" and bias == "down"
        )
        choppy = (
            bias in {"mixed", "flat"}
            or (efficiency is not None and efficiency < CHOP_EFFICIENCY)
            or or_state == "inside" and (efficiency is None or efficiency < TREND_EFFICIENCY)
        )
        if trend_up and not (efficiency is not None and efficiency < CHOP_EFFICIENCY and bias != "up"):
            regime = "TREND_UP"
            reasons.append("index_above_vwap")
            if efficiency is not None and efficiency >= TREND_EFFICIENCY:
                reasons.append("efficient_trend")
        elif trend_down and not (efficiency is not None and efficiency < CHOP_EFFICIENCY and bias != "down"):
            regime = "TREND_DOWN"
            reasons.append("index_below_vwap")
            if efficiency is not None and efficiency >= TREND_EFFICIENCY:
                reasons.append("efficient_trend")
        elif choppy or phase == "midday_chop":
            regime = "CHOP_RANGE"
            reasons.append("low_efficiency" if efficiency is not None and efficiency < CHOP_EFFICIENCY else "no_trend")
        elif bias == "up":
            regime = "TREND_UP"
            reasons.append("index_above_vwap")
        elif bias == "down":
            regime = "TREND_DOWN"
            reasons.append("index_below_vwap")
        else:
            regime = "CHOP_RANGE"
            reasons.append("no_trend")

    if measured == 0 and regime not in IMMEDIATE_REGIMES:
        reasons.append("thin_tape")
        confidence = min(confidence, 0.40)

    return {
        "regime": regime,
        "confidence": round(confidence, 3),
        "phase": phase,
        "bias": bias,
        "efficiency": round(efficiency, 4) if efficiency is not None else None,
        "chop_ratio": round(chop, 4) if chop is not None else None,
        "vol_ratio": round(vol_ratio, 4) if vol_ratio is not None else None,
        "index": {
            "spy_vs_vwap_pct": round(spy, 4) if spy is not None else None,
            "qqq_vs_vwap_pct": round(qqq, 4) if qqq is not None else None,
        },
        "opening_range": or_state,
        "breadth": round(breadth, 4) if breadth is not None else None,
        "feed": {
            "ok": False if degraded else (None if feed is None else feed.get("ok", True)),
            "http_status": None if not isinstance(feed, dict) else feed.get("http_status"),
            "stale": bool(isinstance(feed, dict) and feed.get("stale")),
            "last_error": (str(feed.get("last_error"))[:180] if isinstance(feed, dict) and feed.get("last_error") else None),
            "endpoint": (str(feed.get("endpoint"))[:120] if isinstance(feed, dict) and feed.get("endpoint") else None),
            "fallback": (str(feed.get("fallback"))[:40] if isinstance(feed, dict) and feed.get("fallback") else None),
        },
        "reason_codes": reasons,
        "event_active": event,
    }


def _clear(spy: Optional[float], qqq: Optional[float], side: str) -> bool:
    vals = [v for v in (spy, qqq) if v is not None]
    if not vals:
        return False
    if side == "up":
        return min(vals) >= CLEAR_VWAP_PCT
    return max(vals) <= -CLEAR_VWAP_PCT


def _blank_memory() -> Dict[str, Any]:
    return {"regime": None, "pending": None, "pending_count": 0}


def advance_regime(memory: Optional[Dict[str, Any]], raw: str, confidence: float) -> Tuple[str, Dict[str, Any], bool]:
    """Apply hysteresis. Returns (regime, new_memory, switched).

    DEGRADED_DATA and EVENT_RISK enter immediately. Leaving a safety regime
    takes two agreeing bars. Everything else takes three, unless confidence
    is already strong.
    """
    state = dict(memory or _blank_memory())
    current = state.get("regime")
    if current not in REGIMES:
        state.update(regime=raw, pending=None, pending_count=0)
        return raw, state, True
    if raw == current:
        state.update(pending=None, pending_count=0)
        return current, state, False
    if raw in IMMEDIATE_REGIMES:
        state.update(regime=raw, pending=None, pending_count=0)
        return raw, state, True

    need = LEAVE_DEGRADED_BARS if current in IMMEDIATE_REGIMES else CONFIRM_BARS
    if state.get("pending") == raw:
        state["pending_count"] = int(state.get("pending_count") or 0) + 1
    else:
        state["pending"] = raw
        state["pending_count"] = 1
    if int(state["pending_count"]) >= need:
        state.update(regime=raw, pending=None, pending_count=0)
        return raw, state, True
    return str(current), state, False


def classify(observation: Optional[Dict[str, Any]], memory: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Raw classification plus the debounced regime the desk will trade."""
    raw = classify_raw(observation)
    regime, memory_out, switched = advance_regime(memory, raw["regime"], float(raw["confidence"]))
    out = dict(raw)
    out["raw_regime"] = raw["regime"]
    out["regime"] = regime
    out["switched"] = switched
    out["pending"] = memory_out.get("pending")
    out["pending_count"] = memory_out.get("pending_count") or 0
    if regime != raw["regime"]:
        out["reason_codes"] = list(raw["reason_codes"]) + ["debounce_held"]
    return out, memory_out
