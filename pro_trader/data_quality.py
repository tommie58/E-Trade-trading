"""Shadow data-quality measurements. Defaults do not change a live decision.

Switches, each read from the environment on every use (not at import):

- ``VWAP_SOURCE`` ``sampled`` (default) or ``bars`` (enforcement, default off)
- ``AGGS_PULL_INTERVAL_S`` unset means 60s, aligned to the minute boundary
- ``AGGS_INCREMENTAL_MERGE`` ``false`` (default). Off keeps the full-day pull
- ``BAD_TICK_FILTER_ENABLED`` ``false`` (default)
- ``GAP_SOURCE`` ``current`` (default) or ``snapshot``

Shadow rows fail closed: a missing or stale bar VWAP is ``no_data`` and is
not replaced with the sampled VWAP. Enforcement (``VWAP_SOURCE=bars``) also
fails closed, with no sampled fallback. A missing snapshot gap leaves the
current gap verdict in place. A bad IV check nulls only the IV field.
Nothing here places an order or reads a secret.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("etrade-bot.data-quality")

# A bar-VWAP row is ``no_data`` when its own bars are missing, stale, or
# rejected. That does not borrow the sampled VWAP. IV is its own field.
SHADOW_OK = "ok"
SHADOW_NO_DATA = "no_data"
_IV_STALE_S = 90.0
_AUTH_REASONS = frozenset({"unauthorized", "forbidden", "not_configured"})

# Quant Lab: a 90s pull was stale 16.2% of the time. 60s on the minute was 0%.
DEFAULT_AGGS_PULL_INTERVAL_S = 60.0
# Previous elapsed throttle. Set AGGS_PULL_INTERVAL_S=90 to restore it.
LEGACY_AGGS_PULL_INTERVAL_S = 90.0
# Minute boundary. Second 0, not five seconds after the close.
AGGS_ALIGN_AFTER_CLOSE_S = 0
# Newest completed bar older than this is not fresh enough to drive the gate.
BAR_VWAP_STALE_S = 150.0
# Bad-tick: wick beyond the body, versus the median true range of the prior bars.
DEFAULT_BAD_TICK_K = 8.0
BAD_TICK_TR_WINDOW = 20
BAD_TICK_MIN_TR_BARS = 5
# Index ETFs also flag a wick wider than this many basis points of the close.
BAD_TICK_INDEX_BPS = 50.0
INDEX_ETFS = frozenset({"SPY", "QQQ", "IWM", "DIA"})
# Quant Lab, Nov 24–25: the bot pulls aggs about every 90–150s.
OBSERVED_PULL_SPACING_S = (90.0, 150.0)
_RTH_OPEN_MINUTE = 9 * 60 + 30
_RTH_CLOSE_MINUTE = 16 * 60
_BAR_SECONDS = 60.0

_STATE: Dict[str, Any] = {}


def _feed(reason: str = "unmeasured") -> Dict[str, Any]:
    return {"status": SHADOW_NO_DATA, "reason": reason}


def _fresh_state() -> Dict[str, Any]:
    return {
        "evaluations": 0,
        "no_data": 0,
        "fallback_stale": 0,
        "fallback_missing": 0,
        "gap_pairs": 0,
        "gap_fallback_missing": 0,
        "max_abs_vwap_diff_bps": None,
        "symbols": {},
        "gaps": {},
        "bad_tick_ids": {},
        "bad_tick_details": [],
        "bad_tick_confirmed_ids": {},
        "bad_tick_confirmed_details": [],
        "symbol_iv": {},
        "agg_requests": {},
        "bars_feed": _feed("unmeasured"),
        "iv_feed": _feed("unmeasured"),
        "symbol_bars": {},
        "entitlement_session": None,
        "entitlement_probed_at": None,
    }


def reset_shadow() -> None:
    """Test helper. Drops the in-process shadow ledger."""
    global _STATE
    _STATE = _fresh_state()


def _state() -> Dict[str, Any]:
    global _STATE
    if not _STATE:
        _STATE = _fresh_state()
    return _STATE


def _env_choice(name: str, default: str, allowed: Sequence[str]) -> str:
    raw = os.getenv(name, default)
    text = str(raw if raw is not None else default).strip().lower()
    if text in allowed:
        return text
    return default


def vwap_source_flag() -> str:
    """``sampled`` or ``bars``. Read on each gate evaluation. Default sampled."""
    return _env_choice("VWAP_SOURCE", "sampled", ("sampled", "bars"))


def gap_source_flag() -> str:
    """``current`` or ``snapshot``. Read each time a gap verdict is built."""
    return _env_choice("GAP_SOURCE", "current", ("current", "snapshot"))


def bad_tick_filter_enabled() -> bool:
    """When false the detector only counts. Read when a series is published."""
    raw = os.getenv("BAD_TICK_FILTER_ENABLED", "false")
    return str(raw if raw is not None else "false").strip().lower() in {"1", "true", "yes", "on"}


def bad_tick_k() -> float:
    """Wick limit in multiples of the median true range. Read on each filter pass."""
    raw = os.getenv("BAD_TICK_K", "")
    text = str(raw if raw is not None else "").strip()
    if not text:
        return DEFAULT_BAD_TICK_K
    try:
        value = float(text)
    except (TypeError, ValueError):
        return DEFAULT_BAD_TICK_K
    if not math.isfinite(value) or value <= 0:
        return DEFAULT_BAD_TICK_K
    return value


def aggs_incremental_merge_enabled() -> bool:
    """Incremental ``from`` merge. Read on each agg pull. Default off.

    Off, every pull is the session date and the VWAP is recomputed from
    that full-day response. On, bars are merged by minute and a running
    sum is checked against that recompute. A relative gap above 1e-9
    discards the running sum. A correction to a minute older than ``from``
    is not in the incremental response, which is why this stays off until
    someone turns it on.
    """
    raw = os.getenv("AGGS_INCREMENTAL_MERGE", "false")
    return str(raw if raw is not None else "false").strip().lower() in {"1", "true", "yes", "on"}


def aggs_pull_interval_s() -> float:
    """Seconds between agg pulls. Unset is 60. Read each check."""
    raw = os.getenv("AGGS_PULL_INTERVAL_S", "")
    text = str(raw if raw is not None else "").strip()
    if not text:
        return DEFAULT_AGGS_PULL_INTERVAL_S
    try:
        value = float(text)
    except (TypeError, ValueError):
        return DEFAULT_AGGS_PULL_INTERVAL_S
    if not math.isfinite(value) or value <= 0:
        return DEFAULT_AGGS_PULL_INTERVAL_S
    return value


def aggs_alignment_active(interval_s: Optional[float] = None) -> bool:
    """Phase-lock to the minute boundary when the interval is 60s or less.

    The default 60s pull is aligned. An explicit 90s interval stays an
    elapsed throttle with no phase.
    """
    interval = aggs_pull_interval_s() if interval_s is None else float(interval_s)
    return interval <= DEFAULT_AGGS_PULL_INTERVAL_S


def _fmt(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "none"
    if not math.isfinite(float(value)):
        return "none"
    return f"{float(value):.4f}"


def _price(raw: Any) -> Optional[float]:
    if isinstance(raw, bool) or raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _finite(raw: Any) -> Optional[float]:
    if isinstance(raw, bool) or raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return value


def ts_seconds(raw: Any) -> Optional[float]:
    """Massive timestamps: ns, us, ms, or seconds. None when it is not a time."""
    value = _finite(raw)
    if value is None or value <= 0:
        return None
    if value > 1e17:
        return value / 1e9
    if value > 1e14:
        return value / 1e6
    if value > 1e11:
        return value / 1e3
    return value


def diff_bps(candidate: Any, baseline: Any) -> Optional[float]:
    """Basis points of candidate versus baseline. None if either side is unmeasured."""
    base = _price(baseline)
    other = _price(candidate)
    if base is None or other is None:
        return None
    return round((other - base) / base * 10000.0, 2)


def _latest_anchor(now_et: datetime, align_s: int = AGGS_ALIGN_AFTER_CLOSE_S) -> datetime:
    """Most recent HH:MM:05 that is not in the future."""
    anchor = now_et.replace(second=int(align_s), microsecond=0)
    if now_et < anchor:
        anchor -= timedelta(minutes=1)
    return anchor


def agg_pull_due(
    now_epoch: float,
    last_epoch: float,
    interval_s: float,
    align: bool,
    now_et: datetime,
) -> bool:
    """Whether this symbol should pull aggs now.

    ``last_epoch`` <= 0 is the first pull and is always due, so a restart
    does not sit fail-closed until the next aligned minute. After that, the
    unset path is ``elapsed >= interval`` (90s today). An aligned interval
    also requires that the latest :05 anchor is strictly after the last pull,
    so a minute produces one request and the request is not sent before the
    bar has had about five seconds to close.
    """
    try:
        now = float(now_epoch)
        last = float(last_epoch or 0)
        interval = float(interval_s)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(now) or not math.isfinite(interval) or interval <= 0:
        return False
    if not math.isfinite(last) or last <= 0:
        return True
    if now - last < interval:
        return False
    if not align:
        return True
    anchor = _latest_anchor(now_et)
    return last < anchor.timestamp() - 1e-6


# Running sum versus a full recompute of the same minutes. Wider than this
# and the pull keeps the recompute.
VWAP_RECOMPUTE_REL_TOL = 1e-9


def agg_path_range(day: str, last_bar_epoch: Optional[float]) -> Tuple[str, str]:
    """Incremental window. ``from`` is the last bar's millisecond timestamp.

    Used only when ``AGGS_INCREMENTAL_MERGE`` is on. ``to`` stays the session date.
    """
    session = str(day or "").strip()
    epoch = _finite(last_bar_epoch)
    if not session:
        return session, session
    if epoch is None or epoch <= 0:
        return session, session
    return str(int(round(epoch * 1000.0))), session


def agg_request_range(
    day: str,
    last_bar_epoch: Optional[float] = None,
    *,
    incremental: Optional[bool] = None,
) -> Tuple[str, str]:
    """Request window for one symbol. Default is the full session day."""
    session = str(day or "").strip()
    use_incremental = aggs_incremental_merge_enabled() if incremental is None else bool(incremental)
    if not use_incremental:
        return session, session
    return agg_path_range(session, last_bar_epoch)


def _bar_weight(bar: Any) -> Optional[Tuple[float, float, float]]:
    """``(timestamp, vw*v, v)`` for one bar. None when vw or v is not a price."""
    if not isinstance(bar, dict):
        return None
    epoch = ts_seconds(bar.get("t"))
    price = _price(bar.get("vw"))
    volume = _price(bar.get("v"))
    if epoch is None or price is None or volume is None:
        return None
    return epoch, price * volume, volume


def full_day_vwap(bars: Optional[Sequence[Any]]) -> Optional[float]:
    """VWAP of the latest bar at each minute. ``sum(vw*v) / sum(v)`` via fsum.

    Duplicate minutes keep the last copy in ``bars``. This is the full-day
    recompute. It does not read a sampled VWAP or a Yahoo volume.
    """
    latest: Dict[float, Tuple[float, float]] = {}
    order: List[float] = []
    for bar in bars or []:
        weight = _bar_weight(bar)
        if weight is None:
            continue
        epoch, numerator, volume = weight
        if epoch not in latest:
            order.append(epoch)
        latest[epoch] = (numerator, volume)
    if not latest:
        return None
    numerator = math.fsum(latest[epoch][0] for epoch in order)
    volume = math.fsum(latest[epoch][1] for epoch in order)
    if volume <= 0 or not math.isfinite(numerator) or not math.isfinite(volume):
        return None
    ratio = numerator / volume
    if not math.isfinite(ratio) or ratio <= 0:
        return None
    return ratio


def relative_delta(left: Optional[float], right: Optional[float]) -> Optional[float]:
    """``|a-b| / max(|a|, |b|)``. None when either side is missing."""
    if left is None or right is None:
        return None
    if not math.isfinite(left) or not math.isfinite(right):
        return None
    scale = max(abs(left), abs(right))
    if scale <= 0:
        return 0.0
    return abs(left - right) / scale


def _running_vwap(prior: Optional[Sequence[Any]], incoming: Optional[Sequence[Any]]) -> Optional[float]:
    """Add and replace minute contributions in arrival order. Independent of fsum.

    The prior cache is applied first. Each incoming bar replaces that minute's
    previous contribution. This is the incremental sum the full recompute checks.
    """
    held: Dict[float, Tuple[float, float]] = {}
    numerator = 0.0
    volume = 0.0
    for group in (prior or [], incoming or []):
        for bar in group:
            weight = _bar_weight(bar)
            if weight is None:
                continue
            epoch, bar_numerator, bar_volume = weight
            previous = held.get(epoch)
            if previous is not None:
                numerator -= previous[0]
                volume -= previous[1]
            held[epoch] = (bar_numerator, bar_volume)
            numerator += bar_numerator
            volume += bar_volume
    if volume <= 0 or not math.isfinite(numerator) or not math.isfinite(volume):
        return None
    ratio = numerator / volume
    if not math.isfinite(ratio) or ratio <= 0:
        return None
    return ratio


def adopt_session_bars(
    prior: Optional[Sequence[Any]],
    incoming: Optional[Sequence[Any]],
    *,
    incremental: bool,
) -> Dict[str, Any]:
    """Bars used for one pull, plus the VWAP of those bars.

    ``incremental`` false replaces the cache with the response. That response
    is the full day. ``incremental`` true merges by minute, checks the
    running sum against a full recompute of the merged minutes, and on a
    relative gap above 1e-9 keeps the recompute (``mismatch`` true).
    """
    arrived = [bar for bar in (incoming or []) if isinstance(bar, dict)]
    if not incremental:
        bars = merge_bars(None, arrived)
        return {
            "bars": bars,
            "vwap": full_day_vwap(bars),
            "mode": "full_day",
            "mismatch": False,
        }
    merged = merge_bars(prior, arrived)
    full = full_day_vwap(merged)
    running = _running_vwap(prior, arrived)
    gap = relative_delta(running, full)
    mismatch = full is None or gap is None or gap > VWAP_RECOMPUTE_REL_TOL
    return {
        "bars": merged,
        "vwap": full,
        "running_vwap": running,
        "mode": "full_recompute_fallback" if mismatch else "incremental",
        "mismatch": mismatch,
    }


def fold_session_batches(
    batches: Sequence[Sequence[Any]],
    *,
    incremental: bool,
    restart_at: Optional[int] = None,
) -> Dict[str, Any]:
    """Fold pull batches the way the cache does. ``restart_at`` clears the cache.

    After a restart the next batch is applied to an empty cache, which is the
    full-day request a process makes when it has no last bar.
    """
    prior: List[Dict[str, Any]] = []
    last: Dict[str, Any] = {
        "bars": [],
        "vwap": None,
        "mode": "full_day",
        "mismatch": False,
    }
    for index, batch in enumerate(batches):
        use_incremental = bool(incremental) and not (restart_at is not None and index == restart_at)
        if restart_at is not None and index == restart_at:
            prior = []
        last = adopt_session_bars(prior, batch, incremental=use_incremental and bool(prior))
        prior = list(last["bars"])
    return last


def merge_bars(existing: Optional[Sequence[Any]], incoming: Optional[Sequence[Any]]) -> List[Dict[str, Any]]:
    """Union by bar timestamp. The incoming print wins on a tie. Order is time."""
    by_ts: Dict[float, Dict[str, Any]] = {}
    for group in (existing or [], incoming or []):
        if isinstance(group, dict):
            group = [group]
        for source in group:
            if not isinstance(source, dict):
                continue
            epoch = ts_seconds(source.get("t"))
            if epoch is None:
                continue
            by_ts[epoch] = dict(source)
    return [by_ts[key] for key in sorted(by_ts)]


def last_bar_epoch(bars: Optional[Sequence[Any]]) -> Optional[float]:
    latest: Optional[float] = None
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        epoch = ts_seconds(bar.get("t"))
        if epoch is None:
            continue
        if latest is None or epoch > latest:
            latest = epoch
    return latest


def completed_rth_vwap(
    bars: Optional[Sequence[Any]],
    now_et: datetime,
    *,
    open_minute: int = _RTH_OPEN_MINUTE,
    close_minute: int = _RTH_CLOSE_MINUTE,
) -> Dict[str, Any]:
    """RTH VWAP = sum(vw * v) / sum(v) over completed 1-minute bars since 09:30 ET.

    A bar timestamp is the start of the minute. The minute still forming
    (start + 60s in the future) is left out. Premarket, the close minute,
    and any other session date are left out. A bar without a positive ``vw``
    and ``v`` does not enter the sum. Never raises.
    """
    empty: Dict[str, Any] = {
        "vwap": None,
        "bar_count": 0,
        "last_bar_epoch": None,
        "last_bar_ts": None,
        "age_seconds": None,
        "sum_vw_v": 0.0,
        "sum_v": 0.0,
    }
    try:
        if now_et is None:
            return empty
        zone = now_et.tzinfo
        now_epoch = now_et.timestamp()
        numerator = 0.0
        denominator = 0.0
        count = 0
        last_epoch: Optional[float] = None
        for bar in bars or []:
            if not isinstance(bar, dict):
                continue
            epoch = ts_seconds(bar.get("t"))
            if epoch is None:
                continue
            # Completed only: the minute has ended.
            if epoch + _BAR_SECONDS > now_epoch + 1e-6:
                continue
            bar_et = datetime.fromtimestamp(epoch, tz=zone)
            if bar_et.date() != now_et.date():
                continue
            minute = bar_et.hour * 60 + bar_et.minute
            if minute < int(open_minute) or minute >= int(close_minute):
                continue
            volume = _price(bar.get("v"))
            vwap = _price(bar.get("vw"))
            if volume is None or vwap is None:
                continue
            numerator += vwap * volume
            denominator += volume
            count += 1
            if last_epoch is None or epoch > last_epoch:
                last_epoch = epoch
        if denominator <= 0 or last_epoch is None or count <= 0:
            return empty
        vwap_px = numerator / denominator
        if not math.isfinite(vwap_px) or vwap_px <= 0:
            return empty
        age = now_epoch - last_epoch
        last_dt = datetime.fromtimestamp(last_epoch, tz=zone)
        return {
            "vwap": round(vwap_px, 4),
            "bar_count": count,
            "last_bar_epoch": last_epoch,
            "last_bar_ts": last_dt.isoformat(),
            "age_seconds": round(age, 1),
            "sum_vw_v": numerator,
            "sum_v": denominator,
        }
    except Exception:
        logger.debug("bar vwap unreadable", exc_info=True)
        return empty


def choose_vwap(
    *,
    sampled_vwap: Any,
    gate_vwap: Any,
    bar_vwap: Any,
    bar_age_s: Any,
    bar_count: int = 0,
    source: Optional[str] = None,
    stale_after_s: float = BAR_VWAP_STALE_S,
) -> Dict[str, Any]:
    """Pick the gate input. Default ``sampled`` does not replace it.

    ``bars`` (enforcement, default off) uses the completed-bar VWAP when it
    is fresh. Stale or missing fails closed: the gate input becomes
    unmeasured and the sampled VWAP is not a fallback.
    """
    selected_flag = source if source in {"sampled", "bars"} else vwap_source_flag()
    sampled = _price(sampled_vwap)
    gate = _price(gate_vwap)
    bar = _price(bar_vwap)
    age = _finite(bar_age_s)
    diff = diff_bps(bar, sampled)
    choice: Dict[str, Any] = {
        "configured_source": selected_flag,
        "selected": "sampled",
        "fallback": None,
        "replace_gate": False,
        "sampled_vwap": sampled,
        "gate_vwap": gate,
        "bar_vwap": bar,
        "diff_bps": diff,
        "bar_age_s": None if age is None else round(age, 1),
        "bar_count": int(bar_count or 0),
        "applied_gate_vwap": gate,
        "fail_closed": False,
        "enforcement_reason": None,
    }
    if selected_flag != "bars":
        return choice
    missing = bar is None or int(bar_count or 0) <= 0
    stale = (not missing) and (age is None or age < 0 or age > float(stale_after_s))
    if missing or stale:
        choice["fail_closed"] = True
        choice["fallback"] = None
        choice["selected"] = None
        choice["replace_gate"] = False
        choice["applied_gate_vwap"] = None
        choice["enforcement_reason"] = "bar_vwap_missing" if missing else "bar_vwap_stale"
        return choice
    choice["selected"] = "bars"
    choice["replace_gate"] = True
    choice["applied_gate_vwap"] = bar
    return choice


def _true_range(bar: Dict[str, Any], prev_close: Optional[float]) -> Optional[float]:
    high = _price(bar.get("h"))
    low = _price(bar.get("l"))
    if high is None or low is None or high + 1e-9 < low:
        return None
    span = high - low
    if prev_close is None:
        return span
    return max(span, abs(high - prev_close), abs(low - prev_close))


def _median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _is_index_etf(symbol: Any) -> bool:
    return str(symbol or "").upper().strip() in INDEX_ETFS


def filter_bad_ticks(
    bars: Optional[Sequence[Any]],
    symbol: Any = "",
    *,
    k: Optional[float] = None,
    index_bps: float = BAD_TICK_INDEX_BPS,
    window: int = BAD_TICK_TR_WINDOW,
) -> Dict[str, Any]:
    """Flag a high or low that sticks out past the body.

    The wick is ``high - max(open, close)`` or ``min(open, close) - low``.
    It is a bad tick when it exceeds K times the median true range of the
    prior bars (up to 20, and only after a handful exist) or, for an index
    ETF, 50 basis points of the close. Flagged bars are also returned
    clamped to that limit. The raw ``vw`` and ``v`` are copied through, so
    this does not move a VWAP. Never raises.
    """
    multiple = bad_tick_k() if k is None else float(k)
    index = _is_index_etf(symbol)
    clean: List[Dict[str, Any]] = []
    flagged: List[Dict[str, Any]] = []
    ranges: List[float] = []
    prev_close: Optional[float] = None
    try:
        ordered = []
        for bar in bars or []:
            if isinstance(bar, dict):
                ordered.append(bar)
        ordered.sort(key=lambda item: ts_seconds(item.get("t")) or 0.0)
        for bar in ordered:
            copied = dict(bar)
            high = _price(copied.get("h"))
            low = _price(copied.get("l"))
            opened = _price(copied.get("o"))
            close = _price(copied.get("c"))
            tr = _true_range(copied, prev_close)
            prior = ranges[-int(window):]
            median_tr = _median(prior) if len(prior) >= BAD_TICK_MIN_TR_BARS else None
            limits: List[float] = []
            if median_tr is not None and median_tr > 0 and multiple > 0:
                limits.append(multiple * median_tr)
            ref = close or opened
            if index and ref is not None and index_bps > 0:
                limits.append(ref * float(index_bps) / 10000.0)
            limit = min(limits) if limits else None
            reasons: List[str] = []
            if (
                limit is not None
                and high is not None
                and opened is not None
                and close is not None
                and high - max(opened, close) > limit + 1e-9
            ):
                reasons.append("high")
                copied["h"] = round(max(opened, close) + limit, 4)
            if (
                limit is not None
                and low is not None
                and opened is not None
                and close is not None
                and min(opened, close) - low > limit + 1e-9
            ):
                reasons.append("low")
                copied["l"] = round(min(opened, close) - limit, 4)
            if reasons:
                flagged.append({
                    "t": copied.get("t"),
                    "t_seconds": ts_seconds(copied.get("t")),
                    "field": "+".join(reasons),
                    "high": high,
                    "low": low,
                    "clamped_high": copied.get("h"),
                    "clamped_low": copied.get("l"),
                    "limit": round(limit, 6) if limit is not None else None,
                    "symbol": str(symbol or "").upper(),
                })
            clean.append(copied)
            if tr is not None and tr >= 0:
                ranges.append(tr)
            if close is not None:
                prev_close = close
    except Exception:
        logger.debug("bad tick filter skipped", exc_info=True)
        return {"bars": [dict(b) for b in bars or [] if isinstance(b, dict)], "flagged": [], "flagged_count": 0}
    return {"bars": clean, "flagged": flagged, "flagged_count": len(flagged)}


def confirmed_bad_ticks(bars: Optional[Sequence[Any]], symbol: Any = "") -> Dict[str, Any]:
    """Shadow-only two-bar confirm. A bar counts only if the next bar also deviates.

    This does not clamp, drop, or replace a bar. Callers log the count and
    leave the series alone.
    """
    report = filter_bad_ticks(bars, symbol)
    deviant = set()
    by_ts: Dict[float, Dict[str, Any]] = {}
    for item in report.get("flagged") or []:
        if not isinstance(item, dict):
            continue
        stamp = item.get("t_seconds")
        if stamp is None:
            stamp = ts_seconds(item.get("t"))
        if stamp is None:
            continue
        key = round(float(stamp), 3)
        deviant.add(key)
        by_ts[key] = item
    ordered: List[Tuple[float, Dict[str, Any]]] = []
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        stamp = ts_seconds(bar.get("t"))
        if stamp is None:
            continue
        ordered.append((round(float(stamp), 3), bar))
    ordered.sort(key=lambda item: item[0])
    confirmed: List[Dict[str, Any]] = []
    for index, (stamp, _bar) in enumerate(ordered[:-1]):
        nxt = ordered[index + 1][0]
        if stamp in deviant and nxt in deviant:
            item = dict(by_ts[stamp])
            item["confirmed_by_next"] = True
            confirmed.append(item)
    return {"confirmed": confirmed, "confirmed_count": len(confirmed)}


def series_for_extremes(bars: Optional[Sequence[Any]], symbol: Any = "") -> Dict[str, Any]:
    """Raw highs and lows unless ``BAD_TICK_FILTER_ENABLED`` is true.

    The filtered series is always computed so the shadow count exists either
    way. Enabling the flag publishes the clamped highs and lows. It does not
    change ``vw`` or ``v``.
    """
    report = filter_bad_ticks(bars, symbol)
    applied = bad_tick_filter_enabled()
    return {
        "applied": applied,
        "bars": report["bars"] if applied else [dict(b) for b in bars or [] if isinstance(b, dict)],
        "flagged": report["flagged"],
        "flagged_count": report["flagged_count"],
    }


def snapshot_gap_pct(open_px: Any, prev_close: Any) -> Optional[float]:
    """Gap percent from the official open and the prior close.

    Same rounding as the bot's ``gap_pct``: three decimals. A missing or
    non-positive print is unmeasured. This function has no bar-open argument
    on purpose: the first 1-minute bar open is not the official open.
    """
    opened = _price(open_px)
    prev = _price(prev_close)
    if opened is None or prev is None:
        return None
    gap = (opened - prev) / prev * 100.0
    if not math.isfinite(gap):
        return None
    return round(gap, 3)


def official_prints(quote: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(quote, dict):
        return None, None
    return _price(quote.get("massive_official_open")), _price(quote.get("massive_official_prev_close"))


def apply_gap_source(
    quote: Any,
    verdict: Any,
    reread: Callable[[float, float], Any],
) -> Any:
    """Keep the current gap verdict unless ``GAP_SOURCE=snapshot``.

    Snapshot mode re-runs the caller's own verifier on day.o and prevDay.c.
    It does not skip that verifier and it does not add a block of its own.
    A missing snapshot leaves the current verdict in place. Default returns
    the same verdict object.
    """
    if not isinstance(verdict, dict):
        return verdict
    current = verdict.get("gap_pct")
    opened, prev = official_prints(quote)
    snap = snapshot_gap_pct(opened, prev)
    selected = "current"
    fallback = None
    out: Any = verdict
    if gap_source_flag() == "snapshot":
        if opened is None or prev is None or snap is None:
            fallback = "missing"
        else:
            try:
                nxt = reread(float(opened), float(prev))
            except Exception:
                nxt = None
                fallback = "verifier_error"
            if isinstance(nxt, dict):
                out = nxt
                selected = "snapshot"
            elif fallback is None:
                fallback = "verifier_error"
    try:
        note_gap(quote, current, snap, selected, fallback)
    except Exception:
        logger.debug("gap shadow skipped", exc_info=True)
    return out


def classify_http_dependency(
    status_code: Any,
    *,
    has_data: bool,
    age_s: Any = None,
    stale_after_s: float = BAR_VWAP_STALE_S,
    kind: str = "bars",
) -> Dict[str, Any]:
    """Map an HTTP result to ok or no_data. Never raises.

    401 and 403 are unauthorized and forbidden. A 200 with no usable
    payload is missing. A usable payload older than ``stale_after_s`` is
    stale. Anything else that is not a 200 is an error. IV reasons keep
    an ``iv_`` prefix so a combined row can name both dependencies.
    """
    prefix = "iv_" if str(kind) == "iv" else ""
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        code = 0
    if code == 401:
        return {"status": SHADOW_NO_DATA, "reason": prefix + "unauthorized"}
    if code == 403:
        return {"status": SHADOW_NO_DATA, "reason": prefix + "forbidden"}
    if code != 200:
        return {"status": SHADOW_NO_DATA, "reason": prefix + "error"}
    if not has_data:
        return {"status": SHADOW_NO_DATA, "reason": prefix + "missing"}
    age = _finite(age_s)
    if age is not None and (age < 0 or age > float(stale_after_s)):
        return {"status": SHADOW_NO_DATA, "reason": prefix + "stale"}
    return {"status": SHADOW_OK, "reason": None}


def dependency_row(*parts: Dict[str, Any]) -> Dict[str, Any]:
    """Fail closed if any dependency is not ok. Reasons stay in order.

    The sampled VWAP is not a part. Callers pass only Massive results.
    """
    reasons: List[str] = []
    for part in parts:
        if not isinstance(part, dict):
            reasons.append("unmeasured")
            continue
        if part.get("status") == SHADOW_OK and not part.get("reason"):
            continue
        reason = part.get("reason")
        reasons.append(str(reason) if reason else "unmeasured")
    if reasons:
        return {"status": SHADOW_NO_DATA, "reason": "+".join(reasons)}
    return {"status": SHADOW_OK, "reason": None}


def bars_feed() -> Dict[str, Any]:
    feed = _state().get("bars_feed")
    return dict(feed) if isinstance(feed, dict) else _feed("unmeasured")


def iv_feed() -> Dict[str, Any]:
    feed = _state().get("iv_feed")
    return dict(feed) if isinstance(feed, dict) else _feed("unmeasured")


def entitlement_session() -> Optional[str]:
    raw = _state().get("entitlement_session")
    return str(raw) if raw else None


def note_bars_feed(result: Dict[str, Any], *, session: Optional[str] = None, probed_at: Any = None) -> None:
    state = _state()
    state["bars_feed"] = {
        "status": SHADOW_OK if result.get("status") == SHADOW_OK else SHADOW_NO_DATA,
        "reason": None if result.get("status") == SHADOW_OK else (result.get("reason") or "unmeasured"),
    }
    if session:
        state["entitlement_session"] = str(session)
    if probed_at is not None:
        state["entitlement_probed_at"] = probed_at


def note_iv_feed(result: Dict[str, Any], *, session: Optional[str] = None, probed_at: Any = None) -> None:
    state = _state()
    state["iv_feed"] = {
        "status": SHADOW_OK if result.get("status") == SHADOW_OK else SHADOW_NO_DATA,
        "reason": None if result.get("status") == SHADOW_OK else (result.get("reason") or "iv_unmeasured"),
    }
    if session:
        state["entitlement_session"] = str(session)
    if probed_at is not None:
        state["entitlement_probed_at"] = probed_at


def note_symbol_bars(symbol: str, result: Dict[str, Any]) -> None:
    sym = str(symbol or "").upper().strip() or "?"
    _state()["symbol_bars"][sym] = {
        "status": SHADOW_OK if result.get("status") == SHADOW_OK else SHADOW_NO_DATA,
        "reason": None if result.get("status") == SHADOW_OK else (result.get("reason") or "missing"),
    }


def symbol_bars(symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip() or "?"
    row = (_state().get("symbol_bars") or {}).get(sym)
    return dict(row) if isinstance(row, dict) else _feed("missing")


def iv_field(symbol: Optional[str] = None) -> Dict[str, Any]:
    """The IV check alone. A failure nulls ``iv`` and does not name other rows."""
    specific = None
    if symbol:
        specific = (_state().get("symbol_iv") or {}).get(str(symbol).upper())
    feed = specific if isinstance(specific, dict) else iv_feed()
    ok = feed.get("status") == SHADOW_OK
    return {
        "status": SHADOW_OK if ok else SHADOW_NO_DATA,
        "reason": None if ok else feed.get("reason"),
        "iv": feed.get("value") if ok else None,
    }


def note_symbol_iv(symbol: str, result: Dict[str, Any]) -> None:
    """One IV check. Other symbols and other shadow fields stay as they are."""
    sym = str(symbol or "").upper().strip() or "?"
    ok = result.get("status") == SHADOW_OK
    _state().setdefault("symbol_iv", {})[sym] = {
        "status": SHADOW_OK if ok else SHADOW_NO_DATA,
        "reason": None if ok else (result.get("reason") or "iv_missing"),
        "value": result.get("value") if ok else None,
    }


def research_status(
    *,
    symbol: Optional[str] = None,
    needs_symbol_bars: bool = True,
    needs_iv: bool = False,
) -> Dict[str, Any]:
    """Bar dependency for one shadow metric. IV is not included.

    A bad IV check nulls the IV field only. It does not blank bar VWAP, gap,
    or bad-tick rows. ``needs_iv`` remains for a caller that asks explicitly.
    """
    parts = [bars_feed()]
    if needs_symbol_bars and symbol:
        parts.append(symbol_bars(symbol))
    if needs_iv:
        parts.append(iv_feed())
    return dependency_row(*parts)


def bars_allowed_for_gate(symbol: str, *, bar_vwap: Any, bar_age_s: Any, bar_count: int) -> bool:
    """Opt-in bar gate input only when this symbol's Massive bars are fresh.

    IV does not move the CALL gate. A 401/403, a missing bar, or a stale
    bar leaves the gate on the sampled path it already computed.
    """
    if bars_feed().get("status") != SHADOW_OK:
        return False
    if symbol_bars(symbol).get("status") != SHADOW_OK:
        return False
    if _price(bar_vwap) is None or int(bar_count or 0) <= 0:
        return False
    age = _finite(bar_age_s)
    if age is None or age < 0 or age > BAR_VWAP_STALE_S:
        return False
    return True


def classify_aggs_payload(payload: Any, now_et: datetime, *, status_code: int = 200) -> Dict[str, Any]:
    """Entitlement of one aggs response. Completed-bar age over 150s is stale."""
    if int(status_code or 0) != 200:
        return classify_http_dependency(status_code, has_data=False, kind="bars")
    results = payload.get("results") if isinstance(payload, dict) else None
    view = completed_rth_vwap(results if isinstance(results, list) else [], now_et)
    return classify_http_dependency(
        200,
        has_data=view.get("vwap") is not None and int(view.get("bar_count") or 0) > 0,
        age_s=view.get("age_seconds"),
        stale_after_s=BAR_VWAP_STALE_S,
        kind="bars",
    )


def classify_iv_payload(payload: Any, now_epoch: float, *, status_code: int = 200) -> Dict[str, Any]:
    """Entitlement of one options-snapshot page. IV must be present and fresh.

    Quote age uses the newest ``last_quote.last_updated`` on a row that has
    implied volatility. No timestamp is not stale. No positive IV is missing.
    """
    if int(status_code or 0) != 200:
        return classify_http_dependency(status_code, has_data=False, kind="iv")
    results = payload.get("results") if isinstance(payload, dict) else None
    if isinstance(results, dict):
        results = [results]
    found = False
    freshest: Optional[float] = None
    for item in results or []:
        if not isinstance(item, dict):
            continue
        iv = _price(item.get("implied_volatility"))
        if iv is None:
            continue
        found = True
        quote = item.get("last_quote") if isinstance(item.get("last_quote"), dict) else {}
        stamp = ts_seconds(quote.get("last_updated"))
        if stamp is None:
            continue
        try:
            age = float(now_epoch) - float(stamp)
        except (TypeError, ValueError):
            continue
        if freshest is None or age < freshest:
            freshest = age
    return classify_http_dependency(
        200,
        has_data=found,
        age_s=freshest,
        stale_after_s=_IV_STALE_S,
        kind="iv",
    )


def entitlement_warning_fields() -> Dict[str, Any]:
    """The only fields the probe is allowed to log. No URL and no secret."""
    bars = bars_feed()
    iv = iv_feed()
    return {
        "session": entitlement_session(),
        "bars": bars.get("status"),
        "bars_reason": bars.get("reason"),
        "iv": iv.get("status"),
        "iv_reason": iv.get("reason"),
    }


def note_evaluation() -> None:
    _state()["evaluations"] = int(_state()["evaluations"]) + 1


def _local_bar_part(choice: Dict[str, Any]) -> Dict[str, Any]:
    """This symbol's completed bars, independent of the sampled gate value."""
    bar = _price(choice.get("bar_vwap"))
    count = int(choice.get("bar_count") or 0)
    age = _finite(choice.get("bar_age_s"))
    if bar is None or count <= 0:
        return {"status": SHADOW_NO_DATA, "reason": "missing"}
    if age is None or age < 0 or age > BAR_VWAP_STALE_S:
        return {"status": SHADOW_NO_DATA, "reason": "stale"}
    return {"status": SHADOW_OK, "reason": None}


def publish_vwap_row(symbol: str, choice: Dict[str, Any]) -> Dict[str, Any]:
    """Shadow VWAP row. no_data does not copy the sampled VWAP into bar_vwap.

    ``live_gate_vwap`` is the gate's own input, recorded so a reader can see
    it was left alone. It is not a Massive result and it is not a fallback.
    """
    sym = str(symbol or "").upper().strip() or "?"
    local = _local_bar_part(choice if isinstance(choice, dict) else {})
    gate = dependency_row(bars_feed(), local)
    measured = gate["status"] == SHADOW_OK
    bar = _price((choice or {}).get("bar_vwap")) if measured else None
    sampled = _price((choice or {}).get("sampled_vwap")) if measured else None
    diff = diff_bps(bar, sampled) if measured else None
    last = _price((choice or {}).get("last"))
    if measured and last is not None and bar is not None:
        bar_verdict = "above" if last + 1e-9 >= bar else "below"
    else:
        bar_verdict = "unmeasured"
    iv = iv_field(sym)
    return {
        "symbol": sym,
        "status": gate["status"],
        "reason": gate["reason"],
        "bar_vwap": bar,
        "bar_verdict": bar_verdict,
        "sampled_vwap": sampled,
        "diff_bps": diff,
        "live_gate_vwap": _price((choice or {}).get("gate_vwap")),
        "bar_age_seconds": None if not measured else (choice or {}).get("bar_age_s"),
        "bar_count": 0 if not measured else int((choice or {}).get("bar_count") or 0),
        "configured_source": (choice or {}).get("configured_source"),
        "selected": "bars" if measured else None,
        "iv_status": iv["status"],
        "iv_reason": iv["reason"],
        "iv": iv["iv"],
    }


def note_vwap(symbol: str, choice: Dict[str, Any]) -> None:
    """Remember one gate evaluation. A failed dependency is status=no_data."""
    sym = str(symbol or "").upper().strip() or "?"
    state = _state()
    row = publish_vwap_row(sym, choice)
    if row["status"] != SHADOW_OK:
        state["no_data"] = int(state.get("no_data") or 0) + 1
        reason = str(row.get("reason") or "")
        if "stale" in reason:
            state["fallback_stale"] = int(state["fallback_stale"]) + 1
        if "missing" in reason:
            state["fallback_missing"] = int(state["fallback_missing"]) + 1
    elif isinstance(row.get("diff_bps"), (int, float)) and not isinstance(row.get("diff_bps"), bool):
        previous = state.get("max_abs_vwap_diff_bps")
        magnitude = abs(float(row["diff_bps"]))
        if previous is None or magnitude > float(previous):
            state["max_abs_vwap_diff_bps"] = round(magnitude, 2)
    state["symbols"][sym] = row
    logger.info(
        "[DATA QUALITY] vwap %s status=%s reason=%s bar=%s sampled=%s diff_bps=%s live_gate=%s bar_age_s=%s",
        sym,
        row["status"],
        row["reason"],
        _fmt(row.get("bar_vwap")),
        _fmt(row.get("sampled_vwap")),
        row.get("diff_bps"),
        _fmt(row.get("live_gate_vwap")),
        row.get("bar_age_seconds"),
    )


def note_gap(
    quote: Any,
    current_pct: Any,
    snapshot_pct: Any,
    selected: str,
    fallback: Optional[str],
) -> None:
    """Shadow gap row. A missing snapshot nulls that field only.

    The live gap verdict is kept. A bad IV check does not blank it.
    """
    sym = "?"
    if isinstance(quote, dict):
        sym = str(quote.get("symbol") or "").upper().strip() or "?"
    state = _state()
    state["gap_pairs"] = int(state["gap_pairs"]) + 1
    if fallback == "missing":
        state["gap_fallback_missing"] = int(state["gap_fallback_missing"]) + 1
    if snapshot_pct is None:
        state["no_data"] = int(state.get("no_data") or 0) + 1
        published_snapshot = None
        reason = "missing"
        status = SHADOW_NO_DATA
        published_selected = "current"
    else:
        published_snapshot = snapshot_pct
        published_selected = selected
        reason = None
        status = SHADOW_OK
    state["gaps"][sym] = {
        "status": status,
        "reason": reason,
        "current_gap_pct": current_pct,
        "snapshot_gap_pct": published_snapshot,
        "selected": published_selected,
        "fallback": fallback,
        "live_gap_pct": current_pct,
    }
    logger.info(
        "[DATA QUALITY] gap %s status=%s reason=%s current=%s snapshot=%s selected=%s",
        sym,
        status,
        reason,
        current_pct,
        published_snapshot,
        published_selected,
    )


def note_bad_ticks(symbol: str, flagged: Sequence[Dict[str, Any]]) -> None:
    """Record flagged bar timestamps. The same bar is not counted twice.

    The count stays on the ledger and is published only while bars and IV
    are both ok. A later no_data feed does not turn the count into a
    measurement.
    """
    sym = str(symbol or "").upper().strip() or "?"
    state = _state()
    seen = set(state["bad_tick_ids"].get(sym) or [])
    details: List[Dict[str, Any]] = list(state["bad_tick_details"])
    for item in flagged or []:
        if not isinstance(item, dict):
            continue
        stamp = item.get("t_seconds")
        if stamp is None:
            stamp = ts_seconds(item.get("t"))
        if stamp is None:
            continue
        key = round(float(stamp), 3)
        if key in seen:
            continue
        seen.add(key)
        details.append({
            "symbol": sym,
            "status": SHADOW_OK,
            "t_seconds": key,
            "field": item.get("field"),
            "high": item.get("high"),
            "low": item.get("low"),
        })
    state["bad_tick_ids"][sym] = sorted(seen)
    state["bad_tick_details"] = details[-20:]


def note_bad_tick_confirmed(symbol: str, confirmed: Sequence[Dict[str, Any]]) -> None:
    """Record two-bar confirmations. Logging only. Nothing here changes a bar."""
    sym = str(symbol or "").upper().strip() or "?"
    state = _state()
    seen = set(state["bad_tick_confirmed_ids"].get(sym) or [])
    details: List[Dict[str, Any]] = list(state.get("bad_tick_confirmed_details") or [])
    added = 0
    for item in confirmed or []:
        if not isinstance(item, dict):
            continue
        stamp = item.get("t_seconds")
        if stamp is None:
            stamp = ts_seconds(item.get("t"))
        if stamp is None:
            continue
        key = round(float(stamp), 3)
        if key in seen:
            continue
        seen.add(key)
        added += 1
        details.append({
            "symbol": sym,
            "t_seconds": key,
            "field": item.get("field"),
            "high": item.get("high"),
            "low": item.get("low"),
            "confirmed_by_next": True,
        })
    state["bad_tick_confirmed_ids"][sym] = sorted(seen)
    state["bad_tick_confirmed_details"] = details[-20:]
    if added:
        logger.info(
            "[DATA QUALITY] bad tick confirmed symbol=%s count=%s",
            sym, len(seen),
        )


def note_agg_request(symbol: str, from_token: str, to_token: str) -> None:
    """Count one GET. The tokens are a date or a bar time, never a key."""
    sym = str(symbol or "").upper().strip() or "?"
    bucket = _state()["agg_requests"]
    row = bucket.get(sym) or {"count": 0, "last_from": None, "last_to": None}
    row["count"] = int(row["count"]) + 1
    row["last_from"] = str(from_token)
    row["last_to"] = str(to_token)
    bucket[sym] = row


def requests_per_hour(spacing_s: float) -> float:
    if spacing_s <= 0:
        return 0.0
    return 3600.0 / float(spacing_s)


def call_budget(symbols: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    """Agg GET budget. One request per symbol per pull.

    The default is 60s on the minute boundary: 60/hour/symbol and 120/hour
    for SPY and QQQ. The old 90s elapsed throttle is the 24–40/hour band.
    Incremental merge is off, so each pull is the full session day.
    """
    names = [str(s).upper() for s in (symbols or ("SPY", "QQQ")) if s]
    count = len(names) or 2
    slow = requests_per_hour(OBSERVED_PULL_SPACING_S[1])
    fast = requests_per_hour(OBSERVED_PULL_SPACING_S[0])
    legacy_ceiling = requests_per_hour(LEGACY_AGGS_PULL_INTERVAL_S)
    aligned = requests_per_hour(DEFAULT_AGGS_PULL_INTERVAL_S)
    return {
        "symbols": names,
        "requests_per_pull_per_symbol": 1,
        "current_interval_s": DEFAULT_AGGS_PULL_INTERVAL_S,
        "current_observed_spacing_s": "60",
        "current_requests_per_hour_per_symbol": int(round(aligned)),
        "current_requests_per_hour": int(round(aligned * count)),
        "legacy_90s_requests_per_hour_per_symbol": "24-40",
        "legacy_90s_requests_per_hour": f"{int(round(slow * count))}-{int(round(fast * count))}",
        "current_throttle_ceiling_per_hour_per_symbol": int(round(legacy_ceiling)),
        "proposal_interval_s": DEFAULT_AGGS_PULL_INTERVAL_S,
        "proposal_align_after_close_s": AGGS_ALIGN_AFTER_CLOSE_S,
        "proposal_requests_per_hour_per_symbol": int(round(aligned)),
        "proposal_requests_per_hour": int(round(aligned * count)),
        "incremental_merge": aggs_incremental_merge_enabled(),
        "note": (
            "One GET per symbol per pull. Unset AGGS_PULL_INTERVAL_S is 60s, "
            "aligned to the minute boundary (60/hour/symbol, 120/hour for "
            "SPY+QQQ). AGGS_PULL_INTERVAL_S=90 restores the old elapsed "
            "throttle. AGGS_INCREMENTAL_MERGE defaults off: every pull is the "
            "full session day and the live cross-check recomputes from that "
            "response. The session VWAP seed is a separate fetch and stops "
            "once the open is stored."
        ),
    }


def _publish_bad_ticks(state: Dict[str, Any]) -> Dict[str, Any]:
    """Single-bar count and the two-bar confirm count. IV does not clear them."""
    flagged = sum(len(ids) for ids in (state.get("bad_tick_ids") or {}).values())
    confirmed = sum(len(ids) for ids in (state.get("bad_tick_confirmed_ids") or {}).values())
    return {
        "bad_ticks_flagged": int(flagged),
        "bad_tick_details": list(state.get("bad_tick_details") or []),
        "bad_tick_confirmed": int(confirmed),
        "bad_tick_confirmed_details": list(state.get("bad_tick_confirmed_details") or []),
    }


def bar_vwap_verdict() -> Dict[str, Any]:
    """Latest per-symbol bar-VWAP rows for an admitted entry. Read-only."""
    symbols = _state().get("symbols") or {}
    out: Dict[str, Any] = {}
    for sym, row in symbols.items():
        if not isinstance(row, dict):
            continue
        out[sym] = {
            "status": row.get("status"),
            "reason": row.get("reason"),
            "bar_vwap": row.get("bar_vwap"),
            "bar_verdict": row.get("bar_verdict"),
            "diff_bps": row.get("diff_bps"),
            "iv": row.get("iv"),
            "iv_status": row.get("iv_status"),
        }
    return out


def shadow_status() -> Dict[str, Any]:
    """Read-only snapshot for /pro-trader/status. Does not fetch or trade."""
    state = _state()
    per_symbol = {
        sym: int((row or {}).get("count") or 0)
        for sym, row in (state.get("agg_requests") or {}).items()
    }
    ticks = _publish_bad_ticks(state)
    feed = bars_feed()
    iv = iv_field()
    return {
        "read_only": True,
        "available": True,
        "decisions_unchanged_at_defaults": True,
        "status": feed["status"],
        "reason": feed["reason"],
        "vwap_source": vwap_source_flag(),
        "vwap_source_read": "runtime",
        "gap_source": gap_source_flag(),
        "gap_source_read": "runtime",
        "bad_tick_filter_enabled": bad_tick_filter_enabled(),
        "bad_tick_filter_read": "runtime",
        "bad_tick_k": bad_tick_k(),
        "bad_tick_k_read": "runtime",
        "bad_tick_index_bps": BAD_TICK_INDEX_BPS,
        "bar_stale_after_s": BAR_VWAP_STALE_S,
        "aggs_pull_interval_s": aggs_pull_interval_s(),
        "aggs_pull_interval_read": "runtime",
        "aggs_alignment": aggs_alignment_active(),
        "aggs_align_after_close_s": AGGS_ALIGN_AFTER_CLOSE_S,
        "aggs_incremental_merge": aggs_incremental_merge_enabled(),
        "aggs_incremental_merge_read": "runtime",
        "evaluations": int(state.get("evaluations") or 0),
        "no_data": int(state.get("no_data") or 0),
        "fallback_stale": int(state.get("fallback_stale") or 0),
        "fallback_missing": int(state.get("fallback_missing") or 0),
        "max_abs_vwap_diff_bps": state.get("max_abs_vwap_diff_bps") if feed["status"] == SHADOW_OK else None,
        "gap_pairs": int(state.get("gap_pairs") or 0),
        "gap_fallback_missing": int(state.get("gap_fallback_missing") or 0),
        "bad_ticks_flagged": ticks["bad_ticks_flagged"],
        "bad_tick_details": ticks["bad_tick_details"],
        "bad_tick_confirmed": ticks["bad_tick_confirmed"],
        "bad_tick_confirmed_details": ticks["bad_tick_confirmed_details"],
        "iv_check": iv,
        "agg_requests": per_symbol,
        "entitlement": {
            "session": entitlement_session(),
            "probed_at": state.get("entitlement_probed_at"),
            "bars": bars_feed(),
            "iv": iv_feed(),
        },
        "symbols": {key: dict(value) for key, value in (state.get("symbols") or {}).items()},
        "gaps": {key: dict(value) for key, value in (state.get("gaps") or {}).items()},
        "call_budget": call_budget(),
    }
