"""Shadow data-quality measurements. Decisions stay on the current inputs.

Four switches, each read from the environment on every use (not at import):

- ``VWAP_SOURCE`` ``sampled`` (default) or ``bars``
- ``AGGS_PULL_INTERVAL_S`` unset (default) keeps the 90s elapsed throttle
- ``BAD_TICK_FILTER_ENABLED`` ``false`` (default)
- ``GAP_SOURCE`` ``current`` (default) or ``snapshot``

The CALL index gate stays fail-closed. A missing or stale bar VWAP falls
back to the sampled gate input. It does not allow the CALL. Official gap
math uses the snapshot day open and previous close only. The first 1-minute
bar open is not an input. Nothing here places an order or reads a secret.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("etrade-bot.data-quality")

# Shadow research fails closed. A row is ``no_data`` when Massive 1-minute
# bars or IV (where the research depends on it) are missing, stale, or
# rejected with 401/403. The sampled VWAP and Yahoo volume are not substitutes.
SHADOW_OK = "ok"
SHADOW_NO_DATA = "no_data"
_IV_STALE_S = 90.0
_AUTH_REASONS = frozenset({"unauthorized", "forbidden", "not_configured"})

# Current refresh throttle in main_bot (_MINUTE_VWAP_REFRESH_S).
DEFAULT_AGGS_PULL_INTERVAL_S = 90.0
# Proposal cadence. Not the default.
PROPOSAL_AGGS_PULL_INTERVAL_S = 60.0
# Pulls that track the minute wait until this many seconds after it closes.
AGGS_ALIGN_AFTER_CLOSE_S = 5
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


def aggs_pull_interval_s() -> float:
    """Seconds between agg pulls. Unset keeps the 90s throttle. Read each check."""
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
    """Phase-lock only when the operator sets an interval of 60s or less.

    The unset default stays an elapsed 90s throttle, with no clock phase.
    """
    raw = os.getenv("AGGS_PULL_INTERVAL_S", "")
    if not str(raw if raw is not None else "").strip():
        return False
    interval = aggs_pull_interval_s() if interval_s is None else float(interval_s)
    return interval <= PROPOSAL_AGGS_PULL_INTERVAL_S


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


def agg_path_range(day: str, last_bar_epoch: Optional[float]) -> Tuple[str, str]:
    """One request per symbol. The first pull of the session is the day.

    Later pulls set ``from`` to the last bar's millisecond timestamp so the
    response is that bar plus anything newer. ``to`` stays the session date.
    """
    session = str(day or "").strip()
    epoch = _finite(last_bar_epoch)
    if not session:
        return session, session
    if epoch is None or epoch <= 0:
        return session, session
    return str(int(round(epoch * 1000.0))), session


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

    ``bars`` replaces the gate input only when the completed-bar VWAP exists
    and the newest completed bar is not older than ``stale_after_s`` (150s).
    Stale or missing falls back to the sampled input and says so. The caller
    still fail-closes when that input is unmeasured.
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
    }
    if selected_flag != "bars":
        return choice
    missing = bar is None or int(bar_count or 0) <= 0
    stale = (not missing) and (age is None or age < 0 or age > float(stale_after_s))
    if missing or stale:
        choice["fallback"] = "missing" if missing else "stale"
        choice["selected"] = "sampled"
        choice["replace_gate"] = False
        choice["applied_gate_vwap"] = gate
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


def research_status(
    *,
    symbol: Optional[str] = None,
    needs_symbol_bars: bool = True,
    needs_iv: bool = True,
) -> Dict[str, Any]:
    """Every shadow metric fails closed unless its Massive dependencies are ok.

    Bars are always required at the feed. A symbol metric also requires that
    symbol's own completed bars. IV is required for the shadow research
    (``needs_iv``). Neither sampled VWAP nor Yahoo volume is consulted.
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
    gate = dependency_row(bars_feed(), local, iv_feed())
    measured = gate["status"] == SHADOW_OK
    bar = _price((choice or {}).get("bar_vwap")) if measured else None
    sampled = _price((choice or {}).get("sampled_vwap")) if measured else None
    diff = diff_bps(bar, sampled) if measured else None
    return {
        "symbol": sym,
        "status": gate["status"],
        "reason": gate["reason"],
        "bar_vwap": bar,
        "sampled_vwap": sampled,
        "diff_bps": diff,
        "live_gate_vwap": _price((choice or {}).get("gate_vwap")),
        "bar_age_seconds": None if not measured else (choice or {}).get("bar_age_s"),
        "bar_count": 0 if not measured else int((choice or {}).get("bar_count") or 0),
        "configured_source": (choice or {}).get("configured_source"),
        "selected": "bars" if measured else None,
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
    """Shadow gap row. A bad bars or IV dependency blanks the snapshot gap.

    The current gap is the live verdict's number. It is not copied into
    ``snapshot_gap_pct`` when the research dependency fails.
    """
    sym = "?"
    if isinstance(quote, dict):
        sym = str(quote.get("symbol") or "").upper().strip() or "?"
    state = _state()
    state["gap_pairs"] = int(state["gap_pairs"]) + 1
    if fallback == "missing":
        state["gap_fallback_missing"] = int(state["gap_fallback_missing"]) + 1
    # Gap does not read 1-minute bars for the formula, but the research
    # bundle is fail-closed when the bars feed or IV feed is not ok.
    gate = research_status(needs_symbol_bars=False, needs_iv=True)
    if gate["status"] != SHADOW_OK:
        state["no_data"] = int(state.get("no_data") or 0) + 1
        published_snapshot = None
        published_selected = None
        reason = gate["reason"]
        status = SHADOW_NO_DATA
    elif snapshot_pct is None:
        state["no_data"] = int(state.get("no_data") or 0) + 1
        published_snapshot = None
        published_selected = None
        reason = "missing"
        status = SHADOW_NO_DATA
    else:
        published_snapshot = snapshot_pct
        published_selected = selected
        reason = None
        status = SHADOW_OK
    state["gaps"][sym] = {
        "status": status,
        "reason": reason,
        "current_gap_pct": current_pct if status == SHADOW_OK else None,
        "snapshot_gap_pct": published_snapshot,
        "selected": published_selected,
        "fallback": None if status == SHADOW_NO_DATA else fallback,
        "live_gap_pct": current_pct,
    }
    logger.info(
        "[DATA QUALITY] gap %s status=%s reason=%s current=%s snapshot=%s selected=%s",
        sym,
        status,
        reason,
        current_pct if status == SHADOW_OK else None,
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
    """Agg GET budget. One request per symbol per pull, both cadences.

    Today the code throttle is 90s (40/hour/symbol if the loop hits it
    exactly). Quant Lab measured pulls every ~90–150s, which is 24–40
    requests/hour/symbol. SPY and QQQ together are 48–80/hour. At the
    proposal 60s cadence that is 60/hour/symbol and 120/hour for the two
    index names. The incremental ``from`` does not add a second request; it
    shortens the payload after the first full-session pull.
    """
    names = [str(s).upper() for s in (symbols or ("SPY", "QQQ")) if s]
    count = len(names) or 2
    slow = requests_per_hour(OBSERVED_PULL_SPACING_S[1])
    fast = requests_per_hour(OBSERVED_PULL_SPACING_S[0])
    ceiling = requests_per_hour(DEFAULT_AGGS_PULL_INTERVAL_S)
    proposal = requests_per_hour(PROPOSAL_AGGS_PULL_INTERVAL_S)
    return {
        "symbols": names,
        "requests_per_pull_per_symbol": 1,
        "current_interval_s": DEFAULT_AGGS_PULL_INTERVAL_S,
        "current_observed_spacing_s": "90-150",
        "current_requests_per_hour_per_symbol": "24-40",
        "current_requests_per_hour": f"{int(round(slow * count))}-{int(round(fast * count))}",
        "current_throttle_ceiling_per_hour_per_symbol": int(round(ceiling)),
        "proposal_interval_s": PROPOSAL_AGGS_PULL_INTERVAL_S,
        "proposal_align_after_close_s": AGGS_ALIGN_AFTER_CLOSE_S,
        "proposal_requests_per_hour_per_symbol": int(round(proposal)),
        "proposal_requests_per_hour": int(round(proposal * count)),
        "note": (
            "One GET per symbol per pull. Unset AGGS_PULL_INTERVAL_S keeps the "
            "90s elapsed throttle (no :05 phase). Setting 60 aligns the next "
            "pull to about 5s after the minute closes and still sends one "
            "request per symbol. The first pull of a session is the session "
            "date; later pulls set from to the last bar timestamp. The session "
            "VWAP seed is a separate fetch and stops once the open is stored."
        ),
    }


def _publish_bad_ticks(state: Dict[str, Any]) -> Dict[str, Any]:
    """Hide the filtered count while bars or IV are no_data. Do not invent 0."""
    gate = research_status(needs_symbol_bars=False, needs_iv=True)
    if gate["status"] != SHADOW_OK:
        return {
            "status": SHADOW_NO_DATA,
            "reason": gate["reason"],
            "bad_ticks_flagged": None,
            "bad_tick_details": [],
        }
    flagged = sum(len(ids) for ids in (state.get("bad_tick_ids") or {}).values())
    return {
        "status": SHADOW_OK,
        "reason": None,
        "bad_ticks_flagged": int(flagged),
        "bad_tick_details": list(state.get("bad_tick_details") or []),
    }


def shadow_status() -> Dict[str, Any]:
    """Read-only snapshot for /pro-trader/status. Does not fetch or trade."""
    state = _state()
    per_symbol = {
        sym: int((row or {}).get("count") or 0)
        for sym, row in (state.get("agg_requests") or {}).items()
    }
    ticks = _publish_bad_ticks(state)
    feed = research_status(needs_symbol_bars=False, needs_iv=True)
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
        "evaluations": int(state.get("evaluations") or 0),
        "no_data": int(state.get("no_data") or 0),
        "fallback_stale": int(state.get("fallback_stale") or 0),
        "fallback_missing": int(state.get("fallback_missing") or 0),
        "max_abs_vwap_diff_bps": state.get("max_abs_vwap_diff_bps") if feed["status"] == SHADOW_OK else None,
        "gap_pairs": int(state.get("gap_pairs") or 0),
        "gap_fallback_missing": int(state.get("gap_fallback_missing") or 0),
        "bad_ticks_flagged": ticks["bad_ticks_flagged"],
        "bad_tick_status": ticks["status"],
        "bad_tick_reason": ticks["reason"],
        "bad_tick_details": ticks["bad_tick_details"],
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
