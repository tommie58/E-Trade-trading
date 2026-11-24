"""Today-only VWAP for the CALL gate, evaluate_quote, and the SPY range.

Frozen America/New_York clocks. The rolling scanner list lives in fakeredis.
Prior-day prints stay in the list. Readers that decide a CALL must not use them.
"""
from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import fakeredis.aioredis
import httpx
import pytest

import main_bot


ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 24, 9, 42, tzinfo=ET)
SPY_LAST = 716.25
QQQ_LAST = 638.70
SPY_TODAY = 714.64
QQQ_TODAY = 637.50
SPY_STALE = 722.40
QQQ_STALE = 650.09
SPY_RTH = 714.10
QQQ_RTH = 638.17


def _run(coro):
    return asyncio.run(coro)


def _prints(day, start_minute, count, price, vol0, step):
    rows = []
    for i in range(count):
        minute = start_minute + i
        ts = day.replace(hour=minute // 60, minute=minute % 60, second=0, microsecond=0)
        rows.append({
            "ts": ts.isoformat(),
            "last": price if not isinstance(price, list) else price[i],
            "total_volume": vol0 + i * step,
        })
    return rows


def _flat(day, start, count, price):
    return _prints(day, start, count, price, 1000, 1000)


def _prior(day, price):
    """22 prior-session prints. Volume dwarfs today so the raw list VWAP is this price."""
    return _prints(day, 14 * 60, 22, price, 1_000_000_000, 1_000_000_000)


def _quote(symbol, last, stale_vwap, day_vw):
    return {
        "Product": {"symbol": symbol},
        "quoteStatus": "REALTIME",
        "massive_day_vwap": day_vw,
        "massive_day_vwap_reason": None,
        "All": {"lastTrade": last, "vwap": stale_vwap},
    }


@pytest.fixture
def frozen(monkeypatch):
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._minute_vwap_refresh_meta.clear()
    yield
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._minute_vwap_refresh_meta.clear()


@pytest.fixture
def redis_scanner(frozen):
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = main_bot.StateStore(client)
    saved = {
        "scanner": main_bot.scanner,
        "state": main_bot.state,
        "fetch": main_bot._scanner_fetch_quotes,
    }

    async def fetch(symbols, *, dry_run=False):
        return {"QuoteResponse": {"QuoteData": [
            _quote("SPY", SPY_LAST, SPY_STALE, 100.0),
            _quote("QQQ", QQQ_LAST, QQQ_STALE, 100.0),
        ]}}

    async def noop(*_args, **_kwargs):
        return {}

    scan = main_bot.scanner_mod.Scanner(
        store, noop, noop, lambda: True, lambda: False, lambda: NOW,
    )
    main_bot.scanner = scan
    main_bot.state = store
    main_bot._scanner_fetch_quotes = fetch
    try:
        yield store, scan
    finally:
        main_bot.scanner = saved["scanner"]
        main_bot.state = saved["state"]
        main_bot._scanner_fetch_quotes = saved["fetch"]
        _run(client.aclose())


async def _seed(store, symbol, samples):
    """Write the capped list and the uncapped session hash, in order."""
    key = main_bot.scanner_mod.SERIES_KEY_FMT.format(symbol)
    for sample in samples:
        await store.list_push_capped(
            key, json.dumps(sample), max_len=main_bot.scanner_mod.MAX_SAMPLES,
        )
        await main_bot.scanner_mod.note_session_vwap(store, symbol, sample, None)


def _seed_rth(symbol, when, vwap):
    day = when.astimezone(ET).date().isoformat()
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key(symbol, day)] = {
        "vwap": vwap,
        "last_bar_epoch": when.timestamp(),
        "last_bar_ts": when.isoformat(),
        "bar_count": 12,
        "session_date": day,
    }


def _live_series():
    prior_day = datetime(2026, 11, 23, tzinfo=ET)
    today = datetime(2026, 11, 24, tzinfo=ET)
    spy = _prior(prior_day, SPY_STALE) + _flat(today, 9 * 60 + 30, 12, SPY_TODAY)
    qqq = _prior(prior_day, QQQ_STALE) + _flat(today, 9 * 60 + 30, 12, QQQ_TODAY)
    return spy, qqq


def _agree():
    _seed_rth("SPY", NOW, SPY_RTH)
    _seed_rth("QQQ", NOW, QQQ_RTH)


def test_default_sample_floor_is_ten_minutes_of_cadence(monkeypatch):
    monkeypatch.delenv("VWAP_MIN_TODAY_SAMPLES", raising=False)
    monkeypatch.setenv("SCANNER_INTERVAL_SECONDS", "30")
    saved = main_bot.scanner_mod._LIVE_INTERVAL_SECONDS
    main_bot.scanner_mod._LIVE_INTERVAL_SECONDS = None
    try:
        # The env seed is not the live cadence. The default interval is 60s.
        assert main_bot.scanner_mod.vwap_min_today_samples() == 10
        main_bot.scanner_mod.note_live_interval(30)
        assert main_bot.scanner_mod.vwap_min_today_samples() == 20
        monkeypatch.setenv("VWAP_MIN_TODAY_SAMPLES", "7")
        assert main_bot.scanner_mod.vwap_min_today_samples() == 7
    finally:
        main_bot.scanner_mod._LIVE_INTERVAL_SECONDS = saved
    monkeypatch.setenv("VWAP_CROSSCHECK_MAX_BPS", "25")
    assert main_bot.vwap_crosscheck_max_bps() == 25
    assert main_bot.vwap_crosscheck_required() is True
    monkeypatch.delenv("VWAP_CROSSCHECK_MAX_AGE_S", raising=False)
    assert main_bot.vwap_crosscheck_max_age_s() == 300


def test_eight_forty_two_today_vwap_allows_the_call(redis_scanner, monkeypatch):
    """Price 716.25 vs today ~714.64 allows. The raw list still says 722.40."""
    store, scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        loaded = await scan._series("SPY")
        stale = main_bot.scanner_mod.sampled_vwap(loaded)
        reading = await main_bot.scanner_mod.read_session_vwap(store, "SPY", NOW)
        _agree()
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        cand = main_bot.scanner_mod.evaluate_quote(
            {"symbol": "SPY", "last": SPY_LAST, "bid": 716.2, "ask": 716.3, "open": 715.0},
            loaded,
            {"min_rvol": 0.9, "min_price": 1, "max_spread_pct": 5, "min_score": 0},
            0.2,
            {"now_et": NOW, "quotes": {}, "session_vwap": {"SPY": reading}},
        )
        return stale, quotes, reason, cand

    stale, quotes, reason, cand = _run(scenario())
    by_sym = {row["symbol"]: row for row in quotes}
    assert stale == pytest.approx(SPY_STALE)
    assert stale > SPY_LAST
    assert by_sym["SPY"]["vwap"] == pytest.approx(SPY_TODAY)
    assert by_sym["QQQ"]["vwap"] == pytest.approx(QQQ_TODAY)
    assert by_sym["SPY"]["vwap_source"] == "session_sampled"
    assert by_sym["SPY"]["gate_vwap_source"] == "session_sampled"
    assert by_sym["SPY"]["gate_vwap_verdict"] == "above"
    assert by_sym["QQQ"]["gate_vwap_verdict"] == "above"
    assert by_sym["SPY"]["massive_day_vwap"] == 100.0
    assert by_sym["SPY"]["vwap"] != 100.0
    assert by_sym["SPY"]["vwap"] != pytest.approx(SPY_STALE)
    assert reason is None
    assert cand["vwap"] == pytest.approx(SPY_TODAY)
    assert cand["vwap_source"] == "session_sampled"
    assert cand["vwap"] != pytest.approx(SPY_STALE)
    diff = by_sym["SPY"]["gate_vwap_crosscheck_bps"]
    assert diff == pytest.approx((SPY_RTH - SPY_TODAY) / SPY_TODAY * 10000, abs=0.02)
    assert abs(diff) <= 25


def test_reverse_stale_below_today_above_blocks(redis_scanner):
    """A raw list VWAP under the price would have passed. Today's VWAP blocks."""
    store, _scan = redis_scanner
    prior_day = datetime(2026, 11, 23, tzinfo=ET)
    today = datetime(2026, 11, 24, tzinfo=ET)
    series = _prior(prior_day, 700.0) + _flat(today, 9 * 60 + 30, 12, 720.0)

    async def scenario():
        await _seed(store, "SPY", series)
        await _seed(store, "QQQ", series)
        loaded = await main_bot.scanner._series("SPY")
        stale = main_bot.scanner_mod.sampled_vwap(loaded)
        _seed_rth("SPY", NOW, 720.0)
        _seed_rth("QQQ", NOW, 720.0)
        saved = main_bot._scanner_fetch_quotes

        async def fetch(symbols, *, dry_run=False):
            return {"QuoteResponse": {"QuoteData": [
                _quote("SPY", SPY_LAST, 700.0, 100.0),
                _quote("QQQ", SPY_LAST, 700.0, 100.0),
            ]}}

        main_bot._scanner_fetch_quotes = fetch
        try:
            quotes = await main_bot._index_vwap_quotes()
            reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        finally:
            main_bot._scanner_fetch_quotes = saved
        return stale, quotes, reason

    stale, quotes, reason = _run(scenario())
    assert stale == pytest.approx(700.0)
    assert stale < SPY_LAST
    assert quotes[0]["vwap"] == pytest.approx(720.0)
    assert reason is not None
    assert reason.startswith("CALL blocked while the index is below day VWAP")
    assert "720.00" in reason


def test_fewer_than_n_today_samples_fails_closed(redis_scanner):
    store, _scan = redis_scanner
    today = datetime(2026, 11, 24, tzinfo=ET)
    thin = _prior(datetime(2026, 11, 23, tzinfo=ET), SPY_STALE) + _flat(today, 9 * 60 + 30, 9, SPY_TODAY)

    async def scenario():
        await _seed(store, "SPY", thin)
        await _seed(store, "QQQ", thin)
        _agree()
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert all(row["vwap"] is None for row in quotes)
    assert all(row["gate_vwap_verdict"] == "unmeasured" for row in quotes)
    assert quotes[0]["gate_vwap_reason"] == "today_samples_insufficient"
    assert quotes[0]["gate_vwap_samples"] == 8
    assert reason is not None
    assert "fail closed" in reason
    assert "today_samples_insufficient" in reason


def test_preopen_samples_only_are_unmeasured(redis_scanner):
    store, _scan = redis_scanner
    today = datetime(2026, 11, 24, tzinfo=ET)
    early = _flat(today, 8 * 60, 12, SPY_TODAY)

    async def scenario():
        await _seed(store, "SPY", early)
        await _seed(store, "QQQ", early)
        _agree()
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert quotes[0]["vwap"] is None
    assert quotes[0]["gate_vwap_reason"] == "today_samples_insufficient"
    assert quotes[0]["gate_vwap_samples"] == 0
    assert reason is not None
    assert "fail closed" in reason
    assert "today_samples_insufficient" in reason


def test_massive_agreement_within_25_bps_passes(redis_scanner):
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        _agree()
        quotes = await main_bot._index_vwap_quotes()
        return main_bot.assess_call_index_vwap_gate("CALL", quotes), quotes

    reason, quotes = _run(scenario())
    assert reason is None
    assert abs(quotes[0]["gate_vwap_crosscheck_bps"]) <= 25
    assert abs(quotes[1]["gate_vwap_crosscheck_bps"]) <= 25


def test_massive_disagreement_fails_closed(redis_scanner):
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        _seed_rth("SPY", NOW, 730.0)
        _seed_rth("QQQ", NOW, 660.0)
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert all(row["vwap"] is None for row in quotes)
    assert quotes[0]["gate_vwap_reason"] == "crosscheck_gap_bps"
    assert reason is not None
    assert "fail closed" in reason
    assert "crosscheck_gap_bps" in reason
    assert abs(quotes[0]["gate_vwap_crosscheck_bps"]) > 25


def test_missing_massive_fails_closed_when_required(redis_scanner):
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert all(row["vwap"] is None for row in quotes)
    assert quotes[0]["gate_vwap_reason"] == "massive_rth_missing"
    assert reason is not None
    assert "fail closed" in reason
    assert "massive_rth_missing" in reason


def test_missing_massive_uses_today_when_not_required(redis_scanner, monkeypatch):
    monkeypatch.setenv("VWAP_CROSSCHECK_REQUIRED", "false")
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert quotes[0]["vwap"] == pytest.approx(SPY_TODAY)
    assert quotes[0]["gate_vwap_source"] == "session_sampled"
    assert reason is None


def test_day_vw_is_never_the_gate_input(redis_scanner):
    """day.vw and the broker vwap field would pass. Today's VWAP blocks."""
    store, _scan = redis_scanner
    prior_day = datetime(2026, 11, 23, tzinfo=ET)
    today = datetime(2026, 11, 24, tzinfo=ET)
    series = _prior(prior_day, 800.0) + _flat(today, 9 * 60 + 30, 12, 720.0)

    async def scenario():
        await _seed(store, "SPY", series)
        await _seed(store, "QQQ", series)
        _seed_rth("SPY", NOW, 720.0)
        _seed_rth("QQQ", NOW, 720.0)
        saved = main_bot._scanner_fetch_quotes

        async def fetch(symbols, *, dry_run=False):
            return {"QuoteResponse": {"QuoteData": [
                _quote("SPY", SPY_LAST, 100.0, 100.0),
                _quote("QQQ", SPY_LAST, 100.0, 100.0),
            ]}}

        main_bot._scanner_fetch_quotes = fetch
        try:
            quotes = await main_bot._index_vwap_quotes()
            reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
            observe = await main_bot.build_call_index_vwap_observe(quotes)
        finally:
            main_bot._scanner_fetch_quotes = saved
        return quotes, reason, observe

    quotes, reason, observe = _run(scenario())
    spy = quotes[0]
    assert spy["massive_day_vwap"] == 100.0
    assert spy["vwap"] == pytest.approx(720.0)
    assert spy["vwap"] != spy["massive_day_vwap"]
    assert reason is not None and reason.startswith("CALL blocked while the index is below day VWAP")
    row = next(item for item in observe["indexes"] if item["symbol"] == "SPY")
    assert row["massive_day_vwap"] == 100.0
    assert row["current_vwap"] == pytest.approx(720.0)
    assert row["gate_vwap_source"] == "session_sampled"
    assert row["gate_vwap_verdict"] == "below"
    assert observe["gate_vwap_verdict"] == "block"
    assert observe["gate_vwap_source"] == "session_sampled"
    assert row["massive_minute_vwap_rth"] == pytest.approx(720.0)
    assert row["session_sampled_vwap"] == pytest.approx(720.0)


def test_half_day_session_window_excludes_the_afternoon(redis_scanner, monkeypatch):
    half_now = datetime(2026, 11, 27, 15, 30, tzinfo=ET)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: half_now.astimezone(timezone.utc))
    store, _scan = redis_scanner
    morning = _flat(datetime(2026, 11, 27, tzinfo=ET), 9 * 60 + 30, 12, 100.0)
    afternoon = _prints(datetime(2026, 11, 27, tzinfo=ET), 13 * 60 + 30, 4, 400.0, 50_000_000, 50_000_000)
    prior = _prior(datetime(2026, 11, 25, tzinfo=ET), 900.0)
    series = prior + morning + afternoon

    resolved = main_bot.scanner_mod.today_session_vwap(series, half_now)
    assert main_bot._session_close_minute(half_now) == 13 * 60
    assert resolved["measured"] is True
    assert resolved["vwap"] == pytest.approx(100.0)
    assert resolved["samples"] == 11

    full_now = datetime(2026, 11, 24, 15, 30, tzinfo=ET)
    full_series = (
        _prior(datetime(2026, 11, 23, tzinfo=ET), 900.0)
        + _flat(datetime(2026, 11, 24, tzinfo=ET), 9 * 60 + 30, 12, 100.0)
        + _prints(datetime(2026, 11, 24, tzinfo=ET), 13 * 60 + 30, 4, 400.0, 50_000_000, 50_000_000)
    )
    full = main_bot.scanner_mod.today_session_vwap(full_series, full_now)
    assert full["vwap"] != pytest.approx(100.0)
    assert full["samples"] == 15

    async def scenario():
        await _seed(store, "SPY", series)
        await _seed(store, "QQQ", series)
        _seed_rth("SPY", half_now, 100.0)
        _seed_rth("QQQ", half_now, 100.0)
        saved = main_bot._scanner_fetch_quotes

        async def fetch(symbols, *, dry_run=False):
            return {"QuoteResponse": {"QuoteData": [
                _quote("SPY", 101.0, 900.0, 50.0),
                _quote("QQQ", 101.0, 900.0, 50.0),
            ]}}

        main_bot._scanner_fetch_quotes = fetch
        try:
            quotes = await main_bot._index_vwap_quotes()
        finally:
            main_bot._scanner_fetch_quotes = saved
        return quotes

    quotes = _run(scenario())
    assert quotes[0]["vwap"] == pytest.approx(100.0)
    assert quotes[0]["gate_vwap_verdict"] == "above"
    assert main_bot.assess_call_index_vwap_gate("CALL", quotes) is None


def test_spy_range_uses_today_prices_only(redis_scanner):
    store, _scan = redis_scanner
    prior = _prints(datetime(2026, 11, 23, tzinfo=ET), 14 * 60, 22, 780.0, 1_000_000, 1_000_000)
    today_prices = [710.0 + i * 0.5 for i in range(12)]
    today = _prints(datetime(2026, 11, 24, tzinfo=ET), 9 * 60 + 30, 12, today_prices, 1000, 1000)

    async def scenario():
        await _seed(store, "SPY", prior + today)
        return await main_bot._pro_trader_observation({"ticker": "SPY"})

    obs = _run(scenario())
    assert obs["prices"] == today_prices
    assert 780.0 not in obs["prices"]
    assert obs["realized_range_pct"] == pytest.approx((today_prices[-1] - today_prices[0]) / today_prices[0] * 100.0)


def test_order_path_does_not_fetch_massive(redis_scanner, monkeypatch):
    store, _scan = redis_scanner
    spy, qqq = _live_series()
    hits = {"refresh": 0, "http": 0}

    async def refresh(*_args, **_kwargs):
        hits["refresh"] += 1

    class Client:
        def __init__(self, *args, **kwargs):
            hits["http"] += 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *args, **kwargs):
            hits["http"] += 1
            raise AssertionError("order path called Massive")

    monkeypatch.setattr(main_bot, "refresh_massive_minute_vwap", refresh)
    monkeypatch.setattr(httpx, "AsyncClient", Client)

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        _agree()
        payload = {"instrument": "option", "option_right": "CALL", "action": "BUY"}
        reason = await main_bot._call_index_vwap_block(payload)
        return reason, payload

    reason, payload = _run(scenario())
    assert reason is None
    assert hits == {"refresh": 0, "http": 0}
    observe = payload["call_index_vwap_observe"]
    assert observe["gate_vwap_source"] == "session_sampled"
    assert observe["gate_vwap_verdict"] == "allow"
    spy_row = next(row for row in observe["indexes"] if row["symbol"] == "SPY")
    assert spy_row["massive_minute_vwap_rth"] == pytest.approx(SPY_RTH)
    assert spy_row["session_sampled_vwap"] == pytest.approx(SPY_TODAY)
    assert spy_row["current_vwap"] == pytest.approx(SPY_TODAY)
    assert spy_row["massive_day_vwap"] == 100.0
    assert spy_row["gate_vwap_verdict"] == "above"


def _cfg():
    return {"min_rvol": 0.9, "min_price": 1, "max_spread_pct": 5, "min_score": 0}


def test_full_session_vwap_survives_the_120_sample_cap(redis_scanner, monkeypatch):
    """300 trending RTH minutes. The gate VWAP is the whole session, not the last 120."""
    store, scan = redis_scanner
    start = datetime(2026, 11, 24, tzinfo=ET)
    prices = [700.0 + i * 0.05 for i in range(300)]
    samples = _prints(start, 9 * 60 + 30, 300, prices, 1000, 1000)
    end = datetime(2026, 11, 24, 14, 30, tzinfo=ET)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: end.astimezone(timezone.utc))
    full = main_bot.scanner_mod.sampled_vwap(samples)
    trailing = main_bot.scanner_mod.sampled_vwap(samples[-120:])

    async def scenario():
        await _seed(store, "SPY", samples)
        await _seed(store, "QQQ", samples)
        loaded = await scan._series("SPY")
        reading = await main_bot.scanner_mod.read_session_vwap(store, "SPY", end)
        seen = []
        original = store.hash_get

        async def wrapped(key):
            seen.append(key)
            return await original(key)

        store.hash_get = wrapped
        _seed_rth("SPY", end, full)
        _seed_rth("QQQ", end, full)
        saved = main_bot._scanner_fetch_quotes

        async def fetch(symbols, *, dry_run=False):
            last = prices[-1]
            return {"QuoteResponse": {"QuoteData": [
                _quote("SPY", last, 1.0, 1.0),
                _quote("QQQ", last, 1.0, 1.0),
            ]}}

        main_bot._scanner_fetch_quotes = fetch
        try:
            quotes = await main_bot._index_vwap_quotes()
            reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
            cand = main_bot.scanner_mod.evaluate_quote(
                {"symbol": "SPY", "last": prices[-1], "bid": prices[-1], "ask": prices[-1] + 0.01, "open": prices[0]},
                loaded,
                _cfg(),
                0.5,
                {"now_et": end, "quotes": {}, "session_vwap": {"SPY": reading}},
            )
            obs = await main_bot._pro_trader_observation({"ticker": "SPY"})
        finally:
            main_bot._scanner_fetch_quotes = saved
        ttl = await store.redis.ttl(main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24"))
        return loaded, reading, quotes, reason, cand, obs, seen, ttl

    loaded, reading, quotes, reason, cand, obs, seen, ttl = _run(scenario())
    assert len(loaded) == main_bot.scanner_mod.MAX_SAMPLES == 120
    assert full == pytest.approx(reading["vwap"])
    assert trailing != pytest.approx(full)
    gap = abs(trailing - full) / full * 10000
    assert gap > 25
    assert quotes[0]["vwap"] == pytest.approx(full)
    assert quotes[0]["gate_vwap_samples"] == 299
    assert reason is None
    assert cand["vwap"] == pytest.approx(full)
    assert cand["vwap"] != pytest.approx(main_bot.scanner_mod.sampled_vwap(
        main_bot.scanner_mod.session_series(loaded, end)
    ))
    full_range = (prices[-1] - prices[0]) / prices[0] * 100.0
    trail_range = (prices[-1] - prices[-120]) / prices[-120] * 100.0
    assert obs["realized_range_pct"] == pytest.approx(full_range)
    assert obs["realized_range_pct"] != pytest.approx(trail_range)
    assert seen
    assert all(key.endswith("2026-11-24") for key in seen)
    assert all("2026-11-23" not in key for key in seen)
    assert 1.5 * 24 * 3600 < ttl <= 2 * 24 * 3600


def test_prior_day_key_is_never_read_and_rollover_starts_fresh(redis_scanner, monkeypatch):
    store, _scan = redis_scanner
    day1 = datetime(2026, 11, 24, tzinfo=ET)
    day2 = datetime(2026, 11, 25, 10, 30, tzinfo=ET)

    async def scenario():
        await _seed(store, "SPY", _flat(day1, 9 * 60 + 30, 20, 700.0))
        poison = main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-23")
        await store.hash_set(poison, {
            "sum_pv": "999000", "sum_v": "1000", "count": "50",
            "first_ts": "2026-11-23T10:00:00-05:00",
            "last_ts": "2026-11-23T15:00:00-05:00",
        }, 2 * 24 * 3600)
        seen = []
        original = store.hash_get

        async def wrapped(key):
            seen.append(key)
            return await original(key)

        store.hash_get = wrapped
        today = await main_bot.scanner_mod.read_session_vwap(store, "SPY", NOW)
        fresh = await main_bot.scanner_mod.read_session_vwap(store, "SPY", day2)
        await main_bot.scanner_mod.note_session_vwap(
            store, "SPY",
            {"ts": day2.replace(hour=10, minute=0).isoformat(), "last": 50.0, "total_volume": 1000},
            None,
        )
        await main_bot.scanner_mod.note_session_vwap(
            store, "SPY",
            {"ts": day2.replace(hour=10, minute=1).isoformat(), "last": 50.0, "total_volume": 2000},
            None,
        )
        # Eleven more so the new day clears the sample floor on its own.
        for i in range(2, 12):
            await main_bot.scanner_mod.note_session_vwap(
                store, "SPY",
                {
                    "ts": day2.replace(hour=10, minute=i).isoformat(),
                    "last": 50.0,
                    "total_volume": 1000 * (i + 1),
                },
                None,
            )
        nxt = await main_bot.scanner_mod.read_session_vwap(store, "SPY", day2)
        return today, fresh, nxt, seen

    today, fresh, nxt, seen = _run(scenario())
    assert today["vwap"] == pytest.approx(700.0)
    assert today["vwap"] != pytest.approx(999.0)
    assert fresh["samples"] == 0
    assert fresh["measured"] is False
    assert fresh["vwap"] is None
    assert nxt["measured"] is True
    assert nxt["vwap"] == pytest.approx(50.0)
    assert nxt["samples"] == 11
    assert all("2026-11-23" not in key for key in seen)


def test_stale_massive_cache_fails_closed(redis_scanner):
    store, _scan = redis_scanner
    spy, qqq = _live_series()
    stale_at = NOW - timedelta(seconds=301)

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        _seed_rth("SPY", stale_at, SPY_RTH)
        _seed_rth("QQQ", stale_at, QQQ_RTH)
        quotes = await main_bot._index_vwap_quotes()
        return quotes, main_bot.assess_call_index_vwap_gate("CALL", quotes)

    quotes, reason = _run(scenario())
    assert quotes[0]["vwap"] is None
    assert quotes[0]["gate_vwap_reason"] == "massive_rth_stale"
    assert quotes[0]["gate_vwap_verdict"] == "unmeasured"
    view = main_bot._cached_minute_vwap_view("SPY", NOW)
    assert view["age_seconds"] > 300
    assert reason is not None
    assert "massive_rth_stale" in reason
    assert "fail closed" in reason


def test_restart_with_empty_massive_cache_fails_closed_without_raising(redis_scanner):
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        main_bot._minute_vwap_cache.clear()
        main_bot._minute_vwap_refresh_meta.clear()
        main_bot._minute_vwap_attempt_at.clear()
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        payload = {"instrument": "option", "option_right": "CALL", "action": "BUY"}
        blocked = await main_bot._call_index_vwap_block(payload)
        status = main_bot.massive_rth_cache_status(NOW)
        return quotes, reason, blocked, status

    quotes, reason, blocked, status = _run(scenario())
    assert quotes[0]["gate_vwap_reason"] == "massive_rth_missing"
    assert reason is not None and "massive_rth_missing" in reason
    assert blocked is not None and "massive_rth_missing" in blocked
    assert status["symbols"]["SPY"]["refresh_result"] is None
    assert status["symbols"]["SPY"]["age_seconds"] is None
    assert status["max_age_s"] == 300


def test_scanner_loop_refresh_is_invoked_and_throttled(redis_scanner, monkeypatch):
    _store, scan = redis_scanner
    hits = {"n": 0}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *args, **kwargs):
            hits["n"] += 1
            ts = NOW.timestamp() * 1000
            return type("Resp", (), {
                "status_code": 200,
                "json": lambda self: {"results": [{"t": ts, "vw": SPY_RTH, "v": 100, "c": SPY_RTH}]},
            })()

    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(httpx, "AsyncClient", Client)
    scan.refresh_index_vwap = main_bot.refresh_massive_minute_vwap
    assert scan.is_market_open() is True

    async def scenario():
        await scan._refresh_index_vwap()
        await scan._refresh_index_vwap()
        return main_bot.massive_rth_cache_status(NOW)

    status = _run(scenario())
    assert hits["n"] == 2  # SPY and QQQ, once each
    assert status["symbols"]["SPY"]["refresh_result"] == "ok"
    assert status["symbols"]["QQQ"]["refresh_result"] == "ok"
    assert status["symbols"]["SPY"]["age_seconds"] is not None
    assert status["symbols"]["SPY"]["age_seconds"] <= 300
    assert status["symbols"]["SPY"]["refresh_at"]


def test_refresh_timeout_does_not_block_the_entry_path(redis_scanner, monkeypatch):
    store, _scan = redis_scanner
    spy, qqq = _live_series()

    class Slow:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *args, **kwargs):
            await asyncio.sleep(30)
            raise AssertionError("the refresh waited on a hung Massive call")

    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(httpx, "AsyncClient", Slow)
    _seed_rth("SPY", NOW - timedelta(seconds=10), SPY_RTH)
    _seed_rth("QQQ", NOW - timedelta(seconds=10), QQQ_RTH)

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        started = asyncio.get_event_loop().time()
        task = asyncio.create_task(main_bot.refresh_massive_minute_vwap(["SPY"]))
        payload = {"instrument": "option", "option_right": "CALL", "action": "BUY"}
        reason = await main_bot._call_index_vwap_block(payload)
        elapsed = asyncio.get_event_loop().time() - started
        await asyncio.wait_for(task, timeout=5)
        return reason, elapsed, main_bot.massive_rth_cache_status(NOW)

    reason, elapsed, status = _run(scenario())
    assert reason is None
    assert elapsed < 1.0
    assert status["symbols"]["SPY"]["refresh_result"] == "error"
    assert main_bot._cached_minute_vwap_view("SPY", NOW)["vwap"] == pytest.approx(SPY_RTH)


def test_opening_ten_minutes_fail_closed(redis_scanner, monkeypatch):
    """9:30–9:40 ET is unmeasured on purpose. CALLs stay off until N samples exist."""
    store, _scan = redis_scanner
    early = datetime(2026, 11, 24, 9, 39, tzinfo=ET)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: early.astimezone(timezone.utc))
    today = datetime(2026, 11, 24, tzinfo=ET)
    thin = _flat(today, 9 * 60 + 30, 9, SPY_TODAY)

    async def scenario():
        await _seed(store, "SPY", thin)
        await _seed(store, "QQQ", thin)
        _seed_rth("SPY", early, SPY_TODAY)
        _seed_rth("QQQ", early, QQQ_TODAY)
        reading = await main_bot.scanner_mod.read_session_vwap(store, "SPY", early)
        cand = main_bot.scanner_mod.evaluate_quote(
            {"symbol": "SPY", "last": SPY_LAST, "bid": 716.2, "ask": 716.3, "open": 715.0},
            thin,
            _cfg(),
            0.05,
            {"now_et": early, "quotes": {}, "session_vwap": {"SPY": reading}},
        )
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        return reading, cand, quotes, reason

    reading, cand, quotes, reason = _run(scenario())
    assert reading["measured"] is False
    assert reading["samples"] == 8
    assert cand["vwap"] is None
    assert cand["vwap_unmeasured_reason"] == "today_samples_insufficient"
    factor = next(item for item in cand["score_factors"] if item["key"] == "vwap")
    assert factor["fill"] == 0
    assert factor["measured"] is False
    assert quotes[0]["gate_vwap_reason"] == "today_samples_insufficient"
    assert reason is not None and "fail closed" in reason


def test_nan_and_inf_vwap_are_unmeasured(monkeypatch):
    today = datetime(2026, 11, 24, tzinfo=ET)
    series = _flat(today, 9 * 60 + 30, 12, 100.0)
    for value in (float("nan"), float("inf")):
        monkeypatch.setattr(main_bot.scanner_mod, "sampled_vwap", lambda _series, value=value: value)
        out = main_bot.scanner_mod.today_session_vwap(series, NOW)
        assert out["measured"] is False
        assert out["vwap"] is None
        assert out["reason"] == "today_samples_insufficient"
    nan_series = []
    for sample in series:
        copied = dict(sample)
        copied["last"] = float("nan")
        nan_series.append(copied)
    direct = main_bot.scanner_mod.today_session_vwap(nan_series, NOW)
    assert direct["measured"] is False
    assert direct["vwap"] is None
    cand = main_bot.scanner_mod.evaluate_quote(
        {"symbol": "SPY", "last": 100.0, "bid": 100.0, "ask": 100.01, "open": 100.0},
        series,
        _cfg(),
        0.2,
        {
            "now_et": NOW,
            "quotes": {},
            "session_vwap": {"SPY": {
                "vwap": float("nan"), "samples": 12, "measured": True,
                "reason": None, "source": "session_sampled",
            }},
        },
    )
    assert cand["vwap"] is None
    factor = next(item for item in cand["score_factors"] if item["key"] == "vwap")
    assert factor["fill"] == 0.0
    assert factor["measured"] is False
    assert math.isfinite(factor["points"])


def test_nan_price_does_not_poison_the_session_accumulator(redis_scanner):
    store, _scan = redis_scanner
    today = datetime(2026, 11, 24, tzinfo=ET)

    async def scenario():
        await _seed(store, "SPY", _flat(today, 9 * 60 + 30, 12, 100.0))
        await main_bot.scanner_mod.note_session_vwap(store, "SPY", {
            "ts": datetime(2026, 11, 24, 10, 0, tzinfo=ET).isoformat(),
            "last": float("nan"),
            "total_volume": 10_000_000,
        }, None)
        await main_bot.scanner_mod.note_session_vwap(store, "SPY", {
            "ts": datetime(2026, 11, 24, 10, 1, tzinfo=ET).isoformat(),
            "last": float("inf"),
            "total_volume": 10_001_000,
        }, None)
        return await main_bot.scanner_mod.read_session_vwap(store, "SPY", NOW)

    reading = _run(scenario())
    assert reading["measured"] is True
    assert reading["vwap"] == pytest.approx(100.0)
    assert math.isfinite(reading["vwap"])


def test_crosscheck_block_alerts_once_per_fifteen_minutes(redis_scanner, monkeypatch):
    store, _scan = redis_scanner
    spy, qqq = _live_series()
    sent = []

    async def send(*args, **kwargs):
        sent.append(kwargs.get("dedupe_key") or args)

    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        payload = {"instrument": "option", "option_right": "CALL", "action": "BUY"}
        first = await main_bot._call_index_vwap_block(payload)
        second = await main_bot._call_index_vwap_block(dict(payload))
        await asyncio.sleep(0)
        return first, second

    first, second = _run(scenario())
    assert first is not None and "massive_rth_missing" in first
    assert second is not None and "massive_rth_missing" in second
    assert sent == ["vwap_crosscheck_blocked"]


def test_sample_floor_follows_the_stored_scanner_interval(redis_scanner, monkeypatch):
    _store, scan = redis_scanner
    monkeypatch.delenv("VWAP_MIN_TODAY_SAMPLES", raising=False)
    monkeypatch.setenv("SCANNER_INTERVAL_SECONDS", "60")
    saved = main_bot.scanner_mod._LIVE_INTERVAL_SECONDS

    async def scenario():
        await scan.set_config({"interval_seconds": 30})
        return main_bot.scanner_mod.vwap_min_today_samples()

    try:
        assert _run(scenario()) == 20
    finally:
        main_bot.scanner_mod._LIVE_INTERVAL_SECONDS = saved


def test_append_sample_counts_a_1400_et_stamp_that_is_1900_utc(redis_scanner, monkeypatch):
    """UTC stamps go through Scanner._append_sample. 19:00Z is 14:00 ET and counts."""
    store, scan = redis_scanner
    clock = {"t": datetime(2026, 11, 24, 14, 30, tzinfo=timezone.utc)}
    monkeypatch.setattr(main_bot.scanner_mod, "_utcnow", lambda: clock["t"])

    async def scenario():
        vol = 0
        start = datetime(2026, 11, 24, 14, 30, tzinfo=timezone.utc)
        for i in range(180):
            clock["t"] = start + timedelta(minutes=i)
            vol += 1000
            await scan._append_sample({
                "symbol": "SPY", "last": 700.0 + i * 0.01, "total_volume": vol,
            })
        clock["t"] = datetime(2026, 11, 24, 19, 0, tzinfo=timezone.utc)
        vol += 1000
        await scan._append_sample({
            "symbol": "SPY", "last": 720.0, "total_volume": vol,
        })
        fields = await store.hash_get(
            main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24")
        )
        reading = await main_bot.scanner_mod.read_session_vwap(
            store, "SPY", datetime(2026, 11, 24, 19, 0, tzinfo=timezone.utc),
        )
        return fields, reading

    fields, reading = _run(scenario())
    # 181 prints. The first adds no volume, so 180 deltas. A UTC close check
    # keeps only 9:30–11:00 ET (89 deltas) and drops 19:00Z.
    assert int(float(fields["count"])) == 180
    assert reading["measured"] is True
    assert reading["samples"] == 180
    assert reading["vwap"] > 700.0
    last = datetime.fromisoformat(fields["last_ts"])
    assert last.astimezone(ET).hour == 14
    assert last.astimezone(ET).minute == 0
    assert last.date() == datetime(2026, 11, 24, tzinfo=ET).date()


def test_append_sample_half_day_counts_until_1300_et(redis_scanner, monkeypatch):
    """Friday 11/27 closes at 13:00 ET. 14:30Z–17:59Z count. 18:00Z does not."""
    store, scan = redis_scanner
    clock = {"t": datetime(2026, 11, 27, 14, 30, tzinfo=timezone.utc)}
    monkeypatch.setattr(main_bot.scanner_mod, "_utcnow", lambda: clock["t"])

    async def scenario():
        clock["t"] = datetime(2026, 11, 27, 14, 30, tzinfo=timezone.utc)
        await scan._append_sample({"symbol": "SPY", "last": 100.0, "total_volume": 1000})
        clock["t"] = datetime(2026, 11, 27, 17, 59, tzinfo=timezone.utc)
        await scan._append_sample({"symbol": "SPY", "last": 110.0, "total_volume": 2000})
        clock["t"] = datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)
        await scan._append_sample({"symbol": "SPY", "last": 400.0, "total_volume": 50_000_000})
        return await store.hash_get(
            main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-27")
        )

    fields = _run(scenario())
    assert int(float(fields["count"])) == 1
    assert float(fields["sum_pv"]) / float(fields["sum_v"]) == pytest.approx(110.0)
    first = datetime.fromisoformat(fields["first_ts"])
    last = datetime.fromisoformat(fields["last_ts"])
    assert (first.astimezone(ET).hour, first.astimezone(ET).minute) == (9, 30)
    assert (last.astimezone(ET).hour, last.astimezone(ET).minute) == (12, 59)


def test_utc_just_after_midnight_does_not_open_the_next_et_day(redis_scanner, monkeypatch):
    """2026-11-25 00:30Z is 19:30 ET on the 24th. It must not create the 25th key."""
    store, scan = redis_scanner
    clock = {"t": datetime(2026, 11, 25, 0, 30, tzinfo=timezone.utc)}
    monkeypatch.setattr(main_bot.scanner_mod, "_utcnow", lambda: clock["t"])

    async def scenario():
        await scan._append_sample({"symbol": "SPY", "last": 700.0, "total_volume": 5000})
        late = await store.hash_get(main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-25"))
        prior = await store.hash_get(main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24"))
        return late, prior

    late, prior = _run(scenario())
    assert not late
    assert not prior


def test_naive_and_zulu_stamps_are_utc_before_the_et_window():
    mod = main_bot.scanner_mod
    anchor = mod.fold_session_sample(
        None,
        {"ts": "2026-11-24T18:59:00+00:00", "last": 200.0, "total_volume": 1000},
        None,
    )
    naive = mod.fold_session_sample(
        anchor,
        {"ts": "2026-11-24T19:00:00", "last": 210.0, "total_volume": 2000},
        None,
    )
    zulu = mod.fold_session_sample(
        anchor,
        {"ts": "2026-11-24T19:00:00Z", "last": 210.0, "total_volume": 3000},
        None,
    )
    aware = mod.fold_session_sample(
        anchor,
        {"ts": "2026-11-24T14:00:00-05:00", "last": 210.0, "total_volume": 4000},
        None,
    )
    assert anchor is not None and anchor["count"] == 0
    for folded in (naive, zulu, aware):
        assert folded["count"] == 1
        assert folded["sum_pv"] / folded["sum_v"] == pytest.approx(210.0)
        last = datetime.fromisoformat(folded["last_ts"])
        assert last.astimezone(ET).hour == 14
        assert last.date() == datetime(2026, 11, 24, tzinfo=ET).date()


def test_sample_floor_counts_only_prints_that_added_volume(redis_scanner):
    store, _scan = redis_scanner
    day = datetime(2026, 11, 24, tzinfo=ET)
    flat_volume = _prints(day, 9 * 60 + 30, 12, 700.0, 5000, 0)

    async def scenario():
        await _seed(store, "SPY", flat_volume)
        await _seed(store, "QQQ", flat_volume)
        return await main_bot._index_vwap_quotes()

    quotes = _run(scenario())
    assert quotes[0]["gate_vwap_reason"] == "today_samples_insufficient"
    assert quotes[0]["gate_vwap_samples"] == 0
    assert quotes[0]["vwap"] is None


def test_crosscheck_alert_returns_before_the_webhook(redis_scanner, monkeypatch):
    store, _scan = redis_scanner
    spy, qqq = _live_series()
    started = []

    async def send(*_args, **_kwargs):
        started.append(True)
        await asyncio.sleep(8)

    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        reason = await main_bot._call_index_vwap_block(
            {"instrument": "option", "option_right": "CALL", "action": "BUY"}
        )
        elapsed = loop.time() - t0
        await asyncio.sleep(0)
        pending = list(main_bot._vwap_alert_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return reason, elapsed

    reason, elapsed = _run(scenario())
    assert reason is not None and "massive_rth_missing" in reason
    assert elapsed < 0.5
    assert started == [True]
