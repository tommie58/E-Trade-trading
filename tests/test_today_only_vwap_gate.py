"""Today-only VWAP for the CALL gate, evaluate_quote, and the SPY range.

Frozen America/New_York clocks. The rolling scanner list lives in fakeredis.
Prior-day prints stay in the list. Readers that decide a CALL must not use them.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
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
    yield
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()


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
    key = main_bot.scanner_mod.SERIES_KEY_FMT.format(symbol)
    for sample in samples:
        await store.list_push_capped(
            key, json.dumps(sample), max_len=main_bot.scanner_mod.MAX_SAMPLES,
        )


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
    monkeypatch.delenv("SCANNER_INTERVAL_SECONDS", raising=False)
    assert main_bot.scanner_mod.vwap_min_today_samples() == 10
    monkeypatch.setenv("SCANNER_INTERVAL_SECONDS", "30")
    assert main_bot.scanner_mod.vwap_min_today_samples() == 20
    monkeypatch.setenv("VWAP_MIN_TODAY_SAMPLES", "7")
    assert main_bot.scanner_mod.vwap_min_today_samples() == 7
    monkeypatch.setenv("VWAP_CROSSCHECK_MAX_BPS", "25")
    assert main_bot.vwap_crosscheck_max_bps() == 25
    assert main_bot.vwap_crosscheck_required() is True


def test_eight_forty_two_today_vwap_allows_the_call(redis_scanner, monkeypatch):
    """Price 716.25 vs today ~714.64 allows. The raw list still says 722.40."""
    store, scan = redis_scanner
    spy, qqq = _live_series()

    async def scenario():
        await _seed(store, "SPY", spy)
        await _seed(store, "QQQ", qqq)
        loaded = await scan._series("SPY")
        stale = main_bot.scanner_mod.sampled_vwap(loaded)
        _agree()
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        cand = main_bot.scanner_mod.evaluate_quote(
            {"symbol": "SPY", "last": SPY_LAST, "bid": 716.2, "ask": 716.3, "open": 715.0},
            loaded,
            {"min_rvol": 0.9, "min_price": 1, "max_spread_pct": 5, "min_score": 0},
            0.2,
            {"now_et": NOW, "quotes": {}},
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
    assert "only 9 today RTH samples" in quotes[0]["gate_vwap_reason"]
    assert reason is not None
    assert "fail closed" in reason
    assert "only 9 today RTH samples" in reason


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
    assert "pre-open samples only" in quotes[0]["gate_vwap_reason"]
    assert reason is not None
    assert "fail closed" in reason
    assert "pre-open samples only" in reason


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
    assert "differs by" in quotes[0]["gate_vwap_reason"]
    assert reason is not None
    assert "fail closed" in reason
    assert "730.0000" in reason


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
    assert "cross-check required" in quotes[0]["gate_vwap_reason"]
    assert reason is not None
    assert "fail closed" in reason


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
    assert resolved["samples"] == 12

    full_now = datetime(2026, 11, 24, 15, 30, tzinfo=ET)
    full_series = (
        _prior(datetime(2026, 11, 23, tzinfo=ET), 900.0)
        + _flat(datetime(2026, 11, 24, tzinfo=ET), 9 * 60 + 30, 12, 100.0)
        + _prints(datetime(2026, 11, 24, tzinfo=ET), 13 * 60 + 30, 4, 400.0, 50_000_000, 50_000_000)
    )
    full = main_bot.scanner_mod.today_session_vwap(full_series, full_now)
    assert full["vwap"] != pytest.approx(100.0)
    assert full["samples"] == 16

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
