"""Mid-session restart seeds SPY/QQQ session VWAP from Massive RTH minutes.

A redeploy at 11:08 CT (12:08 ET) used to keep only the bars after the boot.
The seed is the regular session from 09:30 ET. Premarket bars stay out.
A failed, stale, or incomplete read does not write, and the CALL gate stays
fail-closed. Bars already in the accumulator are not added again.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

import main_bot


ET = ZoneInfo("America/New_York")
DAY = datetime(2026, 11, 24, tzinfo=ET)
# 12:21:30 ET is 11:21:30 CT. The restart minute is 12:08 ET (11:08 CT).
NOW = datetime(2026, 11, 24, 12, 21, 30, tzinfo=ET)
RESTART_MINUTE = 12 * 60 + 8
OPEN_MINUTE = 9 * 60 + 30
LAST_MINUTE = 12 * 60 + 21


def _run(coro):
    return asyncio.run(coro)


def _minute_of(epoch_ms: int) -> int:
    when = datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).astimezone(ET)
    return when.hour * 60 + when.minute


def _bars(start_minute: int, end_minute: int, *, price0: float, vol0: float) -> list:
    rows = []
    minute = start_minute
    i = 0
    while minute <= end_minute:
        ts = DAY.replace(hour=minute // 60, minute=minute % 60, second=0, microsecond=0)
        # A steady trend makes the post-restart tail more than 25 bps off
        # the full-session VWAP, which is the gap that blocked the live CALLs.
        price = price0 + i * 0.15
        vol = vol0 + (i % 5) * 125
        rows.append({
            "t": int(ts.timestamp() * 1000),
            "vw": round(price, 4),
            "v": vol,
            "c": round(price + 0.02, 4),
        })
        i += 1
        minute += 1
    return rows


def _vwap(rows: list) -> float:
    num = sum(bar["vw"] * bar["v"] for bar in rows)
    den = sum(bar["v"] for bar in rows)
    return round(num / den, 4)


def _rth(price0: float = 580.0) -> list:
    return _bars(OPEN_MINUTE, LAST_MINUTE, price0=price0, vol0=1000)


def _premarket(price0: float = 1.0) -> list:
    return _bars(4 * 60, OPEN_MINUTE - 1, price0=price0, vol0=5_000_000)


def _tail(rows: list) -> list:
    return [bar for bar in rows if _minute_of(bar["t"]) >= RESTART_MINUTE]


def _acc_from_bars(rows: list, *, first: datetime, last_volume: float) -> dict:
    return {
        "sum_pv": repr(sum(bar["vw"] * bar["v"] for bar in rows)),
        "sum_v": repr(sum(bar["v"] for bar in rows)),
        "count": str(len(rows)),
        "first_ts": first.isoformat(),
        "last_ts": NOW.isoformat(),
        "last_volume": repr(float(last_volume)),
    }


def _sum_v(fields: dict) -> float:
    return float(fields["sum_v"])


@pytest.fixture(autouse=True)
def _clear_seed_throttle():
    main_bot._vwap_seed_attempt_at.clear()
    yield
    main_bot._vwap_seed_attempt_at.clear()


def test_empty_restart_matches_the_full_regular_session_and_drops_premarket():
    rth = _rth()
    payload = _premarket() + rth
    plan = main_bot.scanner_mod.plan_session_vwap_seed(None, payload, NOW, max_age_s=300)
    assert plan["applied"] is True
    assert plan["reason"] == "seeded"
    assert plan["bars_used"] == len(rth)
    assert plan["vwap"] == pytest.approx(_vwap(rth))
    assert plan["vwap"] != pytest.approx(_vwap(payload))
    again = main_bot.scanner_mod.plan_session_vwap_seed(plan["fields"], payload, NOW)
    assert again["applied"] is False
    assert again["reason"] == "not_needed"


def test_restart_after_the_open_seeds_only_the_missing_prefix():
    """Bars already folded from 12:08 ET stay. The prefix brings the full session."""
    rth = _rth()
    tail = _tail(rth)
    assert tail and _minute_of(tail[0]["t"]) == RESTART_MINUTE
    held = _acc_from_bars(
        tail,
        first=DAY.replace(hour=12, minute=8),
        last_volume=8_000_000,
    )
    before_v = _sum_v(held)
    plan = main_bot.scanner_mod.plan_session_vwap_seed(
        held, _premarket() + rth, NOW, max_age_s=300,
    )
    assert plan["applied"] is True
    assert plan["bars_used"] == len(rth) - len(tail)
    assert plan["vwap"] == pytest.approx(_vwap(rth))
    assert _sum_v(plan["fields"]) == pytest.approx(sum(bar["v"] for bar in rth))
    assert _sum_v(plan["fields"]) < before_v + sum(bar["v"] for bar in rth)
    assert float(plan["fields"]["last_volume"]) == pytest.approx(8_000_000)
    second = main_bot.scanner_mod.plan_session_vwap_seed(plan["fields"], rth, NOW)
    assert second["applied"] is False
    assert second["reason"] == "not_needed"


def test_a_hash_that_already_starts_at_the_open_is_not_seeded_again():
    rth = _rth()
    held = _acc_from_bars(rth, first=DAY.replace(hour=9, minute=30), last_volume=100)
    plan = main_bot.scanner_mod.plan_session_vwap_seed(held, rth, NOW)
    assert plan["applied"] is False
    assert plan["reason"] == "not_needed"
    assert plan["fields"] is None


def test_premarket_bars_alone_do_not_seed():
    plan = main_bot.scanner_mod.plan_session_vwap_seed(None, _premarket(), NOW)
    assert plan["applied"] is False
    assert plan["reason"] == "no_bars"
    assert plan["vwap"] is None


def test_missing_open_bar_is_incomplete_and_not_applied():
    rth = _rth()[1:]
    plan = main_bot.scanner_mod.plan_session_vwap_seed(None, rth, NOW)
    assert plan["applied"] is False
    assert plan["reason"] == "incomplete"
    assert plan["fields"] is None


def test_stale_bars_are_not_applied():
    rth = _rth()
    later = NOW + timedelta(minutes=20)
    plan = main_bot.scanner_mod.plan_session_vwap_seed(None, rth, later, max_age_s=300)
    assert plan["applied"] is False
    assert plan["reason"] == "stale"
    assert plan["fields"] is None


class _Client:
    def __init__(self, results_for, *, fail=False):
        self.results_for = results_for
        self.fail = fail
        self.urls = []
        self.headers = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        self.urls.append(url)
        self.headers.append(dict(headers or {}))
        if self.fail:
            raise RuntimeError("massive unavailable")
        parts = url.split("/")
        symbol = parts[parts.index("ticker") + 1]
        payload = {"results": list(self.results_for.get(symbol) or [])}
        return type("Resp", (), {
            "status_code": 200,
            "json": lambda _self, payload=payload: payload,
        })()


def _bind_client(monkeypatch, client):
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")

    def factory(*args, **kwargs):
        return client

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _quote(symbol, last):
    return {
        "Product": {"symbol": symbol},
        "quoteStatus": "REALTIME",
        "All": {"lastTrade": last},
    }


def _arm_gate(monkeypatch, store):
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    monkeypatch.setattr(main_bot, "state", store)
    main_bot._minute_vwap_cache.clear()
    main_bot._vwap_seed_attempt_at.clear()

    async def fetch(symbols, *, dry_run=False):
        return {"QuoteResponse": {"QuoteData": [
            _quote("SPY", 590.0),
            _quote("QQQ", 520.0),
        ]}}

    monkeypatch.setattr(main_bot, "_scanner_fetch_quotes", fetch)


def _cache(symbol, vwap):
    day = NOW.date().isoformat()
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key(symbol, day)] = {
        "vwap": vwap,
        "last_bar_epoch": NOW.timestamp(),
        "last_bar_ts": NOW.isoformat(),
        "bar_count": 100,
        "session_date": day,
        "fetched_at": NOW.timestamp(),
    }


def test_gate_restart_seeds_and_a_live_print_does_not_recount_the_seed(monkeypatch, caplog):
    spy = _rth(580.0)
    qqq = _rth(500.0)
    store = main_bot.StateStore(None)
    _arm_gate(monkeypatch, store)
    client = _Client({"SPY": _premarket() + spy, "QQQ": _premarket(2.0) + qqq})
    _bind_client(monkeypatch, client)
    caplog.set_level(logging.INFO, logger="etrade-bot")

    async def scenario():
        tail = _tail(spy)
        restart = DAY.replace(hour=12, minute=8)
        await store.hash_set(
            main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24"),
            _acc_from_bars(tail, first=restart, last_volume=9_000_000),
            main_bot.scanner_mod.SESSION_VWAP_TTL_S,
        )
        _cache("SPY", _vwap(spy))
        _cache("QQQ", _vwap(qqq))
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        fields = await store.hash_get(main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24"))
        seeded_v = float(fields["sum_v"])
        later = NOW + timedelta(seconds=30)
        await main_bot.scanner_mod.note_session_vwap(store, "SPY", {
            "ts": later.isoformat(),
            "last": 591.0,
            "total_volume": 9_000_000 + 400,
        }, later)
        await main_bot._index_vwap_quotes()
        after = await store.hash_get(main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24"))
        return quotes, reason, fields, after, seeded_v

    quotes, reason, fields, after, seeded_v = _run(scenario())
    by_sym = {row["symbol"]: row for row in quotes}
    assert by_sym["SPY"]["vwap"] == pytest.approx(_vwap(spy))
    assert by_sym["QQQ"]["vwap"] == pytest.approx(_vwap(qqq))
    assert reason is None
    assert float(fields["last_volume"]) == pytest.approx(9_000_000)
    assert seeded_v == pytest.approx(sum(bar["v"] for bar in spy))
    # last_volume was already the live anchor, so only the new 400 shares are added.
    assert float(after["sum_v"]) == pytest.approx(seeded_v + 400)
    assert len(client.urls) == 2
    text = caplog.text
    assert "unit-test-key" not in text
    assert "[VWAP SEED] SPY " in text
    assert "reason=seeded" in text
    assert f"vwap={_vwap(spy):.4f}" in text


def test_empty_seed_then_live_prints_add_only_new_volume(monkeypatch):
    """The first print after a seed anchors cumulative volume. It does not add it."""
    spy = _rth()
    store = main_bot.StateStore(None)
    _arm_gate(monkeypatch, store)
    client = _Client({"SPY": spy, "QQQ": _rth(500.0)})
    _bind_client(monkeypatch, client)

    async def scenario():
        await main_bot.ensure_index_session_vwap_seed(["SPY"])
        key = main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24")
        seeded = await store.hash_get(key)
        seeded_v = float(seeded["sum_v"])
        await main_bot.scanner_mod.note_session_vwap(store, "SPY", {
            "ts": NOW.isoformat(),
            "last": 590.0,
            "total_volume": 50_000_000,
        }, NOW)
        anchored = await store.hash_get(key)
        later = NOW + timedelta(seconds=45)
        await main_bot.scanner_mod.note_session_vwap(store, "SPY", {
            "ts": later.isoformat(),
            "last": 590.5,
            "total_volume": 50_000_000 + 250,
        }, later)
        after = await store.hash_get(key)
        return seeded_v, float(anchored["sum_v"]), float(after["sum_v"]), len(client.urls)

    seeded_v, anchored_v, after_v, fetches = _run(scenario())
    assert anchored_v == pytest.approx(seeded_v)
    assert after_v == pytest.approx(seeded_v + 250)
    assert fetches == 1


def test_fetch_failure_leaves_the_gate_fail_closed(monkeypatch, caplog):
    spy = _rth()
    store = main_bot.StateStore(None)
    _arm_gate(monkeypatch, store)
    tail = _tail(spy)
    held = _acc_from_bars(tail, first=DAY.replace(hour=12, minute=8), last_volume=100)
    client = _Client({}, fail=True)
    _bind_client(monkeypatch, client)
    caplog.set_level(logging.INFO, logger="etrade-bot")
    _cache("SPY", _vwap(spy))
    _cache("QQQ", _vwap(_rth(500.0)))

    async def scenario():
        key = main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24")
        await store.hash_set(key, held, main_bot.scanner_mod.SESSION_VWAP_TTL_S)
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        fields = await store.hash_get(key)
        return quotes, reason, fields

    quotes, reason, fields = _run(scenario())
    assert float(fields["sum_v"]) == pytest.approx(_sum_v(held))
    assert quotes[0]["vwap"] is None
    assert reason is not None and "fail closed" in reason
    assert "reason=fetch_failed" in caplog.text
    assert "unit-test-key" not in caplog.text
    assert "reason=seeded" not in caplog.text


def test_stale_seed_bars_leave_the_gate_fail_closed(monkeypatch, caplog):
    spy = _rth()
    # End the feed 40 minutes before the restart clock. That is stale.
    stale = [bar for bar in spy if _minute_of(bar["t"]) <= 11 * 60 + 40]
    store = main_bot.StateStore(None)
    _arm_gate(monkeypatch, store)
    tail = _tail(spy)
    held = _acc_from_bars(tail, first=DAY.replace(hour=12, minute=8), last_volume=100)
    client = _Client({"SPY": stale, "QQQ": stale})
    _bind_client(monkeypatch, client)
    caplog.set_level(logging.INFO, logger="etrade-bot")
    _cache("SPY", _vwap(spy))
    _cache("QQQ", _vwap(spy))

    async def scenario():
        key = main_bot.scanner_mod.session_vwap_key("SPY", "2026-11-24")
        await store.hash_set(key, held, main_bot.scanner_mod.SESSION_VWAP_TTL_S)
        quotes = await main_bot._index_vwap_quotes()
        reason = main_bot.assess_call_index_vwap_gate("CALL", quotes)
        fields = await store.hash_get(key)
        return quotes, reason, fields

    quotes, reason, fields = _run(scenario())
    assert float(fields["sum_v"]) == pytest.approx(_sum_v(held))
    assert quotes[0]["vwap"] is None
    assert reason is not None and "fail closed" in reason
    assert "reason=stale" in caplog.text
    assert "unit-test-key" not in caplog.text
