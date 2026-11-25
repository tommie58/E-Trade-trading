"""Observe-only SPY/QQQ VWAP comparison. The gate's decision inputs stay put.

day.vw of 0, negative, missing, NaN, or non-numeric is unmeasured everywhere
the observe block reports it: null value, reason day_vw_zero_or_missing, and
a null bps diff. A 0.0 is never shown as a VWAP and never fed to the gate.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient

import main_bot


ET = ZoneInfo("America/New_York")
_MISSING = object()
DAY_REASON = "day_vw_zero_or_missing"
DAY_LABEL = "includes premarket (from 4:00 ET)"
UNMEASURED = (
    "CALL blocked — SPY/QQQ day VWAP unmeasured — fail closed until an index VWAP is measured"
)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _clear_minute_cache():
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._status_observe_quotes.clear()
    main_bot._status_observe_flight = None
    yield
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._status_observe_quotes.clear()
    main_bot._status_observe_flight = None


def _save():
    return {
        "state": main_bot.state,
        "scanner": main_bot.scanner,
        "client": main_bot.massive_client,
        "source": dict(main_bot._scanner_quote_source),
        "desk": main_bot._PRO_TRADER,
    }


def _restore(saved):
    main_bot.state = saved["state"]
    main_bot.scanner = saved["scanner"]
    main_bot.massive_client = saved["client"]
    main_bot._scanner_quote_source.clear()
    main_bot._scanner_quote_source.update(saved["source"])
    main_bot._PRO_TRADER = saved["desk"]


def _ticker(symbol, day_vw=_MISSING, minute_vw=_MISSING, last=500.0):
    body = {
        "ticker": symbol,
        "lastTrade": {"p": last, "t": time_ns()},
        "day": {"c": last, "v": 1000, "o": last, "h": last, "l": last},
        "min": {"c": last},
    }
    if day_vw is not _MISSING:
        body["day"]["vw"] = day_vw
    if minute_vw is not _MISSING:
        body["min"]["vw"] = minute_vw
    return body


def time_ns():
    return int(datetime.now(tz=ET).timestamp() * 1e9)


def _translated(symbol, day_vw=_MISSING, minute_vw=_MISSING, last=500.0):
    now = datetime.now(tz=ET).timestamp()
    return main_bot.massive.snapshot_to_quote_data(
        _ticker(symbol, day_vw, minute_vw, last), now,
    )


def _parsed(symbol, day_vw=_MISSING, minute_vw=_MISSING, last=500.0):
    entry = _translated(symbol, day_vw, minute_vw, last)
    rows = main_bot.scanner_mod.parse_quotes({"QuoteResponse": {"QuoteData": [entry]}})
    assert len(rows) == 1
    return rows[0]


def _zero_quote_response(spy_vwap=510.25):
    """SPY day.vw is 0. QQQ day.vw is missing. SPY's gate VWAP is a real price."""
    now = datetime.now(tz=ET).timestamp()
    resp = main_bot.massive.snapshot_to_quote_response(
        {"tickers": [
            _ticker("SPY", 0, minute_vw=0, last=505.12),
            _ticker("QQQ", _MISSING, minute_vw=_MISSING, last=430.5),
        ]},
        now,
    )
    for entry in resp["QuoteResponse"]["QuoteData"]:
        if entry["Product"]["symbol"] == "SPY":
            entry["All"]["vwap"] = spy_vwap
    return resp


def _assert_unmeasured_day(row):
    assert row["massive_day_vwap"] is None
    assert row["massive_day_vwap_reason"] == DAY_REASON
    assert row["massive_day_vwap_label"] == DAY_LABEL
    assert row["massive_day_vwap_diff_bps"] is None
    blob = json.dumps(row)
    assert '"massive_day_vwap": 0' not in blob
    assert '"massive_day_vwap": 0.0' not in blob
    assert '"massive_day_vwap":0' not in blob


def test_translator_keeps_positive_day_vw_off_the_gate_key():
    entry = _translated("SPY", 497.25, minute_vw=498.5, last=500.0)
    assert entry["massive_day_vwap"] == 497.25
    assert entry["massive_day_vwap_reason"] is None
    assert entry["massive_day_vwap_label"] == DAY_LABEL
    assert entry["massive_minute_vw"] == 498.5
    assert entry["massive_minute_vw_reason"] is None
    assert "vwap" not in entry["All"]
    assert "VWAP" not in entry["All"]
    assert "averagePrice" not in entry["All"]
    assert "massive_day_vwap" not in entry["All"]
    parsed = main_bot.scanner_mod.parse_quotes({"QuoteResponse": {"QuoteData": [entry]}})[0]
    assert parsed["vwap"] is None
    assert parsed["vwap_field"] is None
    assert parsed["massive_day_vwap"] == 497.25
    assert parsed["massive_day_vwap_reason"] is None
    # A real day VWAP that never lands in `vwap` is not a pass input.
    gate = main_bot.assess_call_index_vwap_gate("CALL", [parsed, _parsed("QQQ", 400.0)])
    assert gate == UNMEASURED


@pytest.mark.parametrize("day_vw", [0, 0.0, -1, -0.01, None, float("nan"), "nope", "0", True, False])
def test_day_vw_zero_negative_missing_non_numeric_is_unmeasured(day_vw):
    if day_vw is None:
        entry = _translated("SPY", _MISSING, last=500.0)
    else:
        entry = _translated("SPY", day_vw, last=500.0)
    assert entry["massive_day_vwap"] is None
    assert entry["massive_day_vwap_reason"] == DAY_REASON
    assert "vwap" not in entry["All"]
    parsed = main_bot.scanner_mod.parse_quotes({"QuoteResponse": {"QuoteData": [entry]}})[0]
    assert parsed["vwap"] is None
    assert parsed["massive_day_vwap"] is None
    assert parsed["massive_day_vwap_reason"] == DAY_REASON
    other = _parsed("QQQ", _MISSING)
    gate = main_bot.assess_call_index_vwap_gate("CALL", [parsed, other])
    assert gate == UNMEASURED
    assert gate is not None


def test_leaked_zero_on_a_quote_is_still_unmeasured_in_observe():
    now = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    row = main_bot.observe_index_vwap_row(
        {
            "symbol": "SPY",
            "vwap": 510.25,
            "vwap_source": "quote",
            "vwap_field": "vwap",
            "massive_day_vwap": 0,
            "massive_minute_vw": float("nan"),
        },
        [],
        now,
    )
    _assert_unmeasured_day(row)
    assert row["current_vwap"] == 510.25
    assert row["massive_minute_vw"] is None
    assert row["massive_minute_vw_reason"] == "min_vw_zero_or_missing"
    assert row["massive_minute_vw_diff_bps"] is None


@pytest.mark.parametrize("bad", [0, 0.0, -4, float("nan"), "nope", True, None, ""])
def test_bps_is_null_when_day_vwap_is_unmeasured(bad):
    now = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    row = main_bot.observe_index_vwap_row(
        {"symbol": "SPY", "vwap": 500.0, "vwap_source": "quote", "massive_day_vwap": bad},
        [],
        now,
    )
    _assert_unmeasured_day(row)
    assert row["current_vwap"] == 500.0


def test_bps_is_computed_only_for_two_positive_vwaps():
    now = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    row = main_bot.observe_index_vwap_row(
        {
            "symbol": "SPY",
            "vwap": 500.0,
            "vwap_source": "quote",
            "vwap_field": "vwap",
            "massive_day_vwap": 505.0,
            "massive_minute_vw": 502.5,
        },
        [],
        now,
    )
    assert row["massive_day_vwap"] == 505.0
    assert row["massive_day_vwap_reason"] is None
    assert row["massive_day_vwap_diff_bps"] == 100.0
    assert row["massive_minute_vw_diff_bps"] == 50.0
    # A zero baseline is not a VWAP, so the diff stays null.
    zero_base = main_bot.observe_index_vwap_row(
        {"symbol": "SPY", "vwap": 0, "massive_day_vwap": 505.0},
        [],
        now,
    )
    assert zero_base["current_vwap"] is None
    assert zero_base["massive_day_vwap"] == 505.0
    assert zero_base["massive_day_vwap_diff_bps"] is None


def test_parse_quotes_vwap_field_keeps_the_truthy_or_chain():
    def one(all_q):
        entry = {
            "Product": {"symbol": "SPY"},
            "quoteStatus": "REALTIME",
            "All": {"lastTrade": 500.0, **all_q},
        }
        return main_bot.scanner_mod.parse_quotes({"QuoteResponse": {"QuoteData": [entry]}})[0]

    assert one({"vwap": 480.0, "VWAP": 470.0})["vwap_field"] == "vwap"
    assert one({"vwap": 480.0, "VWAP": 470.0})["vwap"] == 480.0
    assert one({"vwap": 0, "VWAP": 470.0})["vwap_field"] == "VWAP"
    assert one({"vwap": 0, "VWAP": 470.0})["vwap"] == 470.0
    assert one({"vwap": 0, "VWAP": 0, "averagePrice": 501.5})["vwap"] == 501.5
    assert one({"vwap": 0, "VWAP": 0, "averagePrice": 501.5})["vwap_field"] == "averagePrice"
    # `a or b or c` returns the last operand when every one is falsy, so an
    # all-zero quote keeps 0.0. The gate still treats that as unmeasured.
    zeros = one({"vwap": 0, "VWAP": 0, "averagePrice": 0})
    assert zeros["vwap"] == 0.0
    assert zeros["vwap_field"] is None
    assert main_bot.assess_call_index_vwap_gate("CALL", [zeros, one({"vwap": 0}) | {"symbol": "QQQ", "last": 400.0, "vwap": 0}]) == UNMEASURED


def test_extra_day_vwap_does_not_change_the_gate():
    below = [
        {"symbol": "SPY", "last": 500.0, "vwap": 505.0, "vwap_source": "quote"},
        {"symbol": "QQQ", "last": 400.0, "vwap": 410.0, "vwap_source": "quote"},
    ]
    # 490 would pass SPY if it were written into `vwap`. It must not.
    leaked = [
        {**below[0], "massive_day_vwap": 0, "massive_minute_vw": 490.0},
        {**below[1], "massive_day_vwap": 490.0},
    ]
    blocked = main_bot.assess_call_index_vwap_gate("CALL", below)
    assert blocked == main_bot.assess_call_index_vwap_gate("CALL", leaked)
    assert blocked is not None and blocked.startswith("CALL blocked while the index is below day VWAP")

    above = [
        {"symbol": "SPY", "last": 510.0, "vwap": 505.0, "vwap_source": "quote"},
        {"symbol": "QQQ", "last": 390.0, "vwap": 410.0, "massive_day_vwap": 0},
    ]
    assert main_bot.assess_call_index_vwap_gate("CALL", above) is None
    bare_above = [
        {"symbol": "SPY", "last": 510.0, "vwap": 505.0},
        {"symbol": "QQQ", "last": 390.0, "vwap": 410.0},
    ]
    assert main_bot.assess_call_index_vwap_gate("CALL", bare_above) is None


def _series():
    """Prior-day samples stay in the window. Only today's regular session counts."""
    return [
        {"ts": "2026-11-23T14:00:00-05:00", "last": 100.0, "total_volume": 1000},
        {"ts": "2026-11-23T14:01:00-05:00", "last": 110.0, "total_volume": 2000},
        {"ts": "2026-11-24T10:00:00-05:00", "last": 200.0, "total_volume": 100},
        {"ts": "2026-11-24T10:01:00-05:00", "last": 210.0, "total_volume": 500},
    ]


def test_session_sampled_vwap_drops_prior_day_and_gate_does_not_keep_full_series(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setenv("MASSIVE_ENABLED", "false")

    async def fetch_bars(symbol, day):
        raise AssertionError(f"Massive minute fetch ran for {symbol} {day}")

    monkeypatch.setattr(main_bot, "_fetch_massive_minute_bars", fetch_bars)
    series = _series()
    now = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    full = main_bot.scanner_mod.sampled_vwap(series)
    today = main_bot.scanner_mod.sampled_vwap(main_bot.scanner_mod.session_series(series, now))
    assert full == pytest.approx(133.5714)
    assert today == pytest.approx(205.0)
    assert full != pytest.approx(today)

    class Scan:
        async def _series(self, ticker):
            return list(series) if ticker == "SPY" else []

    saved_scanner = main_bot.scanner
    original_fetch = main_bot._scanner_fetch_quotes
    saved_now = main_bot._utcnow
    saved_state = main_bot.state

    async def fetch(symbols, *, dry_run=False):
        return {"QuoteResponse": {"QuoteData": [
            {"Product": {"symbol": "SPY"}, "quoteStatus": "REALTIME", "All": {"lastTrade": 210.0}},
            {"Product": {"symbol": "QQQ"}, "quoteStatus": "REALTIME", "All": {"lastTrade": 400.0, "vwap": 410.0}},
        ]}}

    async def run():
        main_bot.scanner = Scan()
        main_bot._scanner_fetch_quotes = fetch
        main_bot._utcnow = lambda: now.astimezone(timezone.utc)
        # The gate reads the per-date hash, not the mocked capped list.
        # Two today prints are under the floor, so the full-series VWAP is unused.
        main_bot.state = main_bot.StateStore(None)
        for sample in series:
            await main_bot.scanner_mod.note_session_vwap(main_bot.state, "SPY", sample, now)
        try:
            return await main_bot._index_vwap_quotes()
        finally:
            main_bot._scanner_fetch_quotes = original_fetch
            main_bot.scanner = saved_scanner
            main_bot._utcnow = saved_now
            main_bot.state = saved_state

    quotes = _run(run())
    spy = next(q for q in quotes if q["symbol"] == "SPY")
    # Two today prints are under the sample floor, so the stale full-series
    # VWAP is not the gate input and the thin today print is not measured.
    assert spy["vwap"] is None
    assert spy["vwap_source"] is None
    assert spy["gate_vwap_verdict"] == "unmeasured"
    assert spy.get("gate_vwap_reason") == "today_samples_insufficient"
    assert spy["vwap"] != pytest.approx(full)
    row = main_bot.observe_index_vwap_row(spy, series, now)
    assert row["current_vwap"] is None
    assert row["session_sampled_vwap"] == pytest.approx(today)
    assert row["samples_in_window"] == 4
    assert row["session_samples"] == 2
    assert row["gate_vwap_verdict"] == "unmeasured"
    assert row["session_sampled_vwap_diff_bps"] is None
    spy["massive_day_vwap"] = 0
    zero = main_bot.observe_index_vwap_row(spy, series, now)
    _assert_unmeasured_day(zero)
    assert zero["current_vwap"] is None
    assert zero["session_sampled_vwap"] == pytest.approx(today)


def test_minute_aggs_rth_vwap_starts_at_the_open():
    now = datetime(2026, 11, 24, 10, 40, tzinfo=ET)

    def bar(hour, minute, vw, vol, close=None):
        ts = datetime(2026, 11, 24, hour, minute, tzinfo=ET)
        return {"t": ts.timestamp() * 1000, "vw": vw, "v": vol, "c": close if close is not None else vw}

    payload = {"results": [
        bar(8, 0, 100, 1000),
        bar(9, 30, 200, 100),
        bar(9, 31, 210, 100),
        bar(16, 0, 300, 500),
    ]}
    out = main_bot.minute_aggs_rth_vwap(payload, now)
    assert out["vwap"] == pytest.approx(205.0)
    assert out["bar_count"] == 2
    assert out["last_bar_ts"]
    day = datetime.fromtimestamp(out["last_bar_epoch"], tz=ET).date().isoformat()
    seen_at = datetime.fromtimestamp(out["last_bar_epoch"] + 120, tz=ET)
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key("SPY", day)] = out
    view = main_bot._cached_minute_vwap_view("SPY", seen_at)
    assert view["vwap"] == pytest.approx(205.0)
    assert view["reason"] is None
    assert view["age_seconds"] == 120.0
    # A cached 0 is not a VWAP.
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key("QQQ", day)] = {
        "vwap": 0, "last_bar_epoch": out["last_bar_epoch"],
    }
    zero = main_bot._cached_minute_vwap_view("QQQ", seen_at)
    assert zero["vwap"] is None


def test_block_matches_the_gate_and_observe_failure_does_not_change_it(monkeypatch):
    quotes = [
        {"symbol": "SPY", "last": 500.0, "vwap": None, "massive_day_vwap": 0},
        {"symbol": "QQQ", "last": 400.0, "vwap": None, "massive_day_vwap": "nope"},
    ]
    expected = main_bot.assess_call_index_vwap_gate("CALL", quotes)
    assert expected == UNMEASURED

    async def feed(symbols=None, *, dry_run=False):
        return [dict(q) for q in quotes]

    refreshes = {"n": 0}

    async def refresh(symbols=None):
        refreshes["n"] += 1

    real_build = main_bot.build_call_index_vwap_observe
    monkeypatch.setattr(main_bot, "_index_vwap_quotes", feed)
    monkeypatch.setattr(main_bot, "refresh_massive_minute_vwap", refresh)

    payload = {"instrument": "option", "option_right": "CALL", "action": "BUY", "ticker": "NVDA"}
    reason = _run(main_bot._call_index_vwap_block(payload))
    assert reason == expected
    assert refreshes["n"] == 0
    spy = next(r for r in payload["call_index_vwap_observe"]["indexes"] if r["symbol"] == "SPY")
    _assert_unmeasured_day(spy)
    assert spy["massive_minute_vwap_rth"] is None

    async def boom(quotes=None):
        raise RuntimeError("observe down")

    monkeypatch.setattr(main_bot, "build_call_index_vwap_observe", boom)
    again = {"instrument": "option", "option_right": "CALL", "action": "BUY"}
    reason = _run(main_bot._call_index_vwap_block(again))
    assert reason == expected
    assert again["call_index_vwap_observe"]["read_only"] is True
    assert again["call_index_vwap_observe"]["indexes"][0]["massive_day_vwap"] is None
    assert refreshes["n"] == 0

    # A cached minute VWAP from today's session is reported. The entry path still does not refresh.
    now_et = main_bot._utcnow().astimezone(ET)
    epoch = now_et.timestamp() - 90
    if datetime.fromtimestamp(epoch, tz=ET).date() != now_et.date():
        epoch = now_et.timestamp()
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key("SPY", now_et.date().isoformat())] = {
        "vwap": 205.0,
        "last_bar_epoch": epoch,
        "last_bar_ts": datetime.fromtimestamp(epoch, tz=ET).isoformat(),
        "bar_count": 2,
        "session_date": now_et.date().isoformat(),
    }
    monkeypatch.setattr(main_bot, "build_call_index_vwap_observe", real_build)
    third = {"instrument": "option", "option_right": "CALL"}
    reason = _run(main_bot._call_index_vwap_block(third))
    assert reason == expected
    assert refreshes["n"] == 0
    spy = next(r for r in third["call_index_vwap_observe"]["indexes"] if r["symbol"] == "SPY")
    assert spy["massive_minute_vwap_rth"] == pytest.approx(205.0)
    assert spy["massive_minute_vwap_rth_bar_age_seconds"] is not None
    _assert_unmeasured_day(spy)


def test_filter_blocked_by_is_unchanged_when_observe_raises(monkeypatch):
    saved = _save()
    quotes = [
        {"symbol": "SPY", "last": 500.0, "vwap": 505.0, "vwap_source": "quote", "massive_day_vwap": 0.0},
        {"symbol": "QQQ", "last": 400.0, "vwap": 410.0, "vwap_source": "quote", "massive_day_vwap": -1},
    ]
    expected = main_bot.assess_call_index_vwap_gate("CALL", quotes)

    async def feed(symbols=None, *, dry_run=False):
        return [dict(q) for q in quotes]

    monkeypatch.setattr(main_bot, "_index_vwap_quotes", feed)
    payload = {
        "ticker": "NVDA",
        "action": "BUY",
        "instrument": "option",
        "option_right": "CALL",
        "setup": "breakout",
        "score": 90,
        "rvol": 2.0,
        "mode": "paper",
    }

    async def scenario():
        main_bot.state = main_bot.StateStore(None)
        first = dict(payload)
        ok1, blocked1 = await main_bot._passes_entry_filters(first, dry_run=True)
        real = main_bot.build_call_index_vwap_observe

        async def boom(quotes=None):
            raise RuntimeError("observe down")

        monkeypatch.setattr(main_bot, "build_call_index_vwap_observe", boom)
        second = dict(payload)
        ok2, blocked2 = await main_bot._passes_entry_filters(second, dry_run=True)
        return ok1, blocked1, ok2, blocked2, first

    try:
        ok1, blocked1, ok2, blocked2, first = _run(scenario())
    finally:
        _restore(saved)
    assert blocked1 == blocked2
    assert ok1 is ok2
    assert expected in blocked1
    spy = next(r for r in first["call_index_vwap_observe"]["indexes"] if r["symbol"] == "SPY")
    _assert_unmeasured_day(spy)


def test_entry_and_ledger_observe_treat_day_vw_zero_as_unmeasured(monkeypatch):
    saved = _save()
    quotes = [
        _parsed("SPY", 0, minute_vw=0, last=505.12),
        _parsed("QQQ", _MISSING, last=430.5),
    ]
    quotes[0]["vwap"] = 510.25
    quotes[0]["vwap_source"] = "quote"
    quotes[0]["vwap_field"] = "vwap"
    seen = []

    async def feed(symbols=None, *, dry_run=False):
        return [dict(q) for q in quotes]

    async def record(event, data=None):
        seen.append((event, data))
        return {"event": event}

    monkeypatch.setattr(main_bot, "_index_vwap_quotes", feed)
    monkeypatch.setattr(main_bot.trade_ledger, "record", record)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        await store.set_position("NVDA", {"ticker": "NVDA", "qty": 1})
        pos = await main_bot._stamp_entry_market_context("NVDA")
        return pos

    try:
        pos = _run(scenario())
    finally:
        _restore(saved)

    observe = pos["entry_index_vwap_observe"]
    spy = next(r for r in observe["indexes"] if r["symbol"] == "SPY")
    qqq = next(r for r in observe["indexes"] if r["symbol"] == "QQQ")
    _assert_unmeasured_day(spy)
    _assert_unmeasured_day(qqq)
    assert spy["current_vwap"] == 510.25
    assert spy["massive_minute_vw"] is None
    assert spy["massive_minute_vw_reason"] == "min_vw_zero_or_missing"
    # The journal row the gate already writes is unchanged and is not 0.
    journal = pos["entry_index_vwap"]
    assert journal[0]["symbol"] == "SPY"
    assert journal[0]["vwap"] == pytest.approx(510.25)
    assert "massive_day_vwap" not in journal[0]
    ledger = next(data for event, data in seen if event == "entry_market_context")
    ledger_spy = next(r for r in ledger["entry_index_vwap_observe"]["indexes"] if r["symbol"] == "SPY")
    _assert_unmeasured_day(ledger_spy)
    review = main_bot.build_trade_review_fields(pos)
    assert review["entry_index_vwap_observe"]["indexes"][0]["massive_day_vwap"] is None
    assert review["entry_index_vwap"][0]["vwap"] == pytest.approx(510.25)


def test_rejected_entry_ledger_carries_unmeasured_day_vwap(monkeypatch):
    saved = _save()
    quotes = [
        {"symbol": "SPY", "last": 500.0, "vwap": None, "massive_day_vwap": 0},
        {"symbol": "QQQ", "last": 400.0, "vwap": None},
    ]
    seen = []

    async def feed(symbols=None, *, dry_run=False):
        return [dict(q) for q in quotes]

    async def record(event, data=None):
        seen.append((event, data))
        return {"event": event}

    async def refuse_order(payload):
        raise AssertionError("order path reached")

    # 11:00 ET, outside the 15:40 flatten window, so the test does not depend on the clock.
    monkeypatch.setattr(
        main_bot, "_utcnow",
        lambda: datetime(2026, 11, 24, 16, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(main_bot, "_index_vwap_quotes", feed)
    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    monkeypatch.setattr(main_bot, "_is_market_open", lambda: True)
    monkeypatch.setattr(main_bot, "execute_live_order", refuse_order)
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        await store.set_killed(True)
        return await main_bot.process_signal({
            "ticker": "NVDA",
            "action": "BUY",
            "instrument": "option",
            "option_right": "CALL",
            "mode": "live",
            "signal_id": "observe-day-vw-zero",
            "score": 90,
            "rvol": 2.0,
            "setup": "breakout",
        })

    try:
        result = _run(scenario())
    finally:
        _restore(saved)

    assert result["status"] == "rejected"
    assert "unmeasured" in result["reason"] or "kill switch" in result["reason"]
    rejected = next(data for event, data in seen if event == "entry_rejected")
    assert any("kill switch" in item for item in rejected["blocked_by"])
    spy = next(r for r in rejected["call_index_vwap_observe"]["indexes"] if r["symbol"] == "SPY")
    _assert_unmeasured_day(spy)
    assert UNMEASURED in rejected["blocked_by"]


class _Resp:
    def __init__(self, code, body):
        self.status_code = code
        self._body = body

    def json(self):
        return self._body


class _Client:
    """httpx stand-in. Request headers are never stored."""

    def __init__(self, *args, **kwargs):
        self.mode = kwargs.get("mode")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        code = _Client.code
        if code in (401, 403):
            return _Resp(code, {})
        if "snapshot" in url:
            now_ticker = _ticker("SPY", 0, last=500.0)
            return _Resp(200, {"tickers": [now_ticker]})
        return _Resp(200, {"results": []})


_Client.code = 200


def test_snapshot_401_and_403_are_generic_and_a_later_success_keeps_the_count(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    client = main_bot.massive.MassiveClient()

    _Client.code = 403
    with pytest.raises(RuntimeError, match="HTTP 403"):
        _run(client.fetch_quote_response(["SPY"]))
    assert client.snapshot_auth == "not_authorized"
    assert client.snapshot_auth_failure_count == 1
    assert client.snapshot_auth_failed_at is not None
    assert client.auth_status == 403
    assert client.failures == 1
    tel = client.stocks_snapshot_telemetry()
    assert tel["stocks_snapshot_auth"] == "not_authorized"
    assert tel["stocks_snapshot_auth_failure_count"] == 1
    assert tel["stocks_snapshot_auth_at"]
    assert "lack" not in json.dumps(tel)

    _Client.code = 401
    with pytest.raises(RuntimeError, match="HTTP 401"):
        _run(client.fetch_quote_response(["SPY"]))
    assert client.snapshot_auth == "unauthorized"
    assert client.snapshot_auth_failure_count == 2
    assert client.auth_status == 401

    _Client.code = 200
    resp = _run(client.fetch_quote_response(["SPY"]))
    entry = resp["QuoteResponse"]["QuoteData"][0]
    assert entry["massive_day_vwap"] is None
    assert entry["massive_day_vwap_reason"] == DAY_REASON
    assert "vwap" not in entry["All"]
    assert client.snapshot_auth == "ok"
    assert client.snapshot_auth_failure_count == 2
    assert client.auth_status is None
    assert "vwap" not in client.status()
    assert "stocks_snapshot_auth" not in client.status()


def test_dry_run_snapshot_does_not_touch_live_auth_counters(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    _Client.code = 403
    client = main_bot.massive.MassiveClient()
    with pytest.raises(RuntimeError, match="HTTP 403"):
        _run(client.fetch_quote_response(["SPY"], dry_run=True))
    assert client.snapshot_auth is None
    assert client.snapshot_auth_failure_count == 0
    assert client.failures == 0
    assert client.auth_status is None


def test_fallback_still_happens_and_quote_source_stays_etrade(monkeypatch):
    saved = _save()
    called = []

    async def boom(symbols, *, dry_run=False):
        raise RuntimeError("Massive rejected the API key (HTTP 403)")

    async def etrade(symbols):
        called.append(list(symbols))
        return {"QuoteResponse": {"QuoteData": []}}

    async def persist():
        return None

    monkeypatch.setattr(main_bot.massive_client, "fetch_quote_response", boom)
    monkeypatch.setattr(main_bot, "_scanner_fetch_etrade_quotes", etrade)
    monkeypatch.setattr(main_bot, "_persist_feed_health", persist)
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")

    async def scenario():
        main_bot._scanner_quote_source.clear()
        await main_bot._scanner_fetch_quotes(["SPY", "QQQ"])
        return main_bot._market_data_status()

    try:
        data = _run(scenario())
    finally:
        _restore(saved)

    assert called == [["SPY", "QQQ"]]
    assert data["quote_source"] == "etrade_fallback"
    assert data["scanner_feed"]["last"] == "etrade"
    assert "stocks_snapshot_auth" in data
    assert "stocks_snapshot_auth_failure_count" in data


def test_status_day_vw_zero_is_unmeasured(monkeypatch):
    """GET /status observe: day.vw = 0 is null + day_vw_zero_or_missing, never 0.0."""
    saved = _save()
    frozen_enabled = main_bot.CALL_INDEX_VWAP_GATE
    frozen_symbols = main_bot.CALL_INDEX_VWAP_SYMBOLS
    response_body = _zero_quote_response()
    fetches = []

    async def fetch(symbols, *, dry_run=False):
        fetches.append(dry_run)
        return response_body

    monkeypatch.setattr(main_bot, "_scanner_fetch_quotes", fetch)
    monkeypatch.setenv("CALL_INDEX_VWAP_GATE", "false")
    monkeypatch.setenv("CALL_INDEX_VWAP_SYMBOLS", "dia, spy")
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    try:
        with TestClient(main_bot.app) as client:
            response = client.get("/status", headers={"X-Rork-Secret": "s3cret"})
    finally:
        _restore(saved)

    assert response.status_code == 200
    assert fetches
    assert all(flag is True for flag in fetches)
    body = response.json()
    gate = body["filters"]["call_index_vwap_gate"]
    assert gate["enabled"] is frozen_enabled
    assert gate["symbols"] == list(frozen_symbols)
    assert gate["mode"] == ("fail_closed" if frozen_enabled else "off")
    text = response.text
    assert '"massive_day_vwap": 0' not in text
    assert '"massive_day_vwap": 0.0' not in text
    assert '"massive_day_vwap":0' not in text
    assert '"massive_day_vwap":0.0' not in text
    indexes = {row["symbol"]: row for row in gate["observe"]["indexes"]}
    assert gate["observe"]["massive_day_vwap_label"] == DAY_LABEL
    assert gate["observe"]["read_only"] is True
    spy = indexes["SPY"]
    qqq = indexes["QQQ"]
    _assert_unmeasured_day(spy)
    _assert_unmeasured_day(qqq)
    # A broker vwap field with no today-session samples is not the gate input.
    assert spy["current_vwap"] is None
    assert spy["vwap_source"] is None
    assert spy["vwap_field"] is None
    assert spy["gate_vwap_source"] is None
    assert spy["gate_vwap_verdict"] == "unmeasured"
    assert gate["observe"]["gate_vwap_source"] == "session_sampled"
    assert gate["observe"]["gate_vwap_verdict"] == "unmeasured"
    assert spy["massive_minute_vw"] is None
    assert spy["massive_minute_vw_reason"] == "min_vw_zero_or_missing"
    assert spy["massive_minute_vw_diff_bps"] is None
    # QQQ had no day.vw at all. Same unmeasured reason, and no invented 0.
    assert qqq["massive_day_vwap"] is None
    market = body["market_data"]
    assert "stocks_snapshot_auth" in market
    assert "stocks_snapshot_auth_at" in market
    assert "stocks_snapshot_auth_failure_count" in market
    assert "quote_source" in market
    assert "scanner_feed" in market


def _seed_minute(symbol, when, vwap):
    day = when.astimezone(ET).date().isoformat()
    payload = {
        "vwap": vwap,
        "last_bar_epoch": when.timestamp(),
        "last_bar_ts": when.isoformat(),
        "bar_count": 4,
        "session_date": day,
    }
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key(symbol, day)] = payload
    return payload


def test_prior_day_minute_vwap_is_null_today():
    yesterday = datetime(2026, 11, 23, 15, 0, tzinfo=ET)
    today = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    _seed_minute("SPY", yesterday, 199.5)
    # The old unkeyed slot must not leak through either.
    main_bot._minute_vwap_cache["SPY"] = {
        "vwap": 199.5,
        "last_bar_epoch": yesterday.timestamp(),
    }
    view = main_bot._cached_minute_vwap_view("SPY", today)
    assert view["vwap"] is None
    assert view["reason"] == "minute_vwap_not_today"
    row = main_bot.observe_index_vwap_row(
        {"symbol": "SPY", "vwap": 500.0, "vwap_source": "quote"},
        [],
        today,
        view,
    )
    assert row["massive_minute_vwap_rth"] is None
    assert row["massive_minute_vwap_rth_reason"] == "minute_vwap_not_today"
    assert row["massive_minute_vwap_rth_diff_bps"] is None
    assert 199.5 not in row.values()


def test_premarket_refresh_with_no_rth_bars_is_null(monkeypatch):
    frozen = datetime(2026, 11, 24, 8, 0, tzinfo=ET)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: frozen.astimezone(timezone.utc))
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    _seed_minute("SPY", datetime(2026, 11, 23, 15, 0, tzinfo=ET), 188.0)

    class _Quiet:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            ts = datetime(2026, 11, 24, 8, 0, tzinfo=ET).timestamp() * 1000
            return _Resp(200, {"results": [{"t": ts, "vw": 180.0, "v": 100, "c": 180.0}]})

    monkeypatch.setattr(httpx, "AsyncClient", _Quiet)
    _run(main_bot.refresh_massive_minute_vwap(["SPY", "QQQ"]))
    today_key = main_bot._minute_vwap_cache_key("SPY", "2026-11-24")
    assert today_key not in main_bot._minute_vwap_cache
    view = main_bot._cached_minute_vwap_view("SPY", frozen)
    assert view["vwap"] is None
    assert view["reason"] == "no_rth_bars_yet"
    assert view["vwap"] != 188.0
    row = main_bot.observe_index_vwap_row({"symbol": "SPY", "vwap": 500.0}, [], frozen, view)
    assert row["massive_minute_vwap_rth"] is None
    assert row["massive_minute_vwap_rth_reason"] == "no_rth_bars_yet"


def test_today_rth_minute_vwap_is_served(monkeypatch):
    frozen = datetime(2026, 11, 24, 10, 40, tzinfo=ET)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: frozen.astimezone(timezone.utc))
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")

    def bar(hour, minute, vw, vol):
        ts = datetime(2026, 11, 24, hour, minute, tzinfo=ET)
        return {"t": ts.timestamp() * 1000, "vw": vw, "v": vol, "c": vw}

    class _Rth:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            return _Resp(200, {"results": [bar(10, 38, 200, 100), bar(10, 39, 210, 100)]})

    monkeypatch.setattr(httpx, "AsyncClient", _Rth)
    _run(main_bot.refresh_massive_minute_vwap(["SPY"]))
    view = main_bot._cached_minute_vwap_view("SPY", frozen)
    assert view["vwap"] == pytest.approx(205.0)
    assert view["reason"] is None
    row = main_bot.observe_index_vwap_row({"symbol": "SPY", "vwap": 500.0}, [], frozen, view)
    assert row["massive_minute_vwap_rth"] == pytest.approx(205.0)
    assert row["massive_minute_vwap_rth_reason"] is None
    assert row["massive_minute_vwap_rth_diff_bps"] is not None


class _CountingClient:
    """Counts snapshot and daily-agg URLs. Never stores request headers."""

    snapshot_urls = []
    day_urls = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None, headers=None):
        text = str(url)
        if "/v2/snapshot/" in text:
            _CountingClient.snapshot_urls.append(text)
            now_ns = int(datetime(2026, 11, 24, 10, 40, tzinfo=ET).timestamp() * 1e9)
            tickers = []
            for sym, last in (("SPY", 510.0), ("QQQ", 440.0)):
                tickers.append({
                    "ticker": sym,
                    "lastTrade": {"p": last, "t": now_ns},
                    "day": {"c": last, "v": 1000, "vw": last - 1},
                    "min": {"c": last, "vw": last},
                })
            return _Resp(200, {"tickers": tickers})
        if "/range/1/day/" in text:
            _CountingClient.day_urls.append(text)
            return _Resp(200, {"results": []})
        return _Resp(200, {"results": []})


def test_status_snapshot_is_cached_and_shadow_skips_adv(monkeypatch):
    saved = _save()
    client = main_bot.massive_client
    saved_adv = dict(getattr(client, "_adv", {}) or {})
    saved_day = getattr(client, "_adv_day", None)
    saved_calls = client.calls
    saved_failures = client.failures
    _CountingClient.snapshot_urls = []
    _CountingClient.day_urls = []
    monkeypatch.setattr(httpx, "AsyncClient", _CountingClient)
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    client._adv = {}
    client._adv_day = None
    main_bot._status_observe_quotes.clear()
    main_bot._status_observe_flight = None

    try:
        with TestClient(main_bot.app) as http:
            first = http.get("/status", headers={"X-Rork-Secret": "s3cret"})
            second = http.get("/status", headers={"X-Rork-Secret": "s3cret"})
        assert first.status_code == 200
        assert second.status_code == 200
        assert len(_CountingClient.snapshot_urls) == 1
        assert _CountingClient.day_urls == []
        assert client._adv == {}
        assert client._adv_day is None
        assert client.calls == saved_calls
        assert client.failures == saved_failures

        async def both():
            main_bot._status_observe_quotes.clear()
            main_bot._status_observe_flight = None
            _CountingClient.snapshot_urls = []
            _CountingClient.day_urls = []
            await asyncio.gather(
                main_bot.call_index_vwap_gate_view(),
                main_bot.call_index_vwap_gate_view(),
            )

        _run(both())
        assert len(_CountingClient.snapshot_urls) == 1
        assert _CountingClient.day_urls == []
        assert client._adv == {}
        assert client._adv_day is None

        shadow = main_bot.massive.MassiveClient()
        shadow._adv = {}
        shadow._adv_day = None
        before = (dict(shadow._adv), shadow._adv_day, shadow.calls, shadow.failures)
        _CountingClient.day_urls = []
        _run(shadow.fetch_quote_response(["SPY", "QQQ"], dry_run=True))
        assert _CountingClient.day_urls == []
        assert (dict(shadow._adv), shadow._adv_day, shadow.calls, shadow.failures) == before

        live = main_bot.massive.MassiveClient()
        live._adv = {}
        live._adv_day = None
        _CountingClient.day_urls = []
        _run(live.fetch_quote_response(["SPY", "QQQ"]))
        assert len(_CountingClient.day_urls) == 2
    finally:
        client._adv = saved_adv
        client._adv_day = saved_day
        client.calls = saved_calls
        client.failures = saved_failures
        _restore(saved)
