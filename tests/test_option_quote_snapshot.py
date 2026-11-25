"""Option quote snapshots at entry and at modeled exits. Logging only."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import main_bot


FAKE_KEY = "TESTACCTKEY123"
ET = ZoneInfo("America/New_York")


@pytest.fixture(autouse=True)
def _isolate_snapshots():
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    main_bot._OPTION_SNAPSHOT_SEEN.clear()
    main_bot._OPTION_SNAPSHOT_STOP_TOUCHED.clear()
    yield
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    main_bot._OPTION_SNAPSHOT_SEEN.clear()
    main_bot._OPTION_SNAPSHOT_STOP_TOUCHED.clear()


def _run(coro):
    return asyncio.run(coro)


def _signal(signal_id="snap-1"):
    return {
        "signal_id": signal_id,
        "ticker": "SPY",
        "action": "BUY",
        "instrument": "option",
        "option_right": "CALL",
        "strike_hint": 600,
        "expiration_hint": "2026-12-18",
        "mode": "paper",
        "timestamp": "2026-11-25T19:00:00+00:00",
    }


def _at(hour, minute, day=25):
    return datetime(2026, 11, day, hour, minute, tzinfo=ET)


def test_schedule_keeps_15_30_60_before_the_flatten():
    planned = main_bot.plan_option_snapshot_schedule(_at(14, 0))
    assert [item[0] for item in planned] == [15, 30, 60]
    assert [(item[1].hour, item[1].minute) for item in planned] == [
        (14, 15), (14, 30), (15, 0),
    ]


def test_schedule_uses_the_flatten_when_it_comes_earlier():
    planned = main_bot.plan_option_snapshot_schedule(_at(15, 0))
    assert [item[0] for item in planned] == [15, 30, 40]
    assert (planned[2][1].hour, planned[2][1].minute) == (15, 40)

    late = main_bot.plan_option_snapshot_schedule(_at(15, 30))
    assert [item[0] for item in late] == [10]
    assert (late[0][1].hour, late[0][1].minute) == (15, 40)

    half = main_bot.plan_option_snapshot_schedule(_at(12, 0, day=27))
    assert [item[0] for item in half] == [15, 30, 40]
    assert (half[2][1].hour, half[2][1].minute) == (12, 40)

    after = main_bot.plan_option_snapshot_schedule(_at(15, 50))
    assert after == []


def test_note_dedupes_and_caps_pending(monkeypatch):
    monkeypatch.setenv("OPTION_QUOTE_SNAPSHOT_MAX_PENDING", "2")
    when = _at(14, 0)

    async def scenario():
        await main_bot.note_option_quote_snapshots(_signal("one"), now=when)
        await main_bot.note_option_quote_snapshots(_signal("one"), now=when)
        await main_bot.note_option_quote_snapshots(_signal("two"), now=when)

    _run(scenario())
    keys = sorted(main_bot._OPTION_SNAPSHOT_PENDING)
    assert len(keys) == 2
    assert len({key[0] for key in keys}) == 1
    assert [key[1] for key in keys] == [0, 15]


def test_missing_quote_still_writes_a_ledger_row(tmp_path, monkeypatch):
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })

    async def boom(*args, **kwargs):
        raise RuntimeError(
            "500 https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders.json"
            "?accountIdKey=TESTACCTKEY123 "
            'Authorization: OAuth oauth_token="sekret1234"'
        )

    monkeypatch.setattr(main_bot, "_etrade_call", boom)
    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(path)
    when = _at(14, 0)
    try:
        async def scenario():
            await main_bot.note_option_quote_snapshots(_signal(), now=when)
            wrote = await main_bot._option_snapshot_tick(when)
            assert wrote == 1

        _run(scenario())
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror

    assert len(rows) == 1
    data = rows[0]["data"]
    assert rows[0]["event"] == "option_quote_snapshot"
    assert data["event_type"] == "option_quote_snapshot"
    assert data["signal_key"].startswith("sig:")
    assert data["offset_min"] == 0
    assert data["status"] == "missing"
    assert data["quote"]["status"] == "missing"
    assert data["quote"]["bid"] is None
    assert data["quote"]["ask"] is None
    assert data["quote"]["last"] is None
    assert "option_symbol" in data["quote"]
    blob = path.read_text(encoding="utf-8")
    assert FAKE_KEY not in blob
    assert "sekret1234" not in blob


def test_scheduler_error_is_a_warning_and_does_not_raise(monkeypatch, caplog):
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    async def explode(*args, **kwargs):
        raise RuntimeError("scheduler blew TESTACCTKEY123")

    monkeypatch.setattr(main_bot, "_write_option_snapshot_row", explode)
    when = _at(14, 0)

    async def scenario():
        await main_bot.note_option_quote_snapshots(_signal(), now=when)
        wrote = await main_bot._option_snapshot_tick(when)
        assert wrote == 0
        await main_bot.note_option_quote_snapshots(_signal("other"), now=when)

    def broken_plan(_signal_at):
        raise RuntimeError("plan blew TESTACCTKEY123")

    _run(scenario())
    monkeypatch.setattr(main_bot, "plan_option_snapshot_schedule", broken_plan)

    async def schedule_fails():
        await main_bot.note_option_quote_snapshots(_signal("third"), now=when)

    _run(schedule_fails())
    warnings = [rec for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert warnings
    assert any("option quote snapshot" in rec.getMessage() for rec in warnings)
    assert FAKE_KEY not in caplog.text


def test_snapshots_do_not_touch_orders_positions_or_stops(tmp_path, monkeypatch):
    calls = []

    async def touched(*args, **kwargs):
        calls.append("order")

    monkeypatch.setattr(main_bot, "execute_live_order", touched)
    monkeypatch.setattr(main_bot, "_place_order_smart", touched)
    monkeypatch.setattr(main_bot, "_place_order_once", touched)
    monkeypatch.setattr(main_bot, "_cancel_order_call", touched)
    monkeypatch.setattr(main_bot, "_place_protective_stop", touched)
    monkeypatch.setattr(main_bot, "_snap_option_contract", touched)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)

    class _Market:
        def get_quote(self, symbols, resp_format="json"):
            return {"QuoteResponse": {"QuoteData": [{
                "dateTime": "15:30:00 EST 11-25-2026",
                "quoteStatus": "REALTIME",
                "Product": {"symbol": "SPY"},
                "All": {
                    "bid": 1.2,
                    "ask": 1.35,
                    "lastTrade": 1.28,
                    "quoteDetail": (
                        "https://api.etrade.com/v1/accounts/TESTACCTKEY123/x"
                        "?accountIdKey=TESTACCTKEY123"
                    ),
                },
            }]}}

    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", lambda *a, **k: _Market())
    saved_ledger = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    saved_state = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    main_bot.trade_ledger.set_mirror(None)
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.init(path)
    marks = dict(main_bot._LAST_MARK_QUALITY)
    when = _at(14, 0)
    try:
        async def scenario():
            payload = _signal()
            before = dict(payload)
            await main_bot.note_option_quote_snapshots(payload, now=when)
            assert payload == before
            wrote = await main_bot._option_snapshot_tick(when + main_bot.timedelta(minutes=15))
            assert wrote == 2
            positions = await main_bot.state.all_positions()
            assert positions == {}

        _run(scenario())
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved_ledger
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot.state = saved_state

    assert calls == []
    assert main_bot._LAST_MARK_QUALITY == marks
    assert [row["data"]["offset_min"] for row in rows] == [0, 15]
    quote = rows[0]["data"]["quote"]
    assert quote["status"] == "ok"
    assert quote["bid"] == 1.2
    assert quote["ask"] == 1.35
    assert quote["last"] == 1.28
    assert quote["dateTime"] == "15:30:00 EST 11-25-2026"
    assert quote["option_symbol"] == "SPY:2026:12:18:CALL:600"
    assert FAKE_KEY not in path.read_text(encoding="utf-8")


def test_entry_hook_notes_a_refused_entry_and_skips_a_close(monkeypatch):
    seen = []
    orders = []

    async def grab(payload, now=None):
        seen.append(payload.get("signal_id"))

    async def execute(payload):
        orders.append(payload.get("signal_id"))
        return {"status": "skipped", "reason": "test"}

    monkeypatch.setattr(main_bot, "note_option_quote_snapshots", grab)
    monkeypatch.setattr(main_bot, "execute_live_order", execute)
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "load_tokens", lambda: None)

    async def record(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    try:
        async def scenario():
            refused = await main_bot.process_signal({
                "ticker": "SPY",
                "action": "BUY",
                "instrument": "option",
                "mode": "live",
            })
            closed = await main_bot.process_signal({
                "ticker": "SPY",
                "action": "BUY",
                "intent": "close",
                "instrument": "option",
                "mode": "paper",
                "signal_id": "close-1",
            })
            return refused, closed

        refused, closed = _run(scenario())
    finally:
        main_bot.state = saved

    assert refused["status"] == "rejected"
    assert refused["reason"] == "live entry requires signal_id"
    assert seen == [None]
    assert closed["status"] == "processed_directly"
    assert "close-1" not in seen
    assert orders == ["close-1"]


def _option_pos(order_id="ORD44", stop=1.0):
    return {
        "entry": 2.0,
        "entry_premium": 2.0,
        "qty": 0,
        "filled_qty": 0,
        "action": "BUY",
        "ts": "2026-11-25T15:00:00+00:00",
        "contract": {"right": "CALL", "expiration": "2026-12-18", "strike": 600},
        "entry_signal": {"signal_id": "exit-1", "ticker": "SPY", "action": "BUY"},
        "stop_order_id": order_id,
        "stop_premium": stop,
    }


def test_exit_fill_and_stop_trigger_enqueue_the_order_id(monkeypatch):
    when = _at(14, 0)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when)
    pos = _option_pos()
    main_bot.enqueue_exit_option_snapshot(
        "SPY", pos, {"ticker": "SPY", "order_id": "ORD44"}, "close_fill", False,
    )
    main_bot.enqueue_exit_option_snapshot(
        "SPY", pos, {"ticker": "SPY", "order_id": "ORD44"}, "close_fill", False,
    )
    main_bot.enqueue_exit_option_snapshot(
        "SPY", _option_pos("STOP7"), {"ticker": "SPY"}, "stop_fill", False,
    )
    jobs = list(main_bot._OPTION_SNAPSHOT_PENDING.values())
    kinds = sorted(job["offset_kind"] for job in jobs)
    assert kinds == ["exit_fill", "stop_trigger"]
    by_kind = {job["offset_kind"]: job for job in jobs}
    assert by_kind["exit_fill"]["order_id"] == "ORD44"
    assert by_kind["stop_trigger"]["order_id"] == "STOP7"
    assert all(job["preset_quote"] is None for job in jobs)


def _queue_one_stop_touch(monkeypatch):
    fetched = {"n": 0}

    async def refuse_read(spec):
        fetched["n"] += 1
        raise AssertionError("stop touch must not fetch another quote")

    monkeypatch.setattr(main_bot, "_read_option_snapshot_quote", refuse_read)
    pos = _option_pos(stop=1.0)
    sym = "SPY:2026:12:18:CALL:600"
    main_bot._LAST_QUOTE_STAMP[sym] = {
        "dateTime": "15:30:00 EST 11-25-2026",
        "dateTimeUTC": None,
        "last": 0.95,
        "fetched_at": "2026-11-25T20:30:00+00:00",
    }
    main_bot.note_polled_stop_touch("SPY", pos, 0.90, 1.10, 1.20)
    assert main_bot._OPTION_SNAPSHOT_PENDING == {}
    main_bot.note_polled_stop_touch("SPY", pos, 0.90, 1.10, 0.90)
    main_bot.note_polled_stop_touch("SPY", pos, 0.80, 1.00, 0.80)
    assert len(main_bot._OPTION_SNAPSHOT_PENDING) == 1
    job = next(iter(main_bot._OPTION_SNAPSHOT_PENDING.values()))
    assert job["offset_kind"] == "stop_touch"
    assert job["order_id"] == "ORD44"
    assert job["preset_quote"]["bid"] == 0.90
    assert job["preset_quote"]["ask"] == 1.10

    async def write():
        return await main_bot._option_snapshot_tick(
            datetime(2099, 1, 1, tzinfo=main_bot.timezone.utc)
        )

    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    # ledger is set by the caller via tmp? keep it inside the function below
    return fetched, write, saved, saved_mirror


def test_stop_touch_row_ages_the_broker_quote(tmp_path, monkeypatch):
    fetched, write, saved, saved_mirror = _queue_one_stop_touch(monkeypatch)
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(path)
    try:
        wrote = _run(write())
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._LAST_QUOTE_STAMP.clear()
    assert fetched["n"] == 0
    assert wrote == 1
    data = rows[0]["data"]
    assert data["offset_kind"] == "stop_touch"
    assert data["order_id"] == "ORD44"
    assert data["quote"]["bid"] == 0.90
    assert data["quote"]["last"] == 0.95
    assert data["quote_time_source"] == "broker_quote"
    # 15:30 EST is 20:30 UTC, the same instant as fetched_at.
    assert data["quote_age_ms"] == 0
    assert "dateTimeUTC" not in data["quote"]


def test_exit_path_does_not_wait_on_the_snapshot(monkeypatch):
    calls = {"read": 0, "place": 0}

    async def slow_read(spec):
        calls["read"] += 1
        await asyncio.sleep(5)
        return {}

    async def place(*args, **kwargs):
        calls["place"] += 1
        raise AssertionError("exit snapshot must not place")

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "_read_option_snapshot_quote", slow_read)
    monkeypatch.setattr(main_bot, "execute_live_order", place)
    monkeypatch.setattr(main_bot, "_place_order_smart", place)
    monkeypatch.setattr(main_bot, "_cancel_order_call", place)
    monkeypatch.setattr(main_bot, "release_closed_heat", noop)
    monkeypatch.setattr(main_bot, "_schedule_exit_followups", noop)
    monkeypatch.setattr(main_bot, "_announce_learning_transitions", noop)
    monkeypatch.setattr(main_bot, "_remember_trade_review", noop)
    monkeypatch.setattr(main_bot, "_alert_fill", noop)

    async def record(*args, **kwargs):
        return 1

    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)

    async def scenario():
        await main_bot.state.set_position("SPY", _option_pos())
        started = main_bot.time.perf_counter()
        await main_bot._record_close(
            "SPY", None, {"ticker": "SPY", "order_id": "ORD44"},
            exit_premium=1.5, close_reason="close_fill",
        )
        return main_bot.time.perf_counter() - started

    try:
        elapsed = _run(scenario())
    finally:
        main_bot.state = saved
    assert elapsed < 0.5
    assert calls == {"read": 0, "place": 0}
    kinds = [job["offset_kind"] for job in main_bot._OPTION_SNAPSHOT_PENDING.values()]
    assert kinds == ["exit_fill"]


def test_exit_enqueue_error_is_only_a_warning(monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    def explode(*args, **kwargs):
        raise RuntimeError("enqueue failed TESTACCTKEY123")

    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)
    monkeypatch.setattr(main_bot, "enqueue_exit_option_snapshot", explode)
    monkeypatch.setattr(main_bot, "release_closed_heat", lambda *a, **k: _async_none())
    monkeypatch.setattr(main_bot, "_schedule_exit_followups", lambda *a, **k: _async_none())
    monkeypatch.setattr(main_bot, "_announce_learning_transitions", lambda *a, **k: _async_none())
    monkeypatch.setattr(main_bot, "_remember_trade_review", lambda *a, **k: _async_none())
    monkeypatch.setattr(main_bot, "_alert_fill", lambda *a, **k: _async_none())

    async def record(*args, **kwargs):
        return 1

    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)

    async def scenario():
        await main_bot.state.set_position("SPY", _option_pos())
        await main_bot._record_close(
            "SPY", None, {"ticker": "SPY"}, exit_premium=1.5, close_reason="stop_fill",
        )

    try:
        _run(scenario())
    finally:
        main_bot.state = saved
    assert any("option quote snapshot enqueue failed" in rec.getMessage() for rec in caplog.records)
    assert FAKE_KEY not in caplog.text


def _async_none():
    async def _inner(*args, **kwargs):
        return None
    return _inner()


def test_exit_fill_uses_the_polled_execution_clock(tmp_path, monkeypatch):
    """fill_time comes from the order poll already in hand. No second order read."""
    calls = {"orders": 0}

    async def orders():
        calls["orders"] += 1
        raise AssertionError("exit snapshot must not poll orders again")

    async def quote(spec):
        return {
            "status": "ok", "bid": 1.2, "ask": 1.3, "last": 1.25,
            "dateTime": "15:30:00 EST 11-25-2026",
            "option_symbol": (spec or {}).get("option_symbol"),
            "_quote_time_source": "broker_quote",
            "_fetched_at": "2026-11-25T20:30:00+00:00",
        }

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", orders)
    monkeypatch.setattr(main_bot, "_read_option_snapshot_quote", quote)
    when = datetime(2026, 11, 25, 15, 30, tzinfo=ET)
    pos = _option_pos()
    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(path)
    try:
        main_bot.enqueue_exit_option_snapshot(
            "SPY", pos, {"ticker": "SPY", "executed_ts": when.timestamp()},
            "close_fill", False,
        )
        bare = _option_pos("ORD45")
        main_bot.enqueue_exit_option_snapshot(
            "SPY", bare, {"ticker": "SPY", "signal_id": "exit-2", "order_id": "ORD45"},
            "close_fill", False,
        )

        async def write():
            return await main_bot._option_snapshot_tick(datetime(2099, 1, 1, tzinfo=timezone.utc))

        assert _run(write()) == 2
        rows = [json.loads(line)["data"] for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
    assert calls["orders"] == 0
    by_order = {row["order_id"]: row for row in rows}
    assert by_order["ORD44"]["fill_time"] == when.isoformat()
    assert by_order["ORD44"]["fill_time_source"] == "broker_executed"
    assert by_order["ORD45"]["fill_time"] is None
    assert by_order["ORD45"]["fill_time_source"] == "local_record"
    assert main_bot.broker_fill_time_et(when.timestamp() * 1000) == when.isoformat()


def test_timed_out_snapshot_append_keeps_the_hash_chain(tmp_path, monkeypatch):
    """A cancelled waiter does not cut the append. The next record chains after it."""
    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(path)
    original = main_bot.trade_ledger.TradeLedger._append_line
    started = threading.Event()

    def slow(self, line):
        started.set()
        time.sleep(0.3)
        return original(self, line)

    monkeypatch.setattr(main_bot.trade_ledger.TradeLedger, "_append_line", slow)
    main_bot._LEDGER_WRITTEN = 0
    main_bot._LEDGER_DROPPED = 0

    async def scenario():
        snap = asyncio.create_task(main_bot.trade_ledger.record(
            "option_quote_snapshot", {"option_symbol": OCC, "order_id": ORDER_ID},
        ))
        await asyncio.to_thread(started.wait, 2)
        snap.cancel()
        with pytest.raises(asyncio.CancelledError):
            await snap
        await main_bot.trade_ledger.record("position_opened", {"ticker": "INTC"})
        ok, count, err = main_bot.trade_ledger.verify()
        assert ok, err
        assert count == 2
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert rows[1]["prev_hash"] == rows[0]["hash"]
        assert rows[0]["event"] == "option_quote_snapshot"
        counts = main_bot.ledger_writer_counts()
        assert counts == {"written": 2, "dropped": 0}

    try:
        _run(scenario())
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror


OCC = "INTC  261127C12345678"
ORDER_ID = "991234567800"
SIGNAL_KEY = "sig:abc12345678def"


def test_identity_fields_survive_an_account_id_substring(tmp_path, monkeypatch):
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", "12345678")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "0")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "true")
    main_bot._QUOTE_LOG_MEMORY.clear()
    main_bot._QUOTE_LOG_SEEN.clear()
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    payload = {
        "QuoteResponse": {"QuoteData": [{
            "dateTime": "15:30:00 EST 11-25-2026",
            "quoteStatus": "REALTIME",
            "Product": {"symbol": "INTC", "securityType": "OPTN"},
            "All": {"bid": 1.2, "ask": 1.3, "lastTrade": 1.25, "open": 1.1, "previousClose": 1.0},
        }]},
    }

    async def fetch_and_read():
        await main_bot._etrade_call(
            lambda symbols, resp_format="json": payload,
            [OCC], resp_format="json", source="quote", quote_log_as="/etrade/quote",
        )
        return await main_bot.etrade_quotes_log(
            symbol=OCC, since=None, limit=10, x_rork_secret="s3cret",
        )

    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    main_bot._QUOTE_LOG_READS.clear()
    listed = _run(fetch_and_read())
    assert listed["quotes"][0]["option_symbol"] == OCC

    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    path = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(path)
    preset = {
        "status": "ok", "bid": 1.2, "ask": 1.3, "last": 1.25,
        "dateTime": "15:30:00 EST 11-25-2026",
        "option_symbol": OCC,
        "_quote_time_source": "broker_quote",
        "_fetched_at": "2026-11-25T20:30:00+00:00",
    }
    try:
        main_bot._OPTION_SNAPSHOT_PENDING.clear()
        main_bot._enqueue_option_snapshot(
            SIGNAL_KEY, None, datetime(2026, 11, 25, tzinfo=timezone.utc),
            {"ticker": "INTC", "option_symbol": OCC},
            offset_kind="exit_fill", order_id=ORDER_ID, preset_quote=preset,
            fill_time="2026-11-25T15:30:00-05:00", fill_time_source="broker_executed",
        )

        async def write():
            return await main_bot._option_snapshot_tick(datetime(2099, 1, 1, tzinfo=timezone.utc))

        assert _run(write()) == 1
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)["data"]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror

    assert data["quote"]["option_symbol"] == OCC
    assert data["order_id"] == ORDER_ID
    assert data["signal_key"] == SIGNAL_KEY
    assert data["fill_time"] == "2026-11-25T15:30:00-05:00"
    assert "12345678" in data["quote"]["option_symbol"]
    assert "12345678" in data["order_id"]
    assert "12345678" in data["signal_key"]


def test_quote_age_uses_the_documented_quote_time():
    # E*TRADE documents dateTime as "the date and time of the quote", not the HTTP response.
    assert main_bot._quote_age_ms(
        "15:30:00 EST 11-25-2026", "2026-11-25T20:30:01+00:00",
    ) == 1000
    assert main_bot._quote_age_ms(None, "2026-11-25T20:30:00+00:00") is None

