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
    main_bot._SNAP_FETCHES.clear()
    main_bot._snap_shutdown = False
    main_bot._ORDER_PATH_INFLIGHT = 0
    main_bot.set_quote_snapshot_mirror(None)
    main_bot._snap_durable = 0
    main_bot._snap_durable_base = 0
    main_bot._snap_known_mirrored.clear()
    main_bot._snap_mirror_failures = 0
    main_bot._snap_restore_failed = False
    main_bot._snap_restore_attempts = 0
    main_bot._snap_restore_settled = False
    main_bot._snap_restore_loader = None
    yield
    main_bot._snap_shutdown = True
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    main_bot._OPTION_SNAPSHOT_SEEN.clear()
    main_bot._OPTION_SNAPSHOT_STOP_TOUCHED.clear()
    main_bot._SNAP_FETCHES.clear()


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
    saved_snap = main_bot._snap_log
    ledger = Path(tmp_path) / "ledger.jsonl"
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(ledger)
    main_bot.init_quote_snapshot_log(path)
    when = _at(14, 0)
    try:
        async def scenario():
            await main_bot.note_option_quote_snapshots(_signal(), now=when)
            wrote = await main_bot._option_snapshot_tick(when + main_bot.timedelta(seconds=3))
            assert wrote == 1

        _run(scenario())
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap

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
        wrote = await main_bot._option_snapshot_tick(when + main_bot.timedelta(seconds=3))
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
    saved_snap = main_bot._snap_log
    saved_state = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    main_bot.trade_ledger.set_mirror(None)
    ledger = Path(tmp_path) / "ledger.jsonl"
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.trade_ledger.init(ledger)
    main_bot.init_quote_snapshot_log(path)
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
        main_bot._snap_log = saved_snap
        main_bot.state = saved_state

    assert calls == []
    assert not ledger.exists() or "option_quote_snapshot" not in ledger.read_text(encoding="utf-8")
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
    saved_snap = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
    try:
        wrote = _run(write())
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap
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
    saved_snap = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
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
        main_bot._snap_log = saved_snap
    assert calls["orders"] == 0
    by_order = {row["order_id"]: row for row in rows}
    assert by_order["ORD44"]["fill_time"] == when.isoformat()
    assert by_order["ORD44"]["fill_time_source"] == "broker_executed"
    assert by_order["ORD45"]["fill_time"] is None
    assert by_order["ORD45"]["fill_time_source"] == "local_record"
    assert main_bot.broker_fill_time_et(when.timestamp() * 1000) == when.isoformat()


def test_timed_out_snapshot_append_keeps_the_hash_chain(tmp_path, monkeypatch):
    """A cancelled waiter does not cut the side-log append. The next snapshot chains after it."""
    saved = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    saved_snap = main_bot._snap_log
    side = Path(tmp_path) / "quote_snapshots.jsonl"
    ledger = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(ledger)
    main_bot.init_quote_snapshot_log(side)
    original = main_bot._snap_log.append
    started = threading.Event()

    def slow(data):
        started.set()
        time.sleep(0.3)
        return original(data)

    monkeypatch.setattr(main_bot._snap_log, "append", slow)
    main_bot._snap_written = 0
    main_bot._snap_dropped = 0

    async def scenario():
        snap = asyncio.create_task(main_bot.submit_quote_snapshot(
            {"option_symbol": OCC, "order_id": ORDER_ID},
        ))
        await asyncio.to_thread(started.wait, 2)
        snap.cancel()
        with pytest.raises(asyncio.CancelledError):
            await snap
        await main_bot.submit_quote_snapshot({"option_symbol": "QQQ:2099-01-15:CALL:500"})
        ok, count, err = main_bot._snap_log.verify()
        assert ok, err
        assert count == 2
        rows = [json.loads(line) for line in side.read_text(encoding="utf-8").splitlines()]
        assert rows[1]["prev_hash"] == rows[0]["hash"]
        assert rows[0]["event"] == "option_quote_snapshot"
        assert main_bot.snapshot_writer_counts() == {"written": 2, "dropped": 0}
        await main_bot.trade_ledger.record("position_opened", {"ticker": "INTC"})
        trade_ok, trade_count, trade_err = main_bot.trade_ledger.verify()
        assert trade_ok, trade_err
        assert trade_count == 1
        assert "option_quote_snapshot" not in ledger.read_text(encoding="utf-8")

    try:
        _run(scenario())
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap


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
    saved_snap = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
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
        main_bot._QUOTE_LOG_READS.clear()
        listed = _run(main_bot.etrade_quotes_log(
            symbol=OCC, since=None, limit=10, kind="snapshots", x_rork_secret="s3cret",
        ))
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap

    assert data["quote"]["option_symbol"] == OCC
    assert listed["snapshots"][0]["data"]["quote"]["option_symbol"] == OCC
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


# Module-level trade_ledger.record on main. CI checks out the PR without origin/main.
_MAIN_LEDGER_RECORD = '''async def record(event: str, data: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """Append an event. Never raises — a ledger failure must not block trading.

    Returns the appended record's seq, or None when the append did not land.
    Callers that ignore the return value are unchanged.
    """
    try:
        if _ledger is None:
            logger.warning(f"ledger not initialised — dropped event '{event}'")
            return None
        rec = await _ledger.record(event, data)
        seq = int(rec["seq"])
        if _mirror is not None:
            try:
                await _mirror(seq, str(rec["hash"]), _canonical(rec))
            except Exception as e:
                logger.warning(f"ledger mirror write failed (non-fatal, {event}): {e}")
        return seq
    except Exception as e:
        logger.error(f"ledger append failed ({event}): {e}")
        return None
'''


def _module_record_source(text: str) -> str:
    marker = "async def record(event: str, data: Optional[Dict[str, Any]] = None) -> Optional[int]:"
    start = text.index(marker)
    lines = text[start:].splitlines(keepends=True)
    out = [lines[0]]
    for line in lines[1:]:
        if line.startswith(("async def ", "def ", "class ")):
            break
        out.append(line)
    return "".join(out).rstrip() + "\n"


def test_core_ledger_record_is_the_direct_path():
    current = Path(main_bot.__file__).read_text(encoding="utf-8")
    here = _module_record_source(current)
    assert here == _MAIN_LEDGER_RECORD
    assert "await _ledger.record(event, data)" in here
    assert "Queue" not in here
    assert not hasattr(main_bot.trade_ledger, "_direct_record")
    assert not hasattr(main_bot.trade_ledger, "writer_counts")


def test_snapshot_budget_defers_instead_of_fetching(tmp_path, monkeypatch):
    monkeypatch.setenv("ETRADE_QUOTE_SNAPSHOT_MAX_PER_MIN", "1")
    calls = {"n": 0}

    async def quote(spec):
        calls["n"] += 1
        return {
            "status": "missing", "bid": None, "ask": None, "last": None,
            "dateTime": None, "option_symbol": (spec or {}).get("option_symbol"),
        }

    monkeypatch.setattr(main_bot, "_read_option_snapshot_quote", quote)
    saved = main_bot._snap_log
    main_bot.init_quote_snapshot_log(Path(tmp_path) / "quote_snapshots.jsonl")
    when = _at(14, 0)
    try:
        async def scenario():
            main_bot._SNAP_FETCHES.clear()
            main_bot._enqueue_option_snapshot(
                "sig-a", 0, when, {"option_symbol": "SPY:1"}, offset_kind="entry",
            )
            main_bot._enqueue_option_snapshot(
                "sig-b", 0, when, {"option_symbol": "QQQ:1"}, offset_kind="entry",
            )
            return await main_bot._option_snapshot_tick(when)

        wrote = _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert calls["n"] == 1
    assert wrote == 2
    assert len(main_bot._OPTION_SNAPSHOT_PENDING) == 1


def test_shutdown_flushes_the_snapshot_writer(tmp_path):
    saved = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
    try:
        async def scenario():
            for n in range(3):
                main_bot._put_snapshot({"n": n}, None)
            await main_bot.flush_quote_snapshot_writer(2.0)
            assert main_bot._snap_task.done()
            return main_bot._snap_log.verify()

        ok, count, err = _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert ok, err
    assert count == 3
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [row["data"]["n"] for row in rows] == [0, 1, 2]


def test_snapshot_writer_restarts_after_it_dies(tmp_path, monkeypatch):
    sent = []

    async def fake_send(*args, **kwargs):
        sent.append(args[1] if len(args) > 1 else kwargs.get("event"))

    monkeypatch.setattr(main_bot.alerts, "send", fake_send)
    saved = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
    try:
        async def scenario():
            await main_bot.submit_quote_snapshot({"n": 0})
            old = main_bot._snap_task
            old.remove_done_callback(main_bot._on_snap_writer_done)
            old.cancel()
            with pytest.raises(asyncio.CancelledError):
                await old
            async def boom():
                raise RuntimeError("writer died")
            dead = asyncio.create_task(boom())
            main_bot._snap_task = dead
            dead.add_done_callback(main_bot._on_snap_writer_done)
            await asyncio.sleep(0.1)
            assert main_bot._snap_task is not dead
            assert not main_bot._snap_task.done()
            await main_bot.submit_quote_snapshot({"n": 1})
            return main_bot._snap_log.verify()

        ok, count, err = _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert "quote_snapshot_writer_died" in sent
    assert ok, err
    assert count == 2


def test_full_snapshot_queue_drops_snapshots_only(tmp_path, monkeypatch):
    monkeypatch.setenv("QUOTE_SNAPSHOT_QUEUE_MAX", "1")
    saved_ledger = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    saved_snap = main_bot._snap_log
    side = Path(tmp_path) / "quote_snapshots.jsonl"
    ledger = Path(tmp_path) / "ledger.jsonl"
    main_bot.trade_ledger.set_mirror(None)
    main_bot.trade_ledger.init(ledger)
    main_bot.init_quote_snapshot_log(side)
    original = main_bot._snap_log.append
    entered = threading.Event()
    gate = threading.Event()

    def blocked(data):
        entered.set()
        assert gate.wait(5)
        return original(data)

    monkeypatch.setattr(main_bot._snap_log, "append", blocked)
    main_bot._snap_dropped = 0
    try:
        async def scenario():
            first = asyncio.create_task(main_bot.submit_quote_snapshot({"n": 1}))
            assert await asyncio.to_thread(entered.wait, 2)
            second = asyncio.create_task(main_bot.submit_quote_snapshot({"n": 2}))
            await asyncio.sleep(0.05)
            third = await main_bot.submit_quote_snapshot({"n": 3})
            assert third is None
            assert main_bot.snapshot_writer_counts()["dropped"] >= 1
            seq = await main_bot.trade_ledger.record(
                "position_closed", {"ticker": "INTC", "reason": "stop"},
            )
            assert seq == 1
            trade_ok, trade_count, trade_err = main_bot.trade_ledger.verify()
            assert trade_ok, trade_err
            assert trade_count == 1
            gate.set()
            await first
            await second
            return main_bot._snap_log.verify()

        ok, count, err = _run(scenario())
    finally:
        gate.set()
        main_bot.trade_ledger._ledger = saved_ledger
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap
    text = ledger.read_text(encoding="utf-8")
    assert "position_closed" in text
    assert "option_quote_snapshot" not in text
    assert ok, err
    assert count == 2


def test_writer_death_alert_scrubs_url_token_and_account_key(tmp_path, monkeypatch, caplog):
    token = "sekret1234"
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)
    monkeypatch.setattr(main_bot, "_current_tokens", {"oauth_token": token, "oauth_token_secret": "x"})
    sent = []

    async def fake_send(*args, **kwargs):
        sent.append(args[2] if len(args) > 2 else kwargs.get("message"))

    monkeypatch.setattr(main_bot.alerts, "send", fake_send)
    caplog.set_level(logging.ERROR, logger="etrade-bot")
    saved = main_bot._snap_log
    main_bot.init_quote_snapshot_log(Path(tmp_path) / "quote_snapshots.jsonl")

    class Probe(RuntimeError):
        def __str__(self):
            return (
                "https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders.json"
                "?accountIdKey=TESTACCTKEY123 "
                'oauth_token="sekret1234"'
            )

    try:
        async def scenario():
            await main_bot.submit_quote_snapshot({"n": 0})
            old = main_bot._snap_task
            old.remove_done_callback(main_bot._on_snap_writer_done)
            old.cancel()
            with pytest.raises(asyncio.CancelledError):
                await old

            async def boom():
                raise Probe()

            dead = asyncio.create_task(boom())
            main_bot._snap_task = dead
            dead.add_done_callback(main_bot._on_snap_writer_done)
            await asyncio.sleep(0.1)

        _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert sent
    blob = "\n".join(sent) + "\n" + caplog.text
    assert "Probe" in blob
    assert FAKE_KEY not in blob
    assert token not in blob
    assert "https://api.etrade.com" not in blob
    assert "accountIdKey=TESTACCTKEY123" not in blob


def test_restart_reuses_the_queue_and_orphans_are_dropped(tmp_path):
    saved = main_bot._snap_log
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    main_bot.init_quote_snapshot_log(path)
    try:
        async def scenario():
            await main_bot.submit_quote_snapshot({"n": 0})
            queue = main_bot._snap_q
            old = main_bot._snap_task
            old.remove_done_callback(main_bot._on_snap_writer_done)
            old.cancel()
            with pytest.raises(asyncio.CancelledError):
                await old
            fut = asyncio.get_running_loop().create_future()
            queue.put_nowait(({"n": 1}, fut))
            assert main_bot._ensure_snapshot_writer() is queue
            seq = await asyncio.wait_for(fut, 2)
            assert seq is not None
            orphan = asyncio.Queue()
            orphan_fut = asyncio.get_running_loop().create_future()
            orphan.put_nowait(({"n": "gone"}, orphan_fut))
            before = main_bot.snapshot_writer_counts()["dropped"]
            main_bot._abandon_snapshot_queue(orphan)
            assert orphan_fut.result() is None
            assert main_bot.snapshot_writer_counts()["dropped"] == before + 1
            return main_bot._snap_log.verify()

        ok, count, err = _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert ok, err
    assert count == 2
    rows = [json.loads(line)["data"]["n"] for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [0, 1]


def test_corrupt_tail_keeps_the_last_valid_link(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    secret = "TAILTOKEN99"
    log = main_bot.QuoteSnapshotLog(path)
    log.append({"n": 1})
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            "not-json "
            + secret
            + " https://api.etrade.com/v1/accounts/TESTACCTKEY123/x?accountIdKey=TESTACCTKEY123\n"
        )
    reloaded = main_bot.QuoteSnapshotLog(path)
    assert reloaded._seq == 1
    assert reloaded.corrupt_counts.get("JSONDecodeError") == 1
    reloaded.append({"n": 2})
    ok, count, err = reloaded.verify()
    assert ok, err
    assert count == 2
    assert secret not in caplog.text
    assert "TESTACCTKEY123" not in caplog.text
    assert "JSONDecodeError" in caplog.text
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.startswith("{")]
    assert rows[1]["seq"] == 2
    assert rows[1]["prev_hash"] == rows[0]["hash"]


def test_snapshot_fetch_defers_while_an_order_is_in_flight(monkeypatch):
    release = threading.Event()
    entered = threading.Event()
    fetched = []

    def place_fn(*_args, **_kwargs):
        entered.set()
        assert release.wait(3)
        return {"ok": True}

    original = main_bot._etrade_call

    async def wrapped(fn, *args, source="etrade", **kwargs):
        if source == "option_snapshot":
            fetched.append(source)
        return await original(fn, *args, source=source, **kwargs)

    monkeypatch.setattr(main_bot, "_etrade_call", wrapped)
    when = _at(14, 0)

    async def scenario():
        place = asyncio.create_task(original(
            place_fn, source="place_equity", idempotent=False,
        ))
        assert await asyncio.to_thread(entered.wait, 2)
        assert main_bot._order_path_in_flight()
        main_bot._enqueue_option_snapshot(
            "sig-defer", 0, when, {"option_symbol": "SPY:1"}, offset_kind="entry",
        )
        wrote = await main_bot._option_snapshot_tick(when)
        release.set()
        await place
        return wrote

    try:
        wrote = _run(scenario())
    finally:
        release.set()
    assert wrote == 1
    assert fetched == []
    job = next(iter(main_bot._OPTION_SNAPSHOT_PENDING.values()))
    assert job["deferred"] == "order_in_flight"


def test_stop_placement_does_not_wait_behind_a_snapshot_fetch():
    release = threading.Event()
    entered = threading.Event()

    def snap_fn(*_args, **_kwargs):
        entered.set()
        assert release.wait(3)
        return {}

    def place_fn(*_args, **_kwargs):
        return {"ok": True}

    async def scenario():
        snap = asyncio.create_task(main_bot._etrade_call(snap_fn, source="option_snapshot"))
        assert await asyncio.to_thread(entered.wait, 2)
        started = time.monotonic()
        await main_bot._etrade_call(place_fn, source="place_equity", idempotent=False)
        elapsed = time.monotonic() - started
        still_blocked = not release.is_set()
        release.set()
        await snap
        return elapsed, still_blocked

    try:
        elapsed, still_blocked = _run(scenario())
    finally:
        release.set()
    assert still_blocked
    assert elapsed < 0.5


def test_refused_realtime_build_signal_error_does_not_stop_the_scan(monkeypatch):
    quotes = [
        {"symbol": "AAA", "realtime": False, "quote_status": "DELAYED", "last": 10},
        {"symbol": "BBB", "realtime": True, "quote_status": "REALTIME", "last": 20},
    ]
    dispatched = []

    def evaluate(quote, _series, _cfg, _progress, _context):
        return {
            "ticker": quote["symbol"],
            "qualifies": True,
            "score": 1,
            "shadow_eligible": False,
            "book": {},
            "setup": "orb",
            "side": "long",
            "rvol": 2.0,
            "entry": 1.0,
            "stop": 0.8,
            "target": 1.4,
            "ts": "2026-11-25T19:00:00+00:00",
        }

    def build(quote, cand, mode):
        if quote["symbol"] == "AAA":
            raise RuntimeError("build blew")
        return {"ticker": cand["ticker"], "mode": mode}

    async def dispatch(payload):
        dispatched.append(payload["ticker"])
        return {"status": "sent"}

    async def fetch(_chunk):
        return {}

    monkeypatch.setattr(main_bot.scanner_mod, "parse_quotes", lambda _raw: quotes)
    monkeypatch.setattr(main_bot.scanner_mod, "evaluate_quote", evaluate)
    monkeypatch.setattr(main_bot.scanner_mod, "build_signal", build)
    scan = main_bot.scanner_mod.Scanner(
        main_bot.StateStore(None), fetch, dispatch, lambda: True, lambda: True,
        lambda: _at(14, 0),
    )
    scan._config["mode"] = "live"
    scan._config["universe"] = ["AAA", "BBB"]
    _run(scan.scan_once())
    assert dispatched == ["BBB"]


def test_stop_touch_set_is_bounded_and_cleared_on_close(monkeypatch):
    monkeypatch.setattr(main_bot, "_stop_touch_cap", lambda: 2)
    first = _option_pos("ORD1")
    second = _option_pos("ORD2")
    second["ts"] = "2026-11-25T15:01:00+00:00"
    third = _option_pos("ORD3")
    third["ts"] = "2026-11-25T15:02:00+00:00"
    main_bot.note_polled_stop_touch("SPY", first, 0.5, 0.6, 0.5)
    main_bot.note_polled_stop_touch("SPY", second, 0.5, 0.6, 0.5)
    main_bot.note_polled_stop_touch("SPY", third, 0.5, 0.6, 0.5)
    assert len(main_bot._OPTION_SNAPSHOT_STOP_TOUCHED) == 2
    assert all(not str(key).endswith("|ORD1") for key in main_bot._OPTION_SNAPSHOT_STOP_TOUCHED)
    main_bot.enqueue_exit_option_snapshot(
        "SPY", second, {"ticker": "SPY"}, "close_fill", False,
    )
    assert all(not str(key).endswith("|ORD2") for key in main_bot._OPTION_SNAPSHOT_STOP_TOUCHED)
    assert any(str(key).endswith("|ORD3") for key in main_bot._OPTION_SNAPSHOT_STOP_TOUCHED)


def test_fsync_failure_reloads_the_tail_and_appends_do_not_overlap(tmp_path, monkeypatch):
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    log = main_bot.QuoteSnapshotLog(path)
    real_fsync = main_bot.os.fsync
    calls = {"n": 0}

    def flaky(fd):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("fsync failed")
        return real_fsync(fd)

    monkeypatch.setattr(main_bot.os, "fsync", flaky)
    first = log.append({"n": 1})
    second = log.append({"n": 2})
    ok, count, err = log.verify()
    assert ok, err
    assert count == 2
    assert second["prev_hash"] == first["hash"]
    assert log._prev_hash == second["hash"]

    holding = threading.Event()
    release = threading.Event()

    def blocking(fd):
        holding.set()
        assert release.wait(3)
        return real_fsync(fd)

    monkeypatch.setattr(main_bot.os, "fsync", blocking)
    overlapped = {"second_started_write": False}

    def write_first():
        log.append({"n": 3})

    def write_second():
        holding.wait(3)
        overlapped["second_started_write"] = True
        log.append({"n": 4})

    left = threading.Thread(target=write_first)
    right = threading.Thread(target=write_second)
    left.start()
    assert holding.wait(2)
    right.start()
    time.sleep(0.05)
    assert right.is_alive()
    release.set()
    left.join(3)
    right.join(3)
    ok, count, err = log.verify()
    assert ok, err
    assert count == 4
    assert overlapped["second_started_write"] is True


def test_partial_tail_is_repaired_on_load(tmp_path):
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    log = main_bot.QuoteSnapshotLog(path)
    first = log.append({"n": 1})
    prefix = path.read_bytes()
    with path.open("ab") as handle:
        handle.write(b'{"seq": 2, "partial"')
    reloaded = main_bot.QuoteSnapshotLog(path)
    assert reloaded._seq == 1
    assert reloaded._prev_hash == first["hash"]
    assert path.read_bytes() == prefix
    second = reloaded.append({"n": 2})
    ok, count, err = reloaded.verify()
    assert ok, err
    assert count == 2
    assert second["prev_hash"] == first["hash"]


def test_side_log_mirrors_and_restores_after_a_wipe(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    saved = main_bot._snap_log
    rows = []

    async def remember(seq, digest, line):
        rows.append((seq, digest, line))

    async def broken(seq, digest, line):
        raise RuntimeError("db down SECRETTOKEN99")

    main_bot.init_quote_snapshot_log(path)
    main_bot.set_quote_snapshot_mirror(remember)
    try:
        async def write_two():
            await main_bot.submit_quote_snapshot({"n": 1})
            await main_bot.submit_quote_snapshot({"n": 2})

        _run(write_two())
        assert [row[0] for row in rows] == [1, 2]
        assert main_bot.quote_snapshot_log_status() == {
            "on_disk": 2, "durable": 2, "mirror_failures": 0,
            "restore_failed": False, "restore_attempts": 0,
        }
        path.unlink()
        restored = main_bot.restore_quote_snapshot_lines(path, [row[2] for row in rows])
        assert restored == 2
        assert main_bot.restore_quote_snapshot_lines(path, ["should-not-replace"]) == 0
        main_bot.init_quote_snapshot_log(path)
        ok, count, err = main_bot._snap_log.verify()
        assert ok, err
        assert count == 2
        assert main_bot.quote_snapshot_log_status()["on_disk"] == 2
        main_bot.set_quote_snapshot_mirror(broken)

        async def write_third():
            return await main_bot.submit_quote_snapshot({"n": 3})

        seq = _run(write_third())
        assert seq == 3
        assert main_bot.quote_snapshot_log_status()["mirror_failures"] == 1
        assert main_bot.quote_snapshot_log_status()["on_disk"] == 3
    finally:
        main_bot.set_quote_snapshot_mirror(None)
        main_bot._snap_log = saved
    assert "SECRETTOKEN99" not in caplog.text
    assert "RuntimeError" in caplog.text
    mirror_src = __import__("inspect").getsource(main_bot._quote_snapshot_mirror)
    assert "bot_quote_snapshots" in mirror_src
    assert "bot_ledger" not in mirror_src


def test_backfill_mirrors_a_row_the_live_mirror_missed(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    saved = main_bot._snap_log
    store = {}
    fail = {"left": 1}

    async def mirror(seq, digest, line):
        if fail["left"]:
            fail["left"] -= 1
            raise RuntimeError("mirror once SECRETTOKEN99")
        store[seq] = line

    main_bot.init_quote_snapshot_log(path)
    main_bot.set_quote_snapshot_mirror(mirror)
    try:
        async def write_two():
            await main_bot.submit_quote_snapshot({"n": 1})
            await main_bot.submit_quote_snapshot({"n": 2})

        _run(write_two())
        assert sorted(store) == [2]
        assert main_bot.quote_snapshot_log_status()["on_disk"] == 2
        pushed = _run(main_bot.backfill_quote_snapshots())
        assert pushed == 2
        assert sorted(store) == [1, 2]
        path.unlink()
        restored = main_bot.restore_quote_snapshot_lines(
            path, [store[seq] for seq in sorted(store)],
        )
        assert restored == 2
        main_bot.init_quote_snapshot_log(path)
        ok, count, err = main_bot._snap_log.verify()
    finally:
        main_bot.set_quote_snapshot_mirror(None)
        main_bot._snap_log = saved
    assert ok, err
    assert count == 2
    assert "SECRETTOKEN99" not in caplog.text
    assert "RuntimeError" in caplog.text


def test_restore_after_a_down_database_keeps_one_chain(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    saved = main_bot._snap_log
    prior = main_bot.QuoteSnapshotLog(path)
    prior.append({"n": "old"})
    prior.append({"n": "older"})
    db_lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    path.unlink()
    db = {"up": False, "lines": db_lines}

    async def loader():
        if not db["up"]:
            raise ConnectionError("db down SECRETTOKEN99")
        return list(db["lines"])

    main_bot._snap_restore_loader = loader
    main_bot.init_quote_snapshot_log(path)
    try:
        assert _run(main_bot.restore_quote_snapshots_once(path)) is False
        assert main_bot.quote_snapshot_log_status()["restore_failed"] is True
        assert main_bot.quote_snapshot_log_status()["restore_attempts"] >= 1

        async def write_new():
            await main_bot.submit_quote_snapshot({"n": "new"})
            await main_bot.submit_quote_snapshot({"n": "newer"})

        _run(write_new())
        mid = main_bot.quote_snapshot_log_status()
        assert mid["restore_failed"] is True
        assert mid["on_disk"] == 2
        db["up"] = True
        assert _run(main_bot.restore_quote_snapshots_once(path)) is True
        ok, count, err = main_bot._snap_log.verify()
        rows = [
            json.loads(line)["data"]["n"]
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    finally:
        main_bot._snap_restore_loader = None
        main_bot._snap_log = saved
    assert ok, err
    assert count == 4
    assert rows == ["old", "older", "new", "newer"]
    assert "SECRETTOKEN99" not in caplog.text
    assert "ConnectionError" in caplog.text
    assert main_bot.quote_snapshot_log_status()["restore_failed"] is False


def test_mirror_write_timeout_is_a_counted_failure(tmp_path):
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    saved = main_bot._snap_log
    main_bot._SNAPSHOT_MIRROR_TIMEOUT_S = 0.2

    async def slow(seq, digest, line):
        await asyncio.sleep(2)

    main_bot.init_quote_snapshot_log(path)
    main_bot.set_quote_snapshot_mirror(slow)
    try:
        async def once():
            started = time.monotonic()
            seq = await main_bot.submit_quote_snapshot({"n": 1})
            return seq, time.monotonic() - started

        seq, elapsed = _run(once())
        status = main_bot.quote_snapshot_log_status()
    finally:
        main_bot._SNAPSHOT_MIRROR_TIMEOUT_S = 2.0
        main_bot.set_quote_snapshot_mirror(None)
        main_bot._snap_log = saved
    assert seq == 1
    assert elapsed < 1.0
    assert status["mirror_failures"] == 1
    assert status["on_disk"] == 1


def test_flush_does_not_wait_when_the_writer_is_dead(tmp_path):
    path = Path(tmp_path) / "quote_snapshots.jsonl"
    saved = main_bot._snap_log
    main_bot.init_quote_snapshot_log(path)
    try:
        async def scenario():
            await main_bot.submit_quote_snapshot({"n": 0})
            task = main_bot._snap_task
            task.remove_done_callback(main_bot._on_snap_writer_done)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            fut = asyncio.get_running_loop().create_future()
            main_bot._snap_q.put_nowait(({"n": 9}, fut))
            started = time.monotonic()
            await main_bot.flush_quote_snapshot_writer(2.0)
            return time.monotonic() - started

        elapsed = _run(scenario())
    finally:
        main_bot._snap_log = saved
    assert elapsed < 0.4


def test_quote_snapshot_table_init_is_isolated(monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    class _Conn:
        async def execute(self, *_args, **_kwargs):
            raise RuntimeError("create failed SECRETTOKEN99")

    class _Begin:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_args):
            return False

    class _Engine:
        def begin(self):
            return _Begin()

    monkeypatch.setattr(main_bot, "engine", _Engine())
    assert _run(main_bot._ensure_quote_snapshot_table()) is False
    assert "SECRETTOKEN99" not in caplog.text
    assert "RuntimeError" in caplog.text

