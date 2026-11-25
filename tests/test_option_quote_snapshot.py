"""Option quote snapshots at entry and at modeled exits. Logging only."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
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
    yield
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    main_bot._OPTION_SNAPSHOT_SEEN.clear()


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
