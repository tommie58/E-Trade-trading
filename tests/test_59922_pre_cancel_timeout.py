"""A hung list_orders must not hold the flatten, and must not loosen a re-arm."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot


ET = ZoneInfo("America/New_York")
SAVED_STOP = 2.40
TRAIL_STOP = 2.55
HANG_S = 5.0
BOUND_S = 0.2


def _run(coro):
    return asyncio.run(coro)


def test_pre_cancel_stop_read_timeout_is_four_seconds():
    assert main_bot.PRE_CANCEL_STOP_READ_TIMEOUT_S == 4.0


def _pos() -> dict:
    return {
        "action": "BUY",
        "entry_premium": 3.0,
        "stop_premium": SAVED_STOP,
        "stop": SAVED_STOP,
        "trail": TRAIL_STOP,
        "stop_order_id": "STOP-1",
        "filled_qty": 1,
        "qty": 1,
        "ts": "2026-11-18T14:00:00+00:00",
        "contract": {
            "right": "CALL",
            "strike": 450.0,
            "expiration": "2026-12-18",
        },
    }


def _install_hang(monkeypatch, store, *, fail_close: bool):
    calls = {"fetch": 0}
    placed = []
    closed = []

    async def fetch():
        calls["fetch"] += 1
        if calls["fetch"] == 1:
            await asyncio.sleep(HANG_S)
        return {"OrdersResponse": {}}

    async def cancel(order_id, expected_qty=None):
        return main_bot.CANCEL_CONFIRMED

    async def close(*args, **kwargs):
        closed.append(args)
        if fail_close:
            raise RuntimeError("close failed")
        return "CLOSE-1"

    async def place(ticker, contract, qty, price):
        placed.append(price)
        return {"order_id": "NEW-STOP"}

    when = datetime(2026, 11, 18, 15, 45, tzinfo=ET)

    def frozen():
        return when.astimezone(timezone.utc)

    async def fill(order_id):
        return 4.0

    async def record(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "PRE_CANCEL_STOP_READ_TIMEOUT_S", BOUND_S)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_option_market_close", close)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", fill)
    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    monkeypatch.setattr(main_bot, "_utcnow", frozen)
    return placed, closed


def test_hung_list_orders_does_not_delay_eod_flatten(monkeypatch, caplog):
    store = main_bot.StateStore(None)
    placed, closed = _install_hang(monkeypatch, store, fail_close=False)

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _pos())
        started = time.monotonic()
        await main_bot._maybe_scheduled_exit(
            "QQQ", await store.get_position("QQQ"), 4.0,
        )
        return time.monotonic() - started

    elapsed = _run(scenario())
    assert closed, "the flatten close did not run"
    assert elapsed < BOUND_S + 1.0
    assert "TimeoutError" in caplog.text
    assert placed == []


def test_rearm_floor_is_not_looser_after_a_timed_out_read(monkeypatch):
    store = main_bot.StateStore(None)
    placed, closed = _install_hang(monkeypatch, store, fail_close=True)

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _pos())
        started = time.monotonic()
        await main_bot._maybe_scheduled_exit(
            "QQQ", await store.get_position("QQQ"), 4.0,
        )
        return time.monotonic() - started

    elapsed = _run(scenario())
    assert closed, "the close was not attempted"
    assert placed == [TRAIL_STOP]
    assert placed[0] >= SAVED_STOP
    assert elapsed < BOUND_S + 1.0


def test_native_trail_refuses_when_the_stop_read_times_out(monkeypatch):
    store = main_bot.StateStore(None)
    cancelled = []

    async def fetch():
        await asyncio.sleep(HANG_S)
        return {"OrdersResponse": {}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        return main_bot.CANCEL_CONFIRMED

    async def quote(ticker, contract=None):
        return 4.00, 4.10

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "PRE_CANCEL_STOP_READ_TIMEOUT_S", BOUND_S)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)

    async def scenario():
        pos = _pos()
        pos["breakeven_locked"] = True
        await store.set_position("QQQ", pos)
        started = time.monotonic()
        with pytest.raises(main_bot.NativeTrailRefused, match="could not be read"):
            await main_bot._arm_native_trail(
                "QQQ", min_trigger=2.4, source="auto",
            )
        return time.monotonic() - started

    elapsed = _run(scenario())
    assert cancelled == []
    assert elapsed < HANG_S


def test_rearm_book_read_times_out_and_still_places(monkeypatch):
    store = main_bot.StateStore(None)
    placed = []

    async def fetch():
        await asyncio.sleep(HANG_S)
        return {"OrdersResponse": {}}

    async def place(ticker, contract, qty, price):
        placed.append(price)
        return {"order_id": "NEW-STOP"}

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "PRE_CANCEL_STOP_READ_TIMEOUT_S", BOUND_S)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def scenario():
        started = time.monotonic()
        result = await main_bot._reconcile_rearm_guard("QQQ", _pos())
        return result, time.monotonic() - started

    result, elapsed = _run(scenario())
    assert result == "placed"
    assert placed == [TRAIL_STOP]
    assert elapsed < BOUND_S + 1.0
