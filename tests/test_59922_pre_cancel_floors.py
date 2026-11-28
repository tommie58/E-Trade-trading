"""A cancel-then-re-arm must not rest a stop looser than the one it cancelled.

Each path is driven for a CALL and a PUT. The broker stopPrice is read
before the cancel, and the re-arm floor is never under that price.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot


ET = ZoneInfo("America/New_York")
BROKER_STOP = 2.80
SAVED_STOP = 2.40


def _run(coro):
    return asyncio.run(coro)


def _pos(right: str, **extra) -> dict:
    pos = {
        "action": "BUY",
        "entry_premium": 3.0,
        "stop_premium": SAVED_STOP,
        "stop": SAVED_STOP,
        "stop_order_id": "STOP-1",
        "filled_qty": 2,
        "qty": 2,
        "ts": "2026-11-18T14:00:00+00:00",
        "contract": {
            "right": right,
            "strike": 450.0,
            "expiration": "2026-12-18",
        },
    }
    pos.update(extra)
    return pos


class _Market:
    def __init__(self, *args, **kwargs):
        return None


def _install(monkeypatch, store, floors, *, cancel_result, read_stop=BROKER_STOP):
    phase = {"cancelled": False}
    placed = []

    async def read(order_id):
        assert phase["cancelled"] is False
        return read_stop

    async def cancel(order_id, expected_qty=None):
        phase["cancelled"] = True
        return cancel_result

    async def rearm(ticker, pos, floor_price=None):
        floors.append(floor_price)
        return "placed"

    async def quote(ticker, contract=None):
        return 4.00, 4.10

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_read_open_order_stop", read)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_reconcile_rearm_guard", rearm)
    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot.alerts, "send", quiet)
    return phase, placed


def _assert_floors(floors, placed):
    assert floors, "expected a re-arm"
    for floor in floors:
        assert floor is not None
        assert float(floor) + 1e-9 >= BROKER_STOP
    for price in placed:
        assert float(price) + 1e-9 >= BROKER_STOP


def test_scale_out_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    async def scenario(right, outcome, close_raises):
        store = main_bot.StateStore(None)
        floors = []
        _install(monkeypatch, store, floors, cancel_result=outcome)
        await store.set_position("QQQ", _pos(right))

        def trusted(*args, **kwargs):
            return True

        async def partial(*args, **kwargs):
            return 1

        async def close(*args, **kwargs):
            if close_raises:
                raise RuntimeError("scale close failed")
            return "CLOSE-1"

        monkeypatch.setattr(main_bot, "_mark_trusted_for_harvest", trusted)
        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        monkeypatch.setattr(main_bot, "_place_option_market_close", close)
        monkeypatch.setattr(main_bot, "_resolve_exit_fill", lambda *a, **k: _async_none())
        await main_bot._maybe_scale_out("QQQ", await store.get_position("QQQ"), 4.0)
        _assert_floors(floors, [])

    async def _async_none():
        return None

    for right in ("CALL", "PUT"):
        _run(scenario(right, main_bot.CANCEL_CONFIRMED, True))
        _run(scenario(right, main_bot.CANCEL_PARTIAL, False))


def test_take_profit_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    async def scenario(right, outcome, close_raises):
        store = main_bot.StateStore(None)
        floors = []
        _install(monkeypatch, store, floors, cancel_result=outcome)
        pos = _pos(right, target_premium=3.2, filled_qty=1, qty=1)
        await store.set_position("QQQ", pos)

        def trusted(*args, **kwargs):
            return True

        async def partial(*args, **kwargs):
            return 1

        async def close(*args, **kwargs):
            if close_raises:
                raise RuntimeError("take profit close failed")
            return "CLOSE-1"

        monkeypatch.setattr(main_bot, "_mark_trusted_for_harvest", trusted)
        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        monkeypatch.setattr(main_bot, "_place_option_market_close", close)
        await main_bot._maybe_take_profit("QQQ", await store.get_position("QQQ"), 4.0)
        _assert_floors(floors, [])

    for right in ("CALL", "PUT"):
        _run(scenario(right, main_bot.CANCEL_CONFIRMED, True))
        _run(scenario(right, main_bot.CANCEL_PARTIAL, False))


def test_giveup_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    async def scenario(right, outcome):
        store = main_bot.StateStore(None)
        floors = []
        _phase, placed = _install(monkeypatch, store, floors, cancel_result=outcome)
        await store.set_position("QQQ", _pos(right, trail=None, filled_qty=1, qty=1))

        async def partial(*args, **kwargs):
            return 1

        async def place(ticker, contract, qty, premium):
            placed.append(premium)
            raise RuntimeError("giveup place rejected")

        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
        await main_bot._tighten_stop_after_giveup(
            "QQQ", await store.get_position("QQQ"), "scale_out",
        )
        if outcome == main_bot.CANCEL_PARTIAL:
            _assert_floors(floors, [])
        else:
            _assert_floors(floors, placed)

    for right in ("CALL", "PUT"):
        _run(scenario(right, main_bot.CANCEL_CONFIRMED))
        _run(scenario(right, main_bot.CANCEL_PARTIAL))


def test_scheduled_close_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    when = datetime(2026, 11, 18, 15, 45, tzinfo=ET)

    def frozen():
        return when.astimezone(timezone.utc)

    async def scenario(right, outcome):
        store = main_bot.StateStore(None)
        floors = []
        _install(monkeypatch, store, floors, cancel_result=outcome)
        monkeypatch.setattr(main_bot, "_utcnow", frozen)
        await store.set_position("QQQ", _pos(right, filled_qty=1, qty=1))

        async def partial(*args, **kwargs):
            return 1

        async def close(*args, **kwargs):
            raise RuntimeError("scheduled close failed")

        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        monkeypatch.setattr(main_bot, "_place_option_market_close", close)
        await main_bot._maybe_scheduled_exit(
            "QQQ", await store.get_position("QQQ"), 4.0,
        )
        _assert_floors(floors, [])

    for right in ("CALL", "PUT"):
        _run(scenario(right, main_bot.CANCEL_CONFIRMED))
        _run(scenario(right, main_bot.CANCEL_PARTIAL))


def test_trail_partial_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    when = datetime(2026, 11, 18, 11, 0, tzinfo=ET)

    def frozen():
        return when.astimezone(timezone.utc)

    async def scenario(right):
        store = main_bot.StateStore(None)
        floors = []
        _install(
            monkeypatch, store, floors, cancel_result=main_bot.CANCEL_PARTIAL,
        )
        monkeypatch.setattr(main_bot, "_utcnow", frozen)
        pos = _pos(right, filled_qty=1, qty=1)
        await store.set_position("QQQ", pos)

        async def eyes(ticker, position):
            return position

        async def no(*args, **kwargs):
            return False

        async def partial(*args, **kwargs):
            return 1

        def plan(*args, **kwargs):
            return {
                "initial_stop": SAVED_STOP,
                "high_since": 4.0,
                "low_since": 2.5,
                "new_stop": 2.60,
                "current_stop": SAVED_STOP,
                "should_replace": True,
                "trail": {
                    "tier": "breakeven",
                    "r_multiple": 1.0,
                    "mfe_percent": 10.0,
                    "breakeven_locked": True,
                    "giveback_active": False,
                },
            }

        monkeypatch.setattr(main_bot, "_refresh_trail_eyes", eyes)
        monkeypatch.setattr(main_bot, "_maybe_scheduled_exit", no)
        monkeypatch.setattr(main_bot, "_app_owns_exit", no)
        monkeypatch.setattr(main_bot, "_maybe_take_profit", no)
        monkeypatch.setattr(main_bot, "_maybe_scale_out", no)
        monkeypatch.setattr(main_bot, "_maybe_hand_off_native_trail", no)
        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        monkeypatch.setattr(main_bot.trailing_engine, "plan_ratchet", plan)
        await main_bot._trail_ratchet_one("QQQ", dict(pos))
        _assert_floors(floors, [])

    for right in ("CALL", "PUT"):
        _run(scenario(right))


def test_stop_guard_partial_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    async def scenario(right):
        store = main_bot.StateStore(None)
        floors = []
        _install(
            monkeypatch, store, floors, cancel_result=main_bot.CANCEL_PARTIAL,
        )
        await store.set_position("QQQ", _pos(right, filled_qty=2, qty=2))
        await store.set_guard("QQQ", {
            "done": False,
            "kind": "option",
            "action": "BUY",
            "stop": SAVED_STOP,
            "stop_order_id": "STOP-1",
            "guarded_qty": 1,
            "trued_qty": 2,
            "entry_order_id": "ENTRY-1",
            "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
            "deadline_ts": 9e12,
        })

        async def order_state(order_id, client_id):
            return "EXECUTED", 2

        async def partial(*args, **kwargs):
            return 1

        monkeypatch.setattr(main_bot, "_order_state", order_state)
        monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
        await main_bot._stop_guard_worker("QQQ")
        _assert_floors(floors, [])

    for right in ("CALL", "PUT"):
        _run(scenario(right))


def test_close_failed_rearm_is_not_looser_than_the_pre_cancel_stop(monkeypatch):
    async def limits(*args, **kwargs):
        return None

    async def account(tokens):
        return "acct"

    def snap(*args, **kwargs):
        return ("2026-12-18", 450.0, 4.0, 4.1)

    async def crossing(*args, **kwargs):
        raise RuntimeError("option close failed")

    async def equity_place(*args, **kwargs):
        raise RuntimeError("equity close failed")

    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})
    monkeypatch.setattr(main_bot, "check_risk_limits", limits)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_snap_option_contract", snap)
    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", _Market)

    async def option_close(right):
        store = main_bot.StateStore(None)
        floors = []
        _install(monkeypatch, store, floors, cancel_result=main_bot.CANCEL_CONFIRMED)
        await store.set_position("QQQ", _pos(right, filled_qty=1, qty=1))
        monkeypatch.setattr(main_bot, "_place_crossing_exit", crossing)
        payload = {
            "mode": "live",
            "instrument": "option",
            "ticker": "QQQ",
            "action": "CLOSE",
            "intent": "close",
            "strike": 450,
            "expiration": "2026-12-18",
            "option_right": right,
            "option_contracts": 1,
        }
        with pytest.raises(Exception, match="option close failed"):
            await main_bot.execute_live_order(payload)
        _assert_floors(floors, [])

    async def equity_close():
        store = main_bot.StateStore(None)
        floors = []
        _install(monkeypatch, store, floors, cancel_result=main_bot.CANCEL_CONFIRMED)
        await store.set_position("QQQ", {
            "action": "BUY",
            "entry": 100.0,
            "stop": SAVED_STOP,
            "stop_order_id": "STOP-1",
            "filled_qty": 10,
            "qty": 10,
        })
        monkeypatch.setattr(main_bot, "_place_order_smart", equity_place)
        payload = {
            "mode": "live",
            "instrument": "stock",
            "ticker": "QQQ",
            "action": "CLOSE",
            "intent": "close",
        }
        with pytest.raises(Exception, match="equity close failed"):
            await main_bot.execute_live_order(payload)
        _assert_floors(floors, [])

    for right in ("CALL", "PUT"):
        _run(option_close(right))
    _run(equity_close())
