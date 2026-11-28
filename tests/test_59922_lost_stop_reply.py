"""A stop the broker accepted, whose reply was lost, retries once.

The book lags, so the retry is a 3004. That is the last place. Nothing
cancels the unseen stop and nothing flattens through the gap.
"""
from __future__ import annotations

import asyncio

import main_bot


def _run(coro):
    return asyncio.run(coro)


class _Gone(Exception):
    """Reply lost after the broker accepted the stop."""


class _Allocated(Exception):
    code = "3004"

    def __init__(self):
        super().__init__("Code: 3004 contracts already allocated to an open order")


def test_lost_stop_reply_retries_once_and_stops_on_3004(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    places = []
    cancels = []
    flats = []
    alerts = []

    async def place(ticker, contract, qty, premium):
        places.append((contract.get("right"), qty, premium))
        if len(places) == 1:
            raise _Gone()
        raise _Allocated()

    async def cancel(*args, **kwargs):
        cancels.append(args)
        return main_bot.CANCEL_CONFIRMED

    async def flatten(*args, **kwargs):
        flats.append(args)
        return "FLAT"

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 1

    async def book():
        return {"OrdersResponse": {}}

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)

    async def scenario(right):
        places.clear()
        cancels.clear()
        flats.clear()
        alerts.clear()
        await store.set_position("QQQ", {
            "action": "BUY",
            "entry_premium": 3.0,
            "stop_premium": 2.4,
            "filled_qty": 1,
            "qty": 1,
            "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", {
            "done": False,
            "kind": "option",
            "action": "BUY",
            "stop": 2.4,
            "guarded_qty": 0,
            "trued_qty": 1,
            "entry_order_id": "ENTRY-1",
            "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
            "deadline_ts": 1,
            "stop_deadline_ts": 1,
        })
        for _ in range(3):
            guard = await store.get_guard("QQQ")
            guard["done"] = False
            await store.set_guard("QQQ", guard)
            await main_bot._stop_guard_worker("QQQ")
        assert len(places) == 2
        assert places[0][0] == right
        assert cancels == []
        assert flats == []
        assert ("critical", "position_unprotected") not in alerts
        held = await store.get_guard("QQQ")
        assert held.get("stop_allocation_held") is True

    for right in ("CALL", "PUT"):
        _run(scenario(right))
