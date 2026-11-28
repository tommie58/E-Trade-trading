"""A 1-lot stop on a 2-lot position is partial cover, not an unprotected heal."""
from __future__ import annotations

import asyncio

import main_bot
from tests.test_safety_gates import _dead_stop_option, _qqq_portfolio, _stop_order


def _run(coro):
    return asyncio.run(coro)


def test_partial_cover_does_not_place_a_full_stop_on_top(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    events = []
    live = {"open": True, "right": "CALL"}

    async def cancel(order_id):
        events.append(("cancel", str(order_id)))
        live["open"] = False
        return True

    async def alert(severity, event, message, **kwargs):
        events.append((severity, event))

    async def place(ticker, contract, qty, premium):
        if live["open"]:
            raise RuntimeError("3004 already allocated")
        events.append(("place", qty))
        return {"order_id": "NEW"}

    async def book():
        if not live["open"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": _stop_order(
            "SMALL", "OPEN", 1, 2.4, right=live["right"],
        )}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def scenario(right):
        events.clear()
        live["open"] = True
        live["right"] = right
        pos = _dead_stop_option()
        pos["contract"]["right"] = right
        pos["filled_qty"] = 2
        pos["qty"] = 2
        pos["stop_order_id"] = "SMALL"
        await store.set_position("QQQ", pos)

        async def orders():
            return await book()

        async def portfolio():
            book_pos = _qqq_portfolio(2)
            book_pos["PortfolioResponse"]["AccountPortfolio"]["Position"][0]["Product"]["callPut"] = right
            return book_pos

        await main_bot.reconciliation.reconcile_once(
            store, portfolio, orders, cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )
        assert ("critical", "position_unprotected") not in events
        assert ("warning", "protection_partial") in events
        assert ("cancel", "SMALL") in events
        places = [item for item in events if item[0] == "place"]
        assert places == [("place", 2)]
        assert events.index(("cancel", "SMALL")) < events.index(("place", 2))

    for right in ("CALL", "PUT"):
        _run(scenario(right))
