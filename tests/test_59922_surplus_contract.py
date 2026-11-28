"""Surplus repair is per contract, not per underlying."""
from __future__ import annotations

import asyncio

import main_bot
from tests.test_safety_gates import _dead_stop_option, _qqq_portfolio, _stop_order


def _run(coro):
    return asyncio.run(coro)


def _two_contracts(right="CALL", qty_a=2, qty_b=4):
    book = _qqq_portfolio(qty_a)
    held = book["PortfolioResponse"]["AccountPortfolio"]["Position"][0]
    held["Product"]["callPut"] = right
    held["Product"]["strikePrice"] = 450
    other = {
        "quantity": qty_b,
        "Product": {
            "symbol": "QQQ",
            "securityType": "OPTN",
            "callPut": right,
            "strikePrice": 500,
            "expiryYear": 2026,
            "expiryMonth": 12,
            "expiryDay": 18,
        },
    }
    book["PortfolioResponse"]["AccountPortfolio"]["Position"].append(other)
    return book


def test_surplus_repair_does_not_blend_two_contracts(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    cancelled = []

    async def cancel(order_id):
        cancelled.append(str(order_id))
        return True

    async def alert(*args, **kwargs):
        return None

    async def place(*args, **kwargs):
        raise AssertionError("a different contract must not be replaced")

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def orders(right):
        return {"OrdersResponse": {"Order": [
            _stop_order("MINE", "OPEN", 2, 2.4, right=right, strike=450),
            _stop_order("OTHER", "OPEN", 4, 1.2, right=right, strike=500),
        ]}}

    async def scenario(right):
        cancelled.clear()
        pos = _dead_stop_option()
        pos["contract"]["right"] = right
        pos["filled_qty"] = 2
        pos["qty"] = 2
        pos["stop_order_id"] = "MINE"
        await store.set_position("QQQ", pos)

        async def fetch():
            return await orders(right)

        async def portfolio():
            return _two_contracts(right)

        report = await main_bot.reconciliation.reconcile_once(
            store, portfolio, fetch, cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )
        assert "OTHER" not in cancelled
        assert "MINE" not in cancelled
        assert not any("surplus" in item for item in report["healed"])

    for right in ("CALL", "PUT"):
        _run(scenario(right))


def test_surplus_on_the_same_contract_still_cancels(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    cancelled = []

    async def cancel(order_id):
        cancelled.append(str(order_id))
        return True

    async def alert(*args, **kwargs):
        return None

    async def empty():
        return {"OrdersResponse": {}}

    async def place(ticker, contract, qty, premium):
        return {"order_id": "NEW"}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", empty)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def scenario(right):
        cancelled.clear()
        pos = _dead_stop_option()
        pos["contract"]["right"] = right
        pos["filled_qty"] = 2
        pos["qty"] = 2
        pos["stop_order_id"] = "KEEP"
        await store.set_position("QQQ", pos)

        async def fetch():
            return {"OrdersResponse": {"Order": [
                _stop_order("KEEP", "OPEN", 2, 2.4, right=right),
                _stop_order("EXTRA", "OPEN", 2, 2.4, right=right),
            ]}}

        async def portfolio():
            book = _qqq_portfolio(2)
            book["PortfolioResponse"]["AccountPortfolio"]["Position"][0]["Product"]["callPut"] = right
            return book

        await main_bot.reconciliation.reconcile_once(
            store, portfolio, fetch, cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )
        assert cancelled == ["EXTRA"]

    for right in ("CALL", "PUT"):
        _run(scenario(right))
