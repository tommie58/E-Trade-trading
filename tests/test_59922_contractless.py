"""A contract-less option row alerts once per record per session."""
from __future__ import annotations

import asyncio

import main_bot
from tests.test_safety_gates import _qqq_portfolio


def _run(coro):
    return asyncio.run(coro)


def _legacy(entry: float) -> dict:
    return {
        "action": "BUY",
        "entry_premium": entry,
        "stop_premium": 1.5,
        "filled_qty": 1,
        "qty": 1,
        "ts": f"2026-11-18T15:0{int(entry)}:00+00:00",
    }


def test_contractless_option_alerts_once_per_record(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    main_bot.reconciliation._CONTRACTLESS_ALERTED.clear()
    events = []

    async def cancel(order_id):
        raise AssertionError("a contract-less row must not cancel")

    async def alert(severity, event, message, **kwargs):
        events.append((severity, event, kwargs.get("dedupe_key")))

    async def place(*args, **kwargs):
        raise AssertionError("a contract-less row must not place a stop")

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def orders():
        return {"OrdersResponse": {}}

    async def portfolio():
        book = _qqq_portfolio(1)
        positions = book["PortfolioResponse"]["AccountPortfolio"]["Position"]
        other = {
            "quantity": 1,
            "Product": {
                "symbol": "IWM",
                "securityType": "OPTN",
                "callPut": "CALL",
                "strikePrice": 200,
                "expiryYear": 2026,
                "expiryMonth": 12,
                "expiryDay": 18,
            },
        }
        positions.append(other)
        return book

    async def once():
        return await main_bot.reconciliation.reconcile_once(
            store, portfolio, orders, cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )

    async def scenario():
        await store.set_position("QQQ", _legacy(2.0))
        await store.set_position("IWM", _legacy(3.0))
        await once()
        await once()
        return list(events)

    found = _run(scenario())
    assert len(found) == 2
    assert {item[0] for item in found} == {"error"}
    assert {item[1] for item in found} == {"option_contract_missing"}
    assert len({item[2] for item in found}) == 2
    main_bot.reconciliation._CONTRACTLESS_ALERTED.clear()
