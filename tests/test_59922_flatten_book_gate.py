"""Panic and daily-loss sell when list_orders fails.

The stop-guard deadline flatten still refuses that sell.
"""
from __future__ import annotations

import asyncio

import pytest

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _install(monkeypatch, store):
    sold = []

    async def fetch():
        raise RuntimeError("orders down")

    async def smart(kind, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT-1"}

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    async def quiet(*args, **kwargs):
        return None

    async def send_nowait(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    return sold


def test_panic_and_daily_loss_sell_when_the_order_book_fails(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def scenario(trigger):
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
        })
        await main_bot._flatten_book(trigger)
        return list(sold)

    assert _run(scenario("panic")) == [100]
    sold.clear()
    assert _run(scenario("daily_loss")) == [100]


def test_deadline_flatten_does_not_sell_when_the_order_book_fails(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def scenario():
        with pytest.raises(Exception, match="RuntimeError"):
            await main_bot._emergency_flatten(
                "QQQ", {"kind": "equity", "action": "BUY"}, 100,
            )
        return list(sold)

    assert _run(scenario()) == []


def test_not_found_prior_flatten_sends_one_sell(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def order_state(order_id, client_id):
        return "NOT_FOUND", 0

    async def empty_book():
        return {"OrdersResponse": {}}

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", empty_book)

    async def scenario(kind):
        sold.clear()
        await store.set_guard("QQQ", {
            "done": False,
            "kind": kind,
            "action": "BUY",
            "flatten_order_id": "FLAT-1",
            "flatten_client_id": "FLAT-CLIENT",
            "flatten_attempts": 1,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        order_id = await main_bot._emergency_flatten(
            "QQQ", await store.get_guard("QQQ"), 100,
        )
        return order_id, list(sold)

    assert _run(scenario("equity"))[1] == [100]
    assert _run(scenario("option"))[1] == [100]


def test_stock_does_not_resell_when_a_working_exit_is_already_there(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def order_state(order_id, client_id):
        return "NOT_FOUND", 0

    async def fetch():
        return {"OrdersResponse": {"Order": {
            "orderId": "SELL-1",
            "clientOrderId": "FLAT-CLIENT",
            "OrderDetail": {
                "status": "OPEN",
                "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL",
                    "orderedQuantity": 100,
                    "filledQuantity": 0,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }}}

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)

    async def scenario():
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "flatten_order_id": "FLAT-1", "flatten_client_id": "FLAT-CLIENT",
            "flatten_attempts": 1,
        })
        order_id = await main_bot._emergency_flatten(
            "QQQ", await store.get_guard("QQQ"), 100,
        )
        return order_id, list(sold)

    order_id, qty = _run(scenario())
    assert qty == []
    assert order_id == "FLAT-1"
