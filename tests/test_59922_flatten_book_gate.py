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
        result = await main_bot._emergency_flatten(
            "QQQ", {"kind": "equity", "action": "BUY"}, 100,
        )
        return result, list(sold)

    assert _run(scenario()) == (main_bot.NOT_SOLD, [])


def test_not_found_prior_flatten_sends_one_sell(monkeypatch):
    """Options still re-send. A lagging stock lookup does not."""
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
        return await main_bot._emergency_flatten(
            "QQQ", await store.get_guard("QQQ"), 100,
        ), list(sold)

    equity_id, equity_sold = _run(scenario("equity"))
    assert equity_id == main_bot.NOT_SOLD
    assert equity_sold == []
    assert _run(scenario("option"))[1] == [100]


def test_stock_not_found_resend_is_once_and_never_over_held(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def order_state(order_id, client_id):
        return "NOT_FOUND", 0

    async def book():
        return {"OrdersResponse": {"Order": {
            "orderId": "FLAT-1",
            "clientOrderId": "FLAT-CLIENT",
            "OrderDetail": {
                "status": "REJECTED",
                "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL",
                    "orderedQuantity": 100,
                    "filledQuantity": 0,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }}}

    async def held(ticker, pos):
        return 100

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)

    async def scenario():
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "flatten_order_id": "FLAT-1", "flatten_client_id": "FLAT-CLIENT",
            "flatten_attempts": 1,
        })
        await main_bot._emergency_flatten("QQQ", await store.get_guard("QQQ"), 100)
        again = await main_bot._emergency_flatten("QQQ", await store.get_guard("QQQ"), 100)
        return list(sold), again

    sold_qty, again = _run(scenario())
    assert sold_qty == [100]
    assert again == main_bot.NOT_SOLD


def test_filled_stock_sell_that_reads_not_found_does_not_go_short(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def order_state(order_id, client_id):
        return "NOT_FOUND", 0

    async def empty_book():
        return {"OrdersResponse": {}}

    async def held(ticker, pos):
        return 0

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", empty_book)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)

    async def scenario():
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "flatten_order_id": "FLAT-1", "flatten_client_id": "FLAT-CLIENT",
            "flatten_attempts": 1,
        })
        return await main_bot._emergency_flatten(
            "QQQ", await store.get_guard("QQQ"), 100,
        )

    assert _run(scenario()) == main_bot.NOT_SOLD
    assert sold == []


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
        return order_id

    assert _run(scenario()) == main_bot.NOT_SOLD
    assert sold == []


def test_unsold_prior_flatten_keeps_the_guard_paging(monkeypatch):
    store = main_bot.StateStore(None)
    alerts = []
    clock = {"t": 5_000.0}
    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 100

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def flatten(*args, **kwargs):
        raise main_bot.FlattenNotSent("nothing sold")

    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)

    async def poll():
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 100, "trued_qty": 100,
            "fill_detected_ts": 1, "stop_deadline_ts": 1, "deadline_ts": 1,
            "entry_order_id": "ENTRY-1",
        })
        await poll()
        first = [row for row in alerts if row[1] == "position_unprotected"]
        held = await store.get_guard("QQQ")
        assert held.get("done") is not True
        assert ("critical", "stop_timeout_flattened") not in alerts
        clock["t"] += main_bot.UNCONFIRMED_STOP_PAGE_S
        await poll()
        second = [row for row in alerts if row[1] == "position_unprotected"]
        return len(first), len(second)

    first, second = _run(scenario())
    assert first >= 1
    assert second == first + 1
