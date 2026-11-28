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

    async def held(ticker, pos):
        return None

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", empty_book)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)

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
    option_id, option_sold = _run(scenario("option"))
    assert option_id == main_bot.NOT_SOLD
    assert option_sold == []


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
        first = await main_bot._emergency_flatten("QQQ", await store.get_guard("QQQ"), 100)
        return first, list(sold)

    result, sold_qty = _run(scenario())
    assert result == main_bot.NOT_SOLD
    assert sold_qty == []


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
        assert held.get("result") != "stop_timeout_flattened"
        assert held.get("flatten_unresolved") is True
        assert ("critical", "stop_timeout_flattened") not in alerts
        clock["t"] += main_bot.UNCONFIRMED_STOP_PAGE_S
        await poll()
        second = [row for row in alerts if row[1] == "position_unprotected"]
        return len(first), len(second)

    first, second = _run(scenario())
    assert first >= 1
    assert second == first + 1


def test_panic_stock_sells_the_tracked_quantity(monkeypatch):
    """Panic uses the pre-split path: tracked qty, even if a holdings read would fail."""
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)
    reads = []

    async def held(ticker, pos):
        reads.append(ticker)
        return None

    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "stop_client_id": "LOST-CLIENT", "stop_reply_lost": True,
        })
        await main_bot._flatten_book("panic")
        return list(sold), await store.get_guard("QQQ")

    sold_qty, guard = _run(scenario())
    assert sold_qty == [100]
    assert reads == []
    assert guard.get("result") == "flattened"


def test_option_not_found_resells_only_the_held_contracts(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def order_state(order_id, client_id):
        return "NOT_FOUND", 0

    async def held(ticker, pos):
        return 2

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)

    async def scenario():
        await store.set_guard("QQQ", {
            "done": False, "kind": "option", "action": "BUY",
            "flatten_order_id": "FLAT-1", "flatten_client_id": "FLAT-CLIENT",
            "flatten_attempts": 1,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        return await main_bot._emergency_flatten(
            "QQQ", await store.get_guard("QQQ"), 4,
        )

    assert _run(scenario())
    assert sold == [2]


def test_empty_portfolio_read_is_not_flat(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)

    async def empty():
        return {"PortfolioResponse": {"AccountPortfolio": []}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_portfolio", empty)

    async def scenario():
        return await main_bot._flatten_broker_held_qty("QQQ", {"action": "BUY"})

    assert _run(scenario()) is None


def _drive_deadline(monkeypatch, store, *, kind, reply_lost, client_id, deadline, qty=100):
    sold = []
    alerts = []
    clock = {"t": 10_000.0}

    async def place(*args, **kwargs):
        raise main_bot.ETradeAPIError(400, "rejected")

    async def fetch():
        raise RuntimeError("book down")

    async def smart(kind_name, common, tokens):
        sold.append((
            str(common.get("orderAction") or ""),
            int(common.get("quantity") or 0),
            clock["t"],
        ))
        return {"orderId": "MKT-1"}

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event, clock["t"]))

    async def order_state(order_id, client_id_arg):
        return "EXECUTED", qty

    steps = {"n": 0}

    async def nap(seconds):
        clock["t"] += float(seconds)
        steps["n"] += 1
        if sold or (reply_lost and steps["n"] >= 1) or clock["t"] > 20_000:
            await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    if kind == "option":
        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    else:
        monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    return sold, alerts, clock, deadline


def test_definite_reject_sells_when_the_book_is_down(monkeypatch):
    store = main_bot.StateStore(None)
    deadline = 10_060.0
    sold, alerts, clock, _deadline = _drive_deadline(
        monkeypatch, store, kind="equity", reply_lost=False,
        client_id="", deadline=deadline,
    )

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100,
            "fill_detected_ts": 1, "stop_deadline_ts": deadline,
            "deadline_ts": 100_000.0, "entry_order_id": "ENTRY-1",
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(sold)

    fills = _run(scenario())
    assert fills and fills[0][0] == "SELL" and fills[0][1] == 100
    assert deadline <= fills[0][2] <= deadline + 10


def test_lost_reply_with_the_book_down_pages_and_does_not_sell(monkeypatch):
    store = main_bot.StateStore(None)
    sold, alerts, _clock, _deadline = _drive_deadline(
        monkeypatch, store, kind="equity", reply_lost=True,
        client_id="LOST", deadline=1.0,
    )

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100,
            "fill_detected_ts": 1, "stop_deadline_ts": 1.0,
            "deadline_ts": 100_000.0, "entry_order_id": "ENTRY-1",
            "stop_reply_lost": True, "stop_client_id": "LOST",
            "stop_reply_bound": True,
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(sold), list(alerts)

    fills, sent = _run(scenario())
    assert fills == []
    assert any(event == "position_unprotected" for _sev, event, _t in sent)


def test_option_deadline_still_sells_when_the_book_is_down(monkeypatch):
    store = main_bot.StateStore(None)
    sold, _alerts, _clock, _deadline = _drive_deadline(
        monkeypatch, store, kind="option", reply_lost=False,
        client_id="", deadline=1.0, qty=2,
    )

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": 2, "qty": 2,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "option", "action": "BUY", "stop": 2.4,
            "guarded_qty": 0, "trued_qty": 2,
            "fill_detected_ts": 1, "stop_deadline_ts": 1.0,
            "deadline_ts": 100_000.0, "entry_order_id": "ENTRY-1",
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(sold)

    fills = _run(scenario())
    assert fills and fills[0][0] == "SELL_CLOSE" and fills[0][1] == 2


def test_panic_after_the_lost_sell_does_not_sell_a_flat_or_short(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)
    alerts = []
    mode = {"held": 0}

    async def held(ticker, pos):
        return mode["held"]

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append(event)

    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)

    async def scenario(value):
        sold.clear()
        alerts.clear()
        mode["held"] = value
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "stop_lost_flatten_sent": True, "flatten_client_id": "FLAT",
        })
        await main_bot._flatten_book("panic")
        return list(sold), list(alerts)

    flat_sold, flat_alerts = _run(scenario(0))
    assert flat_sold == []
    short_sold, short_alerts = _run(scenario(-100))
    assert short_sold == []
    assert "position_unprotected" in short_alerts
