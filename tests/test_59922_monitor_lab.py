"""Monitor H1–H5 and the Lab follow-ups on the lost-stop head."""
from __future__ import annotations

import asyncio
import json

import main_bot
from test_59922_lost_reply_retry import _guard, _position, _run, _stop_payload


def _tokens():
    return {"oauth_token": "t", "oauth_token_secret": "s"}


def _wire_book(monkeypatch, client):
    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_require_probeable_session", lambda: None)

    async def account(*_a, **_k):
        return "acct"

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_raw_client", lambda _tokens, timeout=20.0: client)


def _etrade_3004():
    body = json.dumps({
        "Error": {
            "code": 3004,
            "message": (
                "The shares for this security may already be allocated "
                "to an existing open order."
            ),
        },
    })
    return main_bot.ETradeAPIError(
        400, "allocated", body=body, code="3004",
        method="POST", path="/v1/accounts/x/orders/place.json",
    )


def test_full_page_with_a_live_stop_does_not_rearm(monkeypatch):
    store = main_bot.StateStore(None)
    rearms = []
    alerts = []

    class Client:
        async def list_orders(self, account_id_key, status=None, count=50, marker=None):
            assert status == "OPEN"
            assert count == 50
            row = _stop_payload("LIVE-STOP")
            return {"OrdersResponse": {"Order": [row for _ in range(50)]}}

    _wire_book(monkeypatch, Client())
    monkeypatch.setattr(main_bot, "state", store)

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": {"Position": [{
            "symbolDescription": "QQQ", "quantity": 100,
            "Product": {"symbol": "QQQ", "securityType": "EQ"},
        }]}}}

    async def fetch():
        return await main_bot._read_order_book(status="OPEN")

    async def rearm(ticker, position):
        rearms.append(ticker)
        return "placed"

    async def alert(severity, event, message, **kwargs):
        alerts.append(event)

    async def scenario():
        try:
            await main_bot._read_order_book(status="OPEN")
            return "empty"
        except main_bot.OrdersReadInconclusive:
            pass
        await store.set_position("QQQ", _position("equity", 100))
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, fetch, lambda *_a, **_k: True,
            rearm_guard=rearm, alert=alert,
        )
        return list(rearms), list(alerts)

    raised_rearms, events = _run(scenario())
    assert raised_rearms == []
    assert "position_unprotected" not in events


def test_lookup_stops_on_a_page_one_order(monkeypatch):
    calls = []

    class Orders:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            calls.append(marker)
            if marker is not None:
                raise AssertionError("history after the match was read")
            return {"OrdersResponse": {
                "marker": "more-than-500",
                "Order": [{
                    "orderId": "TARGET",
                    "OrderDetail": {
                        "status": "OPEN",
                        "Instrument": {"filledQuantity": 3},
                    },
                }],
            }}

    async def direct(fn, *args, source="etrade", idempotent=True, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_orders_client", lambda _tokens: Orders())
    monkeypatch.setattr(main_bot, "_etrade_call", direct)

    async def account(*_a, **_k):
        return "acct"

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)

    async def scenario():
        return await main_bot._order_state("TARGET", None)

    status, filled = _run(scenario())
    assert (status, filled) == ("OPEN", 3)
    assert calls == [None]


def test_next_url_is_read_as_a_marker():
    seen = []

    async def fetch(marker):
        seen.append(marker)
        if marker == "from-url":
            return {"OrdersResponse": {"Order": [_stop_payload("STOP-9")]}}
        return {"OrdersResponse": {
            "Order": [_stop_payload(f"LMT-{i}", price_type="LIMIT", stop=None) for i in range(25)],
            "next": "https://api.etrade.com/v1/accounts/x/orders.json?count=25&marker=from-url",
        }}

    async def scenario():
        merged = await main_bot.gather_order_pages(fetch, 25)
        orders = main_bot.reconciliation.parse_open_orders(merged)
        return [order["order_id"] for order in orders]

    ids = _run(scenario())
    assert seen == [None, "from-url"]
    assert "STOP-9" in ids


def test_a_repeated_marker_is_inconclusive():
    async def fetch(marker):
        nxt = {None: "A", "A": "B", "B": "A"}[marker]
        return {"OrdersResponse": {
            "Order": [_stop_payload("LMT-1", price_type="LIMIT", stop=None)],
            "marker": nxt,
        }}

    async def scenario():
        try:
            await main_bot.gather_order_pages(fetch, 25)
        except main_bot.OrdersReadInconclusive:
            return "inconclusive"
        return "complete"

    assert _run(scenario()) == "inconclusive"


def test_a_bounded_order_read_times_out(monkeypatch):
    monkeypatch.setattr(main_bot, "PRE_CANCEL_STOP_READ_TIMEOUT_S", 0.05)

    class Client:
        async def list_orders(self, *args, **kwargs):
            await asyncio.sleep(1)
            return {"OrdersResponse": {}}

    _wire_book(monkeypatch, Client())

    async def scenario():
        try:
            await main_bot._read_order_book(bounded=True, status="OPEN")
        except asyncio.TimeoutError:
            return "timeout"
        return "book"

    assert _run(scenario()) == "timeout"


def test_a_different_stop_pages_and_does_not_sell(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []
    alerts = []
    clock = {"t": 9_000.0}

    async def book():
        return {"OrdersResponse": {"Order": [
            _stop_payload("ODD-1", qty=60, stop=50),
        ]}}

    async def place(*args, **kwargs):
        raise main_bot.ETradeAPIError(400, "rejected", code="400")

    async def smart(kind, common, tokens):
        sold.append(common.get("orderAction"))
        return {"orderId": "MKT"}

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append(event)

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_exec())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", lambda *a, **k: _async_held())

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard.update({
            "done": False, "stop_client_id": "LOST", "stop_reply_lost": True,
            "stop_sent_ts": clock["t"] - 60, "lost_stop_qty": 100,
            "lost_stop_price": 90, "guarded_qty": 0,
            "stop_deadline_ts": clock["t"] + 60, "deadline_ts": 1_000_000.0,
            "entry_order_id": "E", "fill_detected_ts": 1,
        })
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        held = await store.get_guard("QQQ")
        return list(sold), list(alerts), held.get("stop_order_id"), held.get("result")

    fills, events, adopted, result = _run(scenario())
    assert fills == []
    assert "position_unprotected" in events
    assert adopted != "ODD-1"
    assert result != "filled_and_protected"


def test_stop_listing_after_the_sell_is_cancelled_not_adopted(monkeypatch):
    store = main_bot.StateStore(None)
    cancelled = []
    pages = []
    clock = {"t": 12_000.0}
    phase = {"confirmed": False}

    async def book():
        return {"OrdersResponse": {"Order": [_stop_payload("LOST-1")]}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        if phase["confirmed"]:
            return main_bot.CANCEL_CONFIRMED
        return main_bot.CANCEL_LIVE

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_exec())
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", lambda *a, **k: _async_held())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    main_bot._LOST_STOP_PAGERS.clear()

    async def once():
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        fresh = await store.get_guard("QQQ")
        return fresh.get("stop_order_id"), fresh.get("result"), len(pages)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": True,
            "lost_stop_cancel_pending": True,
            "lost_stop_watch_until": clock["t"] + 120,
            "lost_stop_qty": 100, "lost_stop_price": 90,
            "stop_sent_ts": 1_600_000_000, "guarded_qty": 0, "trued_qty": 100,
            "entry_order_id": "E", "fill_detected_ts": 1,
            "stop_deadline_ts": 1_000_000.0, "deadline_ts": 1_000_000.0,
            "last_status": "OPEN", "last_filled": 0,
        })
        first = await once()
        phase["confirmed"] = True
        clock["t"] += 30
        second = await once()
        third = await once()
        return first, second, third, list(cancelled)

    first, second, third, ids = _run(scenario())
    assert "LOST-1" in ids
    assert first[0] != "LOST-1"
    assert first[1] != "filled_and_protected"
    assert first[2] >= 1
    assert second[1] != "filled_and_protected"
    assert third[2] == second[2]


def test_x7_stop_lists_after_the_position_is_gone(monkeypatch):
    store = main_bot.StateStore(None)
    cancelled = []
    pages = []
    clock = {"t": 15_000.0}

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": {"Position": []}}}

    async def book():
        return {"OrdersResponse": {"Order": [_stop_payload("LOST-1")]}}

    async def cancel(order_id):
        cancelled.append(order_id)
        return True

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_open())
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", lambda *a, **k: _async_zero())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    main_bot._LOST_STOP_PAGERS.clear()

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "lost_stop_cancel_pending": True,
            "lost_stop_watch_until": clock["t"] - 150,
            "lost_stop_qty": 100, "lost_stop_price": 90,
            "stop_sent_ts": 1_600_000_000, "stop_client_id": "LOST",
            "deadline_ts": 1_000_000_000.0,
        })
        await main_bot._stop_guard_worker("QQQ")
        paged = len(pages)
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, book, cancel, alert=send_nowait,
        )
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        clock["t"] += 30
        await main_bot._stop_guard_worker("QQQ")
        return list(cancelled), paged, len(pages), bool(guard.get("lost_stop_cancel_done"))

    ids, before, after, done = _run(scenario())
    assert ids == ["LOST-1"]
    assert done is True
    assert before >= 1
    assert after == before


def _drive_3004(monkeypatch, store, order):
    sold = []
    cancelled = []
    clock = {"t": 8_000.0}
    live = {"on": True}

    async def place(*args, **kwargs):
        raise _etrade_3004()

    async def book():
        if not live["on"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": [order]}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        live["on"] = False
        return main_bot.CANCEL_CONFIRMED

    async def smart(kind, common, tokens):
        sold.append((clock["t"], str(common.get("orderAction") or ""), int(common.get("quantity") or 0)))
        return {"orderId": "MKT"}

    async def nap(seconds):
        clock["t"] += float(seconds)
        if sold or clock["t"] > 8_200:
            await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    async def order_state(order_id, client_id):
        if client_id or str(order_id or "") == "CLOSE-1":
            if live["on"]:
                return "OPEN", 0
            return "CANCELLED", 0
        return "EXECUTED", 100

    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    return sold, cancelled, clock


def test_hb1_3004_cancels_a_limit_close_and_sells_at_60s(monkeypatch):
    store = main_bot.StateStore(None)
    order = _stop_payload("CLOSE-1", price_type="LIMIT", stop=None)
    order["clientOrderId"] = "FLAT-CLIENT"
    sold, cancelled, clock = _drive_3004(monkeypatch, store, order)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": clock["t"] + 60,
            "deadline_ts": 1_000_000.0, "flatten_client_id": "FLAT-CLIENT",
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(cancelled), list(sold)

    ids, fills = _run(scenario())
    assert "CLOSE-1" in ids
    assert fills and fills[0][0] >= 8_060
    assert fills[0][1:] == ("SELL", 100)
    assert clock["t"] < 8_000 + 5_410


def test_hm_3004_does_not_sell_while_the_close_remains(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []
    clock = {"t": 8_000.0}

    async def place(*args, **kwargs):
        raise _etrade_3004()

    async def book():
        return {"OrdersResponse": {"Order": [{
            "orderId": "CLOSE-1",
            "clientOrderId": "FLAT-CLIENT",
            "OrderDetail": {
                "status": "OPEN", "priceType": "LIMIT",
                "Instrument": {
                    "orderAction": "SELL", "orderedQuantity": 100,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }]}}

    async def cancel(order_id, expected_qty=None):
        return main_bot.CANCEL_LIVE

    async def smart(*args, **kwargs):
        sold.append(clock["t"])
        return {"orderId": "MKT"}

    async def nap(seconds):
        clock["t"] += float(seconds)
        if clock["t"] > 8_200:
            await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_exec())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": clock["t"] + 60,
            "deadline_ts": 1_000_000.0, "flatten_order_id": "CLOSE-1",
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(sold)

    assert _run(scenario()) == []
    assert clock["t"] > 8_060


def test_panic_reuses_a_working_or_filled_sell(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []

    async def smart(kind, common, tokens):
        sold.append((str(common.get("orderAction") or ""), int(common.get("quantity") or 0)))
        return {"orderId": "NEW"}

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "load_tokens", _tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", lambda *a, **k: _async_held())

    async def working_book():
        return {"OrdersResponse": {"Order": [{
            "orderId": "SELL-1",
            "OrderDetail": {
                "status": "OPEN", "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL", "orderedQuantity": 100,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }]}}

    async def empty_book():
        return {"OrdersResponse": {}}

    async def filled_state(order_id, client_id):
        return "EXECUTED", 100

    async def open_state(order_id, client_id):
        return "OPEN", 0

    async def scenario(book, state_fn):
        sold.clear()
        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
        monkeypatch.setattr(main_bot, "_order_state", state_fn)
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "stop_lost_flatten_sent": True,
            "flatten_order_id": "SELL-1", "flatten_client_id": "FLAT",
        })
        await main_bot._flatten_book("panic")
        await main_bot._flatten_book("daily_loss")
        return list(sold)

    assert _run(scenario(working_book, open_state)) == []
    assert _run(scenario(empty_book, filled_state)) == []


def test_an_open_first_sell_is_not_followed_by_another(monkeypatch):
    store = main_bot.StateStore(None)
    calls = []
    alerts = []

    async def flatten(ticker, guard, qty, book="required"):
        calls.append(int(qty))
        return "MKT-1"

    async def order_state(order_id, client_id):
        return "OPEN", 0

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append(event)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)

    async def scenario():
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY",
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": False,
            "flatten_order_id": "MKT-1",
        })
        guard = await store.get_guard("QQQ")
        await main_bot._stock_lost_after_sell("QQQ", guard, 100, 100)
        return list(calls), list(alerts)

    sold, events = _run(scenario())
    assert sold == []
    assert "position_unprotected" in events


def test_a_90s_portfolio_hang_keeps_page_gaps_at_30s(monkeypatch):
    store = main_bot.StateStore(None)
    pages = []
    clock = {"t": 0.0}
    waiters = []
    timeouts = []
    original_sleep = asyncio.sleep

    async def nap(seconds):
        if float(seconds) == 0:
            await original_sleep(0)
            return
        fut = asyncio.get_running_loop().create_future()
        waiters.append((clock["t"] + float(seconds), fut))
        await fut

    async def wait_for(awaitable, timeout):
        timeouts.append(timeout)
        return await awaitable

    async def slow(*args, **kwargs):
        await asyncio.sleep(90)
        return {"PortfolioResponse": {}}

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_lost_stop_page_sleep", nap)
    monkeypatch.setattr(main_bot.asyncio, "wait_for", wait_for)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_portfolio", slow)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_open())
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", lambda: _async_empty())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", quiet)
    main_bot._LOST_STOP_PAGERS.clear()

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": True,
            "lost_stop_cancel_pending": True, "lost_stop_watch_until": 1,
            "fill_detected_ts": 1, "stop_deadline_ts": 1_000_000.0,
            "deadline_ts": 1_000_000.0, "guarded_qty": 100, "trued_qty": 100,
            "entry_order_id": "E", "last_status": "OPEN", "last_filled": 0,
        })
        task = asyncio.create_task(main_bot._stop_guard_worker("QQQ"))
        for _step in range(80):
            await original_sleep(0)
            await original_sleep(0)
            if not waiters:
                if task.done():
                    break
                continue
            waiters.sort(key=lambda item: item[0])
            wake, fut = waiters.pop(0)
            clock["t"] = wake
            if not fut.done():
                fut.set_result(None)
            if clock["t"] >= 90 and len(pages) >= 3:
                await store.update_guard("QQQ", done=True, lost_stop_cancel_done=True)
        if not task.done():
            await store.update_guard("QQQ", done=True, lost_stop_cancel_done=True)
            while waiters:
                _wake, fut = waiters.pop(0)
                if not fut.done():
                    fut.set_result(None)
            await original_sleep(0)
        await task
        return list(pages), list(timeouts)

    times, bounds = _run(scenario())
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert main_bot.PRE_CANCEL_STOP_READ_TIMEOUT_S in bounds
    assert len(times) >= 3
    assert gaps and max(gaps) <= 30


def test_a_failed_adopt_read_resets_the_clean_count(monkeypatch):
    store = main_bot.StateStore(None)
    clock = {"t": 11_000.0}

    async def book():
        raise RuntimeError("book down")

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_exec())
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "_place_protective_stop", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard.update({
            "done": False, "stop_reply_lost": True, "stop_client_id": "LOST",
            "stop_reply_clean_reads": 1, "stop_reply_clean_read_ts": clock["t"] - 10,
            "guarded_qty": 0, "entry_order_id": "E", "fill_detected_ts": 1,
            "stop_deadline_ts": 1_000_000.0, "deadline_ts": 1_000_000.0,
        })
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        fresh = await store.get_guard("QQQ")
        return int(fresh.get("stop_reply_clean_reads") or 0)

    assert _run(scenario()) == 0


async def _async_exec():
    return "EXECUTED", 100


async def _async_open():
    return "OPEN", 0


async def _async_held():
    return 100


async def _async_zero():
    return 0


async def _async_acct():
    return "acct"


async def _async_empty():
    return {"OrdersResponse": {}}
