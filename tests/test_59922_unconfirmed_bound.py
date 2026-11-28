"""At the lost-reply bound, options are sold and stocks are only paged."""
from __future__ import annotations

import asyncio

import main_bot
from test_59922_lost_stop_reply import _Gone, _guard_for


def _run(coro):
    return asyncio.run(coro)


def _clock(monkeypatch, start=1_000.0):
    now = {"t": start}
    monkeypatch.setattr(main_bot.time, "time", lambda: now["t"])
    return now


def _install(monkeypatch, store, *, kind, book, qty):
    places = []
    sold = []
    alerts = []
    clock = _clock(monkeypatch)

    async def place(*args, **kwargs):
        places.append(qty)
        raise _Gone()

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event, clock["t"]))

    async def order_state(order_id, client_id):
        return "EXECUTED", qty

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def smart(kind_name, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT"}

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    if kind == "option":
        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    else:
        monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    return places, sold, alerts, clock


async def _drive(store, loops, clock, step=1.0):
    for _ in range(loops):
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        clock["t"] += step


def _arm(store, kind, qty):
    if kind == "option":
        pos = {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": qty, "qty": qty,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        }
        guard = _guard_for("option", "CALL")
    else:
        pos = {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": qty, "qty": qty,
        }
        guard = _guard_for("equity")
        guard["stop"] = 90.0
    guard["trued_qty"] = qty
    return pos, guard


def test_option_unreadable_book_sells_the_tracked_qty_at_the_bound(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        raise RuntimeError("book down")

    places, sold, alerts, clock = _install(
        monkeypatch, store, kind="option", book=book, qty=2,
    )

    async def scenario():
        pos, guard = _arm(store, "option", 2)
        await store.set_position("QQQ", pos)
        await store.set_guard("QQQ", guard)
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS, clock)
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [2]
    assert sold == [2]
    assert ("critical", "position_unprotected", 1000.0) in [
        (a[0], a[1], a[2]) for a in alerts
    ] or any(event == "position_unprotected" for _sev, event, _t in alerts)
    assert held.get("done") is True


def test_stock_unreadable_book_pages_and_does_not_sell(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        raise RuntimeError("book down")

    places, sold, alerts, clock = _install(
        monkeypatch, store, kind="equity", book=book, qty=100,
    )

    async def scenario():
        pos, guard = _arm(store, "equity", 100)
        await store.set_position("QQQ", pos)
        await store.set_guard("QQQ", guard)
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS, clock)
        first = [row for row in alerts if row[1] == "position_unprotected"]
        clock["t"] += main_bot.UNCONFIRMED_STOP_PAGE_S
        await _drive(store, 1, clock, step=0)
        second = [row for row in alerts if row[1] == "position_unprotected"]
        return first, second

    first, second = _run(scenario())
    assert places == [100]
    assert sold == []
    assert len(first) == 1
    assert len(second) == 2


def test_stock_empty_book_at_the_bound_still_does_not_sell(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, clock = _install(
        monkeypatch, store, kind="equity", book=book, qty=100,
    )

    async def scenario():
        pos, guard = _arm(store, "equity", 100)
        await store.set_position("QQQ", pos)
        await store.set_guard("QQQ", guard)
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS, clock)
        return [row for row in alerts if row[1] == "position_unprotected"]

    pages = _run(scenario())
    assert places == [100]
    assert sold == []
    assert len(pages) == 1


def test_option_empty_book_at_the_bound_sells_the_tracked_qty(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, clock = _install(
        monkeypatch, store, kind="option", book=book, qty=2,
    )

    async def scenario():
        pos, guard = _arm(store, "option", 2)
        await store.set_position("QQQ", pos)
        await store.set_guard("QQQ", guard)
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS, clock)

    _run(scenario())
    assert places == [2]
    assert sold == [2]
    assert any(event == "position_unprotected" for _sev, event, _t in alerts)
