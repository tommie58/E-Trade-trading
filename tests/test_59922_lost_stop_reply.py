"""A lost stop reply does not place again until the book shows the order
is gone. A 3004 or an unreadable book holds only through the stop deadline
plus STOP_ALLOCATION_HELD_POLLS, then the normal unprotected path runs.
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
        # The empty book has not shown the lost order, so there is no
        # second place inside this window.
        assert len(places) == 1
        assert places[0][0] == right
        assert cancels == []
        # Inside the hold the emergency flatten has not run. The hold is
        # not permanent: test_hold_expires_without_adoption covers the end.
        assert flats == []
        held = await store.get_guard("QQQ")
        assert held.get("stop_allocation_held") is True
        assert int(held.get("stop_allocation_held_polls") or 0) < main_bot.STOP_ALLOCATION_HELD_POLLS

    for right in ("CALL", "PUT"):
        _run(scenario(right))


def test_hold_counts_polls_only_after_the_stop_deadline():
    early = main_bot.allocation_hold_after_poll(
        {"stop_deadline_ts": 1000, "stop_allocation_held_polls": 0}, 999,
    )
    assert early == {"polls": 0, "expired": False}
    polls = 0
    expired = False
    for _ in range(main_bot.STOP_ALLOCATION_HELD_POLLS):
        step = main_bot.allocation_hold_after_poll(
            {"stop_deadline_ts": 1000, "stop_allocation_held_polls": polls}, 1000,
        )
        polls = step["polls"]
        expired = step["expired"]
    assert polls == main_bot.STOP_ALLOCATION_HELD_POLLS
    assert expired is True


def _option_order(order_id, price_type, qty, right="CALL", stop=2.4):
    detail = {
        "status": "OPEN",
        "priceType": price_type,
        "Instrument": {
            "orderAction": "SELL_CLOSE",
            "orderedQuantity": qty,
            "filledQuantity": 0,
            "Product": {
                "symbol": "QQQ",
                "securityType": "OPTN",
                "callPut": right,
                "strikePrice": 450,
                "expiryYear": 2026,
                "expiryMonth": 12,
                "expiryDay": 18,
            },
        },
    }
    if price_type == "STOP":
        detail["stopPrice"] = stop
    else:
        detail["limitPrice"] = 3.0
    return {"orderId": order_id, "OrderDetail": detail}


def _equity_stop(order_id, qty, stop):
    return {
        "orderId": order_id,
        "OrderDetail": {
            "status": "OPEN",
            "priceType": "STOP",
            "stopPrice": stop,
            "Instrument": {
                "orderAction": "SELL",
                "orderedQuantity": qty,
                "filledQuantity": 0,
                "Product": {"symbol": "QQQ", "securityType": "EQ"},
            },
        },
    }


def _guard_for(kind, right="CALL"):
    guard = {
        "done": False,
        "kind": kind,
        "action": "BUY",
        "stop": 2.4,
        "guarded_qty": 0,
        "trued_qty": 1,
        "entry_order_id": "ENTRY-1",
        "deadline_ts": 1,
        "stop_deadline_ts": 1,
        # Already anchored, so the worker does not push the stop deadline
        # out by another STOP_PLACEMENT_TIMEOUT_SECONDS.
        "fill_detected_ts": 1,
    }
    if kind == "option":
        guard["contract"] = {"right": right, "strike": 450.0, "expiration": "2026-12-18"}
    return guard


async def _drive(store, loops):
    for _ in range(loops):
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")


def test_hold_expires_without_adoption_and_flattens(monkeypatch):
    """(a) The book never shows a covering stop. The critical fires and the
    deadline emergency flatten runs."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    places = []
    flats = []
    alerts = []

    async def place(ticker, contract, qty, premium):
        places.append(1)
        if len(places) == 1:
            raise _Gone()
        raise _Allocated()

    async def flatten(*args, **kwargs):
        flats.append(args[0] if args else None)
        return "FLAT"

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 1

    async def book():
        return {"OrdersResponse": {}}

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def no_fill(*args, **kwargs):
        return None

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", no_fill)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": 1, "qty": 1,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", _guard_for("option"))
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS)
        held = await store.get_guard("QQQ")
        return held

    held = _run(scenario())
    assert ("critical", "position_unprotected") in alerts
    assert flats == ["QQQ"]
    assert held.get("stop_allocation_held") is not True
    assert len(places) == 1


def test_adoption_inside_the_bound_does_not_flatten(monkeypatch):
    """(b) A covering stop shows up before the hold expires."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    places = []
    flats = []
    alerts = []
    phase = {"show": False}

    async def place(ticker, contract, qty, premium):
        places.append(1)
        phase["show"] = True
        raise _Gone()

    async def flatten(*args, **kwargs):
        flats.append("flat")
        return "FLAT"

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 1

    async def book():
        if not phase["show"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": _option_order(
            "LIVE-STOP", "STOP", 1, right="PUT",
        )}}

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": 1, "qty": 1,
            "contract": {"right": "PUT", "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", _guard_for("option", "PUT"))
        # Lost reply, then the next poll adopts the stop already on the book.
        await _drive(store, 2)
        return await store.get_guard("QQQ"), await store.get_position("QQQ")

    guard, pos = _run(scenario())
    assert len(places) == 1
    assert flats == []
    assert ("critical", "position_unprotected") not in alerts
    assert guard.get("stop_order_id") == "LIVE-STOP"
    assert guard.get("stop_allocation_held") is not True
    assert guard.get("stop_reply_lost") is not True
    assert pos.get("stop_order_id") == "LIVE-STOP"


def test_working_limit_close_is_not_adopted_as_the_stop(monkeypatch):
    """(c) A 3004 from a working close/limit is not a covering stop."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    places = []
    flats = []
    alerts = []

    async def place(ticker, contract, qty, premium):
        places.append(1)
        raise _Allocated()

    async def flatten(*args, **kwargs):
        flats.append("flat")
        return "FLAT"

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 1

    async def book():
        return {"OrdersResponse": {"Order": _option_order("LIMIT-1", "LIMIT", 1)}}

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def no_fill(*args, **kwargs):
        return None

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", no_fill)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": 1, "qty": 1,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", _guard_for("option"))
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS)
        return await store.get_guard("QQQ")

    guard = _run(scenario())
    assert len(places) == 1
    assert guard.get("stop_order_id") != "LIMIT-1"
    assert ("critical", "position_unprotected") in alerts
    assert flats == ["flat"]
    assert guard.get("stop_allocation_held") is not True


def test_equity_lost_reply_adopts_the_book_instead_of_replacing(monkeypatch):
    """Before the retry place, an open equity stop on the book is adopted."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    places = []
    flats = []
    alerts = []
    phase = {"armed": False}

    async def place(ticker, action, qty, stop):
        places.append((action, qty, stop))
        phase["armed"] = True
        raise _Gone()

    async def flatten(*args, **kwargs):
        flats.append("flat")
        return "FLAT"

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", 10

    async def book():
        if not phase["armed"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": _equity_stop("EQ-STOP", 10, 90.0)}}

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 10, "qty": 10,
        })
        guard = _guard_for("equity")
        guard["stop"] = 90.0
        guard["trued_qty"] = 10
        await store.set_guard("QQQ", guard)
        await _drive(store, 2)
        return await store.get_guard("QQQ"), await store.get_position("QQQ")

    guard, pos = _run(scenario())
    assert len(places) == 1
    assert flats == []
    assert ("critical", "position_unprotected") not in alerts
    assert guard.get("stop_order_id") == "EQ-STOP"
    assert guard.get("stop_reply_lost") is not True
    assert pos.get("stop_order_id") == "EQ-STOP"


def _wire_guard(monkeypatch, store, *, place, book, flatten=None, order_qty=1):
    alerts = []
    sold = []

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", order_qty

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def smart(kind, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT"}

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    if flatten is not None:
        monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    return alerts, sold


def test_equity_lost_reply_failed_read_holds_then_pages(monkeypatch):
    """A failed book read after a lost equity reply does not place again."""
    store = main_bot.StateStore(None)
    places = []

    async def place(ticker, action, qty, stop):
        places.append(qty)
        raise _Gone()

    async def book():
        raise RuntimeError("book down")

    alerts, sold = _wire_guard(monkeypatch, store, place=place, book=book, order_qty=100)
    monkeypatch.setattr(main_bot, "_place_protective_stop", place)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        guard = _guard_for("equity")
        guard["stop"] = 90.0
        guard["trued_qty"] = 100
        await store.set_guard("QQQ", guard)
        await _drive(store, 2 + main_bot.STOP_ALLOCATION_HELD_POLLS)
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [100]
    assert sold == []
    assert ("critical", "position_unprotected") in alerts
    assert held.get("stop_order_id") in (None, "")


def test_equity_lost_reply_lag_adopts_on_the_third_poll(monkeypatch):
    """Two empty polls do not place. The stop that appears on poll 3 is adopted."""
    store = main_bot.StateStore(None)
    places = []
    reads = {"n": 0}

    async def place(ticker, action, qty, stop):
        places.append(qty)
        raise _Gone()

    async def book():
        reads["n"] += 1
        if reads["n"] < 3:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": _equity_stop("EQ-STOP", 100, 90.0)}}

    async def flatten(*args, **kwargs):
        raise AssertionError("flatten ran while the lost stop was still unresolved")

    alerts, sold = _wire_guard(
        monkeypatch, store, place=place, book=book, flatten=flatten, order_qty=100,
    )
    monkeypatch.setattr(main_bot, "_place_protective_stop", place)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        guard = _guard_for("equity")
        guard["stop"] = 90.0
        guard["trued_qty"] = 100
        await store.set_guard("QQQ", guard)
        await _drive(store, 4)
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [100]
    assert sold == []
    assert held.get("stop_order_id") == "EQ-STOP"
    assert ("critical", "position_unprotected") not in alerts
    assert reads["n"] >= 3


def test_option_lost_reply_failed_read_does_not_place(monkeypatch):
    store = main_bot.StateStore(None)
    places = []
    flats = []

    async def place(ticker, contract, qty, premium):
        places.append((contract.get("right"), qty))
        raise _Gone()

    async def book():
        raise RuntimeError("book down")

    async def flatten(*args, **kwargs):
        flats.append(args[0] if args else None)
        return "FLAT"

    _wire_guard(monkeypatch, store, place=place, book=book, flatten=flatten)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)

    async def scenario(right):
        places.clear()
        flats.clear()
        await store.set_position("QQQ", {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": 1, "qty": 1,
            "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", _guard_for("option", right))
        await _drive(store, 1 + main_bot.STOP_ALLOCATION_HELD_POLLS)
        return places, flats

    for right in ("CALL", "PUT"):
        seen, flats = _run(scenario(right))
        assert seen == [(right, 1)]
        assert flats == ["QQQ"]


def test_equity_flatten_does_not_sell_more_than_the_long(monkeypatch):
    """A working 100-share stop is cancelled first. A failed cancel sells nothing."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    sold = []
    cancelled = []

    async def book():
        return {"OrdersResponse": {"Order": _equity_stop("EQ-STOP", 100, 90.0)}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append((order_id, expected_qty))
        return main_bot.CANCEL_LIVE

    async def smart(kind, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT"}

    async def send_nowait(*args, **kwargs):
        return None

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)

    async def scenario():
        await store.set_guard("QQQ", _guard_for("equity"))
        try:
            await main_bot._emergency_flatten("QQQ", await store.get_guard("QQQ"), 100)
        except Exception as exc:
            return type(exc).__name__, str(exc)
        return "placed", ""

    kind, text = _run(scenario())
    assert sold == []
    assert cancelled == [("EQ-STOP", 100)]
    assert text == "covered"

    async def cancel_ok(order_id, expected_qty=None):
        cancelled.append((order_id, expected_qty))
        return main_bot.CANCEL_CONFIRMED

    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel_ok)

    async def confirmed():
        cancelled.clear()
        await main_bot._emergency_flatten("QQQ", await store.get_guard("QQQ"), 100)
        return list(sold)

    assert _run(confirmed()) == [100]
    assert sum(sold) <= 100
