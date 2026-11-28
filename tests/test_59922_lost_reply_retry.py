"""One lost-reply retry, and only after the original stop is provably gone."""
from __future__ import annotations

import asyncio

import main_bot
from test_59922_lost_stop_reply import _Gone, _equity_stop, _guard_for, _option_order


def _run(coro):
    return asyncio.run(coro)


def test_clean_read_latch():
    guard = {}
    first = main_bot.lost_reply_clean_read(guard, 1000.0, ok=True)
    assert first == {"count": 1, "ts": 1000.0, "ready": False}
    guard = {"stop_reply_clean_reads": 1, "stop_reply_clean_read_ts": 1000.0}
    early = main_bot.lost_reply_clean_read(guard, 1009.0, ok=True)
    assert early["ready"] is False
    assert early["count"] == 1
    ready = main_bot.lost_reply_clean_read(guard, 1010.0, ok=True)
    assert ready["ready"] is True
    assert ready["count"] == 2
    reset = main_bot.lost_reply_clean_read(
        {"stop_reply_clean_reads": 1, "stop_reply_clean_read_ts": 1000.0},
        1020.0, ok=False,
    )
    assert reset == {"count": 0, "ts": 0.0, "ready": False}
    used = main_bot.lost_reply_clean_read(
        {"stop_reply_retried": True, "stop_reply_clean_reads": 1,
         "stop_reply_clean_read_ts": 1000.0},
        1020.0, ok=True,
    )
    assert used["ready"] is False


def _install(monkeypatch, store, *, kind, qty, book, clearance=None):
    places = []
    sold = []
    alerts = []
    cancels = []
    clock = {"t": 10_000.0}
    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])

    async def place(*args, **kwargs):
        places.append(qty)
        if len(places) == 1:
            raise _Gone()
        return {"order_id": "RETRY", "client_id": "new", "qty": qty, "stop": 90.0}

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event))

    async def order_state(order_id, client_id):
        return "EXECUTED", qty

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def smart(kind_name, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT"}

    async def cancel(order_id, expected_qty=None):
        cancels.append(order_id)
        return main_bot.CANCEL_LIVE

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
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    if clearance is not None:
        async def cleared(guard):
            return clearance
        monkeypatch.setattr(main_bot, "_lost_stop_clearance", cleared)
    if kind == "option":
        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place)
    else:
        monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    return places, sold, alerts, cancels, clock


def _guard(kind, qty):
    guard = _guard_for("option" if kind == "option" else "equity", "CALL")
    guard["stop"] = 90.0 if kind == "equity" else 2.4
    guard["trued_qty"] = qty
    # Far deadline so the 80s bound does not fire during the retry window.
    guard["stop_deadline_ts"] = 1_000_000.0
    guard["fill_detected_ts"] = 1
    return guard


def _position(kind, qty):
    if kind == "option":
        return {
            "action": "BUY", "entry_premium": 3.0, "stop_premium": 2.4,
            "filled_qty": qty, "qty": qty,
            "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
        }
    return {
        "action": "BUY", "entry": 100.0, "stop": 90.0,
        "filled_qty": qty, "qty": qty,
    }


async def _poll(store, clock, step):
    guard = await store.get_guard("QQQ")
    guard["done"] = False
    await store.set_guard("QQQ", guard)
    await main_bot._stop_guard_worker("QQQ")
    clock["t"] += step


def test_retry_places_once_after_two_clean_reads_ten_seconds_apart(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, _cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book, clearance="clear",
    )

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", _guard("equity", 100))
        await _poll(store, clock, 0)       # lost reply
        await _poll(store, clock, 10)      # clean read 1
        await _poll(store, clock, 0)       # clean read 2, 10s later, retry
        await _poll(store, clock, 10)      # must not place again
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [100, 100]
    assert sold == []
    assert held.get("stop_order_id") == "RETRY"


def test_no_retry_after_one_read_or_a_short_gap_or_a_failed_read(monkeypatch):
    store = main_bot.StateStore(None)
    mode = {"fail": False}

    async def book():
        if mode["fail"]:
            mode["fail"] = False
            raise RuntimeError("book down")
        return {"OrdersResponse": {}}

    places, _sold, _alerts, _cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book, clearance="clear",
    )

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", _guard("equity", 100))
        await _poll(store, clock, 0)
        await _poll(store, clock, 5)       # one clean read, gap 0 then 5s
        assert places == [100]
        await _poll(store, clock, 0)       # still under 10s from the first counted read
        assert places == [100]
        mode["fail"] = True
        clock["t"] += 10
        await _poll(store, clock, 0)       # failed read resets
        clock["t"] += 10
        await _poll(store, clock, 0)       # one clean read after the reset
        return list(places)

    assert _run(scenario()) == [100]


def test_equity_book_lag_of_120s_does_not_double_the_stop(monkeypatch):
    """The original stop is invisible until 120s. No second stop and no sell."""
    store = main_bot.StateStore(None)
    clock_box = {}

    async def book():
        if clock_box["t"] < 10_000.0 + 120:
            return {"OrdersResponse": {}}
        guard = await store.get_guard("QQQ")
        return {"OrdersResponse": {"Order": _equity_stop(
            "LATE", 100, 90.0,
        ) | {"clientOrderId": guard.get("stop_client_id")}}}

    # _equity_stop doesn't take clientOrderId at the top level. Patch below.
    places, sold, alerts, _cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book,
    )
    clock_box["t"] = clock["t"]

    async def book2():
        if clock["t"] < 10_120.0:
            return {"OrdersResponse": {}}
        guard = await store.get_guard("QQQ")
        order = _equity_stop("LATE", 100, 90.0)
        order["clientOrderId"] = guard.get("stop_client_id")
        return {"OrdersResponse": {"Order": order}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book2)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", _guard("equity", 100))
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 0)          # retry would fire; cancel not confirmed
        clock["t"] = 10_120.0
        await _poll(store, clock, 0)          # the original stop is finally visible
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [100]
    assert sold == []
    assert held.get("stop_order_id") == "LATE"
    assert sum(places) + sum(sold) <= 100
    assert any(event == "position_unprotected" for _sev, event in alerts)


def test_option_book_lag_flattens_without_a_second_stop(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, _cancels, clock = _install(
        monkeypatch, store, kind="option", qty=2, book=book,
    )

    async def scenario():
        await store.set_position("QQQ", _position("option", 2))
        await store.set_guard("QQQ", _guard("option", 2))
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 0)
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [2]
    assert sold == [2]
    assert held.get("stop_order_id") != "RETRY"
    assert any(event == "position_unprotected" for _sev, event in alerts)


def test_stock_cancel_not_confirmed_pages_without_a_retry_or_a_sell(monkeypatch):
    store = main_bot.StateStore(None)

    async def place(*args, **kwargs):
        # The real installer place is replaced below so we can stamp an id.
        raise AssertionError("replaced")

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book,
    )

    async def place_lost(*args, **kwargs):
        places.append(100)
        guard = await store.get_guard("QQQ")
        guard["stop_order_id"] = "LOST-1"
        await store.set_guard("QQQ", guard)
        raise _Gone()

    monkeypatch.setattr(main_bot, "_place_protective_stop", place_lost)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", _guard("equity", 100))
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 0)
        return list(places), list(sold), list(cancels)

    seen, market, cancel_ids = _run(scenario())
    assert seen == [100]
    assert market == []
    assert "LOST-1" in cancel_ids
    assert any(event == "position_unprotected" for _sev, event in alerts)
