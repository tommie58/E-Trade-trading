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
        monkeypatch, store, kind="option", qty=2, book=book, clearance="clear",
    )

    async def scenario():
        await store.set_position("QQQ", _position("option", 2))
        await store.set_guard("QQQ", _guard("option", 2))
        await _poll(store, clock, 0)       # lost reply
        await _poll(store, clock, 10)      # clean read 1
        await _poll(store, clock, 0)       # clean read 2, 10s later, retry
        await _poll(store, clock, 10)      # must not place again
        return await store.get_guard("QQQ")

    held = _run(scenario())
    assert places == [2, 2]
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


def test_stock_never_listed_lost_stop_does_not_retry_or_rearm(monkeypatch):
    """20s of empty book: one lost place only, and the reconciler does not re-arm."""
    store = main_bot.StateStore(None)
    rearms = []

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, _cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book,
    )

    async def scenario():
        pos = _position("equity", 100)
        guard = _guard("equity", 100)
        guard["deadline_ts"] = 1
        await store.set_position("QQQ", pos)
        await store.set_guard("QQQ", guard)
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 10)
        held = await store.get_guard("QQQ")
        async def rearm(ticker, position):
            rearms.append(int(position.get("filled_qty") or position.get("qty") or 0))
            return "placed"
        async def portfolio():
            return {"PortfolioResponse": {"AccountPortfolio": {"Position": [{
                "symbolDescription": "QQQ",
                "quantity": 100,
                "Product": {"symbol": "QQQ", "securityType": "EQ"},
            }]}}}
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, book, lambda order_id: True,
            rearm_guard=rearm, alert=lambda *a, **k: None,
        )
        return held

    held = _run(scenario())
    assert places == [100]
    assert sold == []
    assert rearms == []
    assert held.get("stop_reply_lost") is True
    assert any(event == "position_unprotected" for _sev, event in alerts)


def test_option_book_lag_retries_once_instead_of_flattening(monkeypatch):
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
    assert places == [2, 2]
    assert sold == []
    assert held.get("stop_order_id") == "RETRY"


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


def test_stock_clean_read_path_keeps_paging_every_30s(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, _cancels, clock = _install(
        monkeypatch, store, kind="equity", qty=100, book=book,
    )

    async def place_fail(*args, **kwargs):
        places.append(100)
        raise _Gone()

    monkeypatch.setattr(main_bot, "_place_protective_stop", place_fail)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", _guard("equity", 100))
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 0)  # retry also lost
        first = [row for row in alerts if row[1] == "position_unprotected"]
        clock["t"] += main_bot.UNCONFIRMED_STOP_PAGE_S
        await _poll(store, clock, 0)
        second = [row for row in alerts if row[1] == "position_unprotected"]
        return first, second

    first, second = _run(scenario())
    assert places == [100]
    assert sold == []
    assert len(first) >= 1
    assert len(second) == len(first) + 1


def test_option_replace_reject_clears_the_stale_reply_bound(monkeypatch):
    """h4: lost reply, accepted retry, fill growth, then a 4xx replace.

    The stale bound and the used retry must clear with the dead stop id,
    so the option flattens once the stop deadline arrives.
    """
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, sold, alerts, cancels, clock = _install(
        monkeypatch, store, kind="option", qty=2, book=book,
    )

    async def reject(*args, **kwargs):
        places.append(2)
        raise main_bot.ETradeAPIError(400, "rejected")

    async def capture(_order_id):
        return None

    async def cancel(order_id, expected_qty=None):
        cancels.append(order_id)
        return main_bot.CANCEL_CONFIRMED

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", reject)
    monkeypatch.setattr(main_bot, "_capture_resting_stop", capture)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)

    async def scenario():
        await store.set_position("QQQ", _position("option", 2))
        guard = _guard("option", 2)
        guard.update({
            "guarded_qty": 1,
            "trued_qty": 2,
            "stop_order_id": "RETRY",
            "stop_client_id": "LOST-CLIENT",
            "stop_reply_retried": True,
            "stop_reply_bound": True,
            "stop_reply_lost": False,
            "stop_deadline_ts": clock["t"] + 20,
            "fill_detected_ts": 1,
        })
        await store.set_guard("QQQ", guard)
        await _poll(store, clock, 20)
        mid = await store.get_guard("QQQ")
        await _poll(store, clock, 0)
        return mid, list(sold), list(alerts)

    mid, sold_qty, sent = _run(scenario())
    assert mid.get("stop_order_id") in (None, "")
    assert mid.get("stop_reply_bound") is False
    assert mid.get("stop_reply_retried") is False
    assert sold_qty == [2]
    assert ("critical", "stop_timeout_flattened") in sent

