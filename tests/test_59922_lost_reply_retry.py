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
    armed = []
    monkeypatch.setattr(
        main_bot, "_arm_option_surplus_watch", lambda ticker: armed.append(ticker),
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
    assert sold == [100]
    assert rearms == []
    assert armed == []
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
    assert sold == [100]
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


def _option_stop(order_id):
    return {
        "order_id": order_id,
        "symbol": "QQQ",
        "status": "OPEN",
        "price_type": "STOP",
        "order_action": "SELL_CLOSE",
        "stop_price": 2.4,
        "security_type": "OPTN",
        "call_put": "CALL",
        "strike": 450.0,
        "expiration": "2026-12-18",
        "ordered": 2,
        "filled": 0,
    }


def test_option_surplus_cancels_the_extra_stop_at_10s_and_on_a_later_listing(monkeypatch):
    store = main_bot.StateStore(None)
    cancelled = []
    listed = {"rows": []}

    async def fetch():
        return {"OrdersResponse": {}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        listed["rows"] = [
            row for row in listed["rows"] if row["order_id"] != order_id
        ]
        return main_bot.CANCEL_CONFIRMED

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(
        main_bot.reconciliation, "parse_open_orders", lambda _resp: list(listed["rows"]),
    )

    async def scenario():
        await store.set_position("QQQ", {
            **_position("option", 2),
            "stop_order_id": "RETRY",
        })
        # The +10s pass: the original stop has listed beside the retry.
        listed["rows"] = [_option_stop("ORIGINAL"), _option_stop("RETRY")]
        await main_bot._option_surplus_once("QQQ")
        after_first = list(cancelled)
        # The retry id is not on the book, so the original stays.
        listed["rows"] = [_option_stop("ORIGINAL")]
        await main_bot._option_surplus_once("QQQ")
        missed = list(cancelled)
        # A close and a take-profit limit are not surplus stops.
        listed["rows"] = [
            _option_stop("ORIGINAL"),
            _option_stop("RETRY"),
            {
                **_option_stop("TRAIL"),
                "price_type": "TRAILING_STOP_CNST",
                "stop_price": None,
                "offset_value": 0.2,
            },
            {**_option_stop("CLOSE"), "price_type": "LIMIT", "stop_price": None},
            {**_option_stop("TARGET"), "price_type": "LIMIT", "stop_price": None},
        ]
        await main_bot._option_surplus_once("QQQ")
        return after_first, missed, list(cancelled)

    first, missed, later = _run(scenario())
    assert first == ["ORIGINAL"]
    assert missed == ["ORIGINAL"]
    assert later == ["ORIGINAL", "ORIGINAL", "TRAIL"]
    assert "CLOSE" not in later and "TARGET" not in later
    assert main_bot._OPTION_SURPLUS_DELAYS == (10.0, 30.0)


def test_option_retry_schedules_the_10s_and_30s_surplus_checks(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    places, _sold, _alerts, _cancels, clock = _install(
        monkeypatch, store, kind="option", qty=2, book=book, clearance="clear",
    )
    delays = []
    real_create = main_bot.asyncio.create_task

    def spy(coro):
        if getattr(getattr(coro, "cr_code", None), "co_name", "") == "_option_surplus_after":
            delays.append(coro.cr_frame.f_locals.get("delay"))
            coro.close()
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            return fut
        return real_create(coro)

    monkeypatch.setattr(main_bot.asyncio, "create_task", spy)

    async def scenario():
        await store.set_position("QQQ", _position("option", 2))
        await store.set_guard("QQQ", _guard("option", 2))
        await _poll(store, clock, 0)
        await _poll(store, clock, 10)
        await _poll(store, clock, 0)
        return list(places)

    assert _run(scenario()) == [2, 2]
    assert delays == [10.0, 30.0]


def test_stock_lost_reply_pages_every_30s_across_5400s(monkeypatch):
    """One worker invocation keeps paging for 5,400s. It does not return after the first page."""
    store = main_bot.StateStore(None)
    pages = []
    places = []
    clock = {"t": 10_000.0}
    end = clock["t"] + 5_400.0
    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def order_state(order_id, client_id):
        return "EXECUTED", 100

    async def nap(seconds):
        clock["t"] += float(seconds)
        if clock["t"] >= end:
            await store.update_guard("QQQ", done=True)

    async def flatten(*args, **kwargs):
        return main_bot.NOT_SOLD

    async def quiet(*args, **kwargs):
        return None

    async def book():
        return {"OrdersResponse": {}}

    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)

    async def place(*args, **kwargs):
        places.append(1)
        return {"order_id": "NO", "client_id": "no", "qty": 100, "stop": 90.0}

    monkeypatch.setattr(main_bot, "_place_protective_stop", place)

    async def scenario():
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 100, "trued_qty": 100,
            "fill_detected_ts": 1, "stop_deadline_ts": 1,
            "deadline_ts": end + 10_000,
            "entry_order_id": "ENTRY-1",
            "stop_client_id": "LOST-CLIENT",
            "stop_reply_bound": True,
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(pages), await store.get_guard("QQQ")

    times, guard = _run(scenario())
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert places == []
    assert guard.get("result") != "stop_timeout_flattened"
    assert len(times) >= 180
    assert times[-1] - times[0] >= 5_400 - 30
    assert gaps and max(gaps) <= 30


def test_reconciler_pages_a_lost_stock_instead_of_skipping(monkeypatch):
    store = main_bot.StateStore(None)
    rearms = []
    pages = []
    monkeypatch.setattr(main_bot, "state", store)

    async def rearm(ticker, position):
        rearms.append(ticker)
        return "placed"

    async def alert(severity, event, message, **kwargs):
        pages.append((severity, event))

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": {"Position": [{
            "symbolDescription": "QQQ",
            "quantity": 100,
            "Product": {"symbol": "QQQ", "securityType": "EQ"},
        }]}}}

    async def book():
        return {"OrdersResponse": {}}

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard["deadline_ts"] = 1_000_000_000.0
        guard["stop_client_id"] = "LOST-CLIENT"
        guard["stop_reply_bound"] = True
        guard["stop_reply_lost"] = True
        guard["stop_order_id"] = None
        await store.set_guard("QQQ", guard)
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, book, lambda order_id: True,
            rearm_guard=rearm, alert=alert,
        )

    _run(scenario())
    assert rearms == []
    assert ("critical", "position_unprotected") in pages


def _lost_stock(monkeypatch, store, *, held, book):
    orders = []
    cancels = []
    alerts = []
    clock = {"t": 10_000.0}
    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)

    async def place(*args, **kwargs):
        raise _Gone()

    async def send_nowait(severity, event, message, **kwargs):
        alerts.append((severity, event, clock["t"]))

    async def order_state(order_id, client_id):
        return "EXECUTED", 100

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def smart(kind_name, common, tokens):
        orders.append((str(common.get("orderAction") or ""), int(common.get("quantity") or 0)))
        return {"orderId": "MKT-1"}

    async def cancel(order_id, expected_qty=None):
        cancels.append(order_id)
        return main_bot.CANCEL_CONFIRMED

    async def held_qty(ticker, pos):
        value = held["n"]
        if isinstance(value, list):
            return value.pop(0)
        return value

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    monkeypatch.setattr(main_bot, "_place_protective_stop", place)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held_qty)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    return orders, cancels, alerts, clock


async def _two_clean_reads(store, clock):
    await store.set_position("QQQ", _position("equity", 100))
    guard = _guard("equity", 100)
    guard["stop_client_id"] = "LOST-CLIENT"
    guard["stop_reply_lost"] = True
    guard["stop_deadline_ts"] = 1_000_000.0
    guard["deadline_ts"] = 1_000_000.0
    await store.set_guard("QQQ", guard)
    for step in (0, 10, 0):
        held = await store.get_guard("QQQ")
        held["done"] = False
        await store.set_guard("QQQ", held)
        await main_bot._stop_guard_worker("QQQ")
        clock["t"] += step


def test_two_clean_reads_sell_the_held_quantity_once(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    orders, _cancels, alerts, clock = _lost_stock(
        monkeypatch, store, held={"n": 40}, book=book,
    )

    async def scenario():
        await _two_clean_reads(store, clock)
        clock["t"] += 30
        held = await store.get_guard("QQQ")
        held["done"] = False
        await store.set_guard("QQQ", held)
        await main_bot._stop_guard_worker("QQQ")
        return list(orders), [row[1] for row in alerts]

    sold, events = _run(scenario())
    assert sold == [("SELL", 40)]
    assert "lost_stop_may_be_live" in events


def test_unreadable_holdings_sell_the_tracked_quantity(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    orders, _cancels, _alerts, clock = _lost_stock(
        monkeypatch, store, held={"n": None}, book=book,
    )

    async def scenario():
        await _two_clean_reads(store, clock)
        return list(orders)

    assert _run(scenario()) == [("SELL", 100)]


def test_late_lost_stop_is_cancelled_by_client_id_and_the_flatten_is_not(monkeypatch):
    store = main_bot.StateStore(None)
    phase = {"listed": False}

    def _row(order_id, client_id):
        return {
            "orderId": order_id,
            "clientOrderId": client_id,
            "OrderDetail": {
                "status": "OPEN",
                "priceType": "STOP",
                "stopPrice": 90,
                "Instrument": {
                    "orderAction": "SELL",
                    "orderedQuantity": 100,
                    "filledQuantity": 0,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }

    async def book():
        if not phase["listed"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": [
            _row("LOST-1", "LOST-CLIENT"),
            _row("MKT-1", "FLAT-CLIENT"),
        ]}}

    orders, cancels, _alerts, clock = _lost_stock(
        monkeypatch, store, held={"n": 100}, book=book,
    )

    async def scenario():
        await _two_clean_reads(store, clock)
        phase["listed"] = True
        guard = await store.get_guard("QQQ")
        guard["flatten_order_id"] = "MKT-1"
        guard["flatten_client_id"] = "FLAT-CLIENT"
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        return list(orders), list(cancels)

    sold, cancelled = _run(scenario())
    assert sold == [("SELL", 100)]
    assert cancelled == ["LOST-1"]


def test_a_short_after_the_lost_stop_fills_is_paged_and_never_bought(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    orders, _cancels, alerts, clock = _lost_stock(
        monkeypatch, store, held={"n": [100, -100, -100, -100]}, book=book,
    )

    async def scenario():
        await _two_clean_reads(store, clock)
        first = [row for row in alerts if row[1] == "position_unprotected"]
        clock["t"] += 30
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        second = [row for row in alerts if row[1] == "position_unprotected"]
        return list(orders), first, second

    sold, first, second = _run(scenario())
    assert sold == [("SELL", 100)]
    assert not any(action.startswith("BUY") for action, _qty in sold)
    assert len(first) >= 1
    assert len(second) == len(first) + 1


def test_a_failed_book_read_stays_page_only(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        raise RuntimeError("book down")

    orders, _cancels, alerts, clock = _lost_stock(
        monkeypatch, store, held={"n": 100}, book=book,
    )

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard.update({
            "stop_client_id": "LOST-CLIENT",
            "stop_reply_lost": True,
            "stop_reply_bound": True,
            "stop_deadline_ts": 1_000_000.0,
            "deadline_ts": 1_000_000.0,
        })
        await store.set_guard("QQQ", guard)
        for step in (0, 10, 10):
            held = await store.get_guard("QQQ")
            held["done"] = False
            await store.set_guard("QQQ", held)
            await main_bot._stop_guard_worker("QQQ")
            clock["t"] += step
        return list(orders), [row[1] for row in alerts]

    sold, events = _run(scenario())
    assert sold == []
    assert "position_unprotected" in events


def test_a_lost_reply_on_the_flatten_sell_is_not_resent(monkeypatch):
    store = main_bot.StateStore(None)
    calls = []

    async def book():
        return {"OrdersResponse": {}}

    async def flatten(ticker, guard, qty, book="required"):
        calls.append(int(qty))
        return main_bot.NOT_SOLD

    _lost_stock(monkeypatch, store, held={"n": 100}, book=book)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard["stop_client_id"] = "LOST-CLIENT"
        await store.set_guard("QQQ", guard)
        await main_bot._stock_lost_reply_flatten_once("QQQ", guard, 100)
        again = await store.get_guard("QQQ")
        await main_bot._stock_lost_reply_flatten_once("QQQ", again, 100)
        return list(calls)

    assert _run(scenario()) == [100]

