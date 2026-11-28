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

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": []}}

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_portfolio", portfolio)
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
        await _poll(store, clock, 60)
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
        await _poll(store, clock, 60)
        await _poll(store, clock, 0)  # second clean read, past 60s
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
    guard["stop_sent_ts"] = clock["t"] - 60
    guard["lost_stop_qty"] = 100
    guard["lost_stop_price"] = 90
    await store.set_guard("QQQ", guard)
    for step in (0, 60, 0):
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

    def _row(order_id, price_type):
        detail = {
            "status": "OPEN",
            "priceType": price_type,
            "placedTime": 1_700_000_000_000,
            "Instrument": {
                "orderAction": "SELL",
                "orderedQuantity": 100,
                "filledQuantity": 0,
                "Product": {"symbol": "QQQ", "securityType": "EQ"},
            },
        }
        if price_type == "STOP":
            detail["stopPrice"] = 90
        return {"orderId": order_id, "OrderDetail": detail}

    async def book():
        if not phase["listed"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": [
            _row("LOST-1", "STOP"),
            _row("MKT-1", "MARKET"),
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


def test_readable_holding_sells_min_of_held_and_tracked(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    orders, _c, _a, clock = _lost_stock(
        monkeypatch, store, held={"n": 200}, book=book,
    )

    async def scenario():
        await _two_clean_reads(store, clock)
        return list(orders)

    assert _run(scenario()) == [("SELL", 100)]


def test_unreadable_holding_sells_tracked_once_and_not_again(monkeypatch):
    store = main_bot.StateStore(None)
    calls = []

    async def book():
        return {"OrdersResponse": {}}

    async def flatten(ticker, guard, qty, book="required"):
        calls.append(int(qty))
        return "MKT-1"

    _lost_stock(monkeypatch, store, held={"n": None}, book=book)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard["stop_client_id"] = "LOST-CLIENT"
        await store.set_guard("QQQ", guard)
        await main_bot._stock_lost_reply_flatten_once("QQQ", guard, 100)
        fresh = await store.get_guard("QQQ")
        await main_bot._stock_lost_after_sell("QQQ", fresh, None, 100)
        return list(calls)

    assert _run(scenario()) == [100]


def test_later_readable_holding_sells_min_once(monkeypatch):
    store = main_bot.StateStore(None)
    calls = []

    async def book():
        return {"OrdersResponse": {}}

    async def flatten(ticker, guard, qty, book="required"):
        calls.append(int(qty))
        return "MKT-1"

    _lost_stock(monkeypatch, store, held={"n": [None, None, 80]}, book=book)
    monkeypatch.setattr(main_bot, "_emergency_flatten", flatten)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard["stop_client_id"] = "LOST-CLIENT"
        await store.set_guard("QQQ", guard)
        await main_bot._stock_lost_reply_flatten_once("QQQ", guard, 100)
        fresh = await store.get_guard("QQQ")
        await main_bot._stock_lost_after_sell("QQQ", fresh, 80, 100)
        fresh = await store.get_guard("QQQ")
        await main_bot._stock_lost_after_sell("QQQ", fresh, 80, 100)
        return list(calls)

    assert _run(scenario()) == [100, 80]


def test_slow_holdings_read_keeps_page_gaps_at_30s(monkeypatch):
    store = main_bot.StateStore(None)
    pages = []
    clock = {"t": 10_000.0}
    real_wait = main_bot.asyncio.wait_for
    timeouts = []

    async def wait_for(awaitable, timeout):
        timeouts.append(timeout)
        if timeout == main_bot.PRE_CANCEL_STOP_READ_TIMEOUT_S:
            awaitable.close()
            raise asyncio.TimeoutError()
        return await real_wait(awaitable, timeout)

    async def book():
        return {"OrdersResponse": {}}

    async def nap(seconds):
        clock["t"] += float(seconds)
        if clock["t"] >= 10_090:
            await store.update_guard("QQQ", done=True)

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot.asyncio, "wait_for", wait_for)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot, "_order_state", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)

    async def slow():
        await asyncio.sleep(90)
        return {"PortfolioResponse": {"AccountPortfolio": []}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_portfolio", slow)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": True,
            "lost_stop_cancel_pending": True, "lost_stop_watch_until": 0,
            "fill_detected_ts": 1, "stop_deadline_ts": 1_000_000.0,
            "deadline_ts": 1_000_000.0, "guarded_qty": 100, "trued_qty": 100,
            "entry_order_id": "ENTRY-1", "stop_reply_bound": True,
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(pages)

    times = _run(scenario())
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert main_bot.PRE_CANCEL_STOP_READ_TIMEOUT_S in timeouts
    assert len(times) >= 3
    assert gaps and max(gaps) <= 30


def _stop_payload(order_id, *, price_type="STOP", qty=100, stop=90, action="SELL",
                  symbol="QQQ", include_qty=True, client_id=None):
    inst = {
        "orderAction": action,
        "filledQuantity": 0,
        "Product": {"symbol": symbol, "securityType": "EQ"},
    }
    if include_qty:
        inst["orderedQuantity"] = qty
    detail = {
        "status": "OPEN", "priceType": price_type, "Instrument": inst,
        "placedTime": 1_700_000_000_000,
    }
    if price_type in {"STOP", "STOP_LIMIT"} and stop is not None:
        detail["stopPrice"] = stop
    order = {"orderId": order_id, "OrderDetail": detail}
    if client_id:
        order["clientOrderId"] = client_id
    return order


def test_matching_stop_without_client_order_id_is_not_a_miss():
    guard = {
        "action": "BUY", "lost_stop_qty": 100, "lost_stop_price": 90, "stop": 90,
        "stop_sent_ts": 1_600_000_000,
    }
    payload = {"OrdersResponse": {"Order": [_stop_payload("STOP-9")]}}
    orders = main_bot.reconciliation.parse_open_orders(payload)
    assert orders[0]["client_order_id"] == ""
    assert main_bot.lost_stop_book_status(orders, guard, "QQQ") == "match"


def test_missing_order_field_is_not_a_miss():
    payload = {"OrdersResponse": {"Order": [_stop_payload("STOP-9", include_qty=False)]}}
    assert main_bot._raw_open_order_missing_match_field(payload) is True


def test_clean_non_matching_book_is_a_miss():
    guard = {"action": "BUY", "lost_stop_qty": 100, "lost_stop_price": 90}
    payload = {"OrdersResponse": {"Order": [
        _stop_payload("LMT-1", price_type="LIMIT", stop=None, qty=100),
    ]}}
    orders = main_bot.reconciliation.parse_open_orders(payload)
    assert main_bot._raw_open_order_missing_match_field(payload) is False
    assert main_bot.lost_stop_book_status(orders, guard, "QQQ") == "miss"


def test_two_matching_stops_are_not_cancelled():
    guard = {
        "action": "BUY", "lost_stop_qty": 100, "lost_stop_price": 90,
        "stop_sent_ts": 1_600_000_000,
    }
    payload = {"OrdersResponse": {"Order": [
        _stop_payload("A"), _stop_payload("B"),
    ]}}
    orders = main_bot.reconciliation.parse_open_orders(payload)
    assert main_bot.lost_stop_book_status(orders, guard, "QQQ") == "possible"
    assert main_bot.lost_stop_match_ids(orders, guard, "QQQ") == []


def test_stop_on_page_two_is_not_absent():
    guard = {
        "action": "BUY", "lost_stop_qty": 100, "lost_stop_price": 90,
        "stop_sent_ts": 1_600_000_000,
    }
    filler = _stop_payload("LMT-{}", price_type="LIMIT", stop=None)
    page1 = {"OrdersResponse": {"Order": [
        {**_stop_payload(f"LMT-{i}", price_type="LIMIT", stop=None), "orderId": f"LMT-{i}"}
        for i in range(25)
    ], "marker": "page-2"}}
    page2 = {"OrdersResponse": {"Order": [_stop_payload("STOP-9")]}}

    async def fetch(marker):
        return page2 if marker == "page-2" else page1

    async def scenario():
        merged = await main_bot.gather_order_pages(fetch, 25)
        orders = main_bot.reconciliation.parse_open_orders(merged)
        return main_bot.lost_stop_book_status(orders, guard, "QQQ")

    assert _run(scenario()) == "match"


def test_full_page_without_a_next_marker_is_not_absent():
    page = {"OrdersResponse": {"Order": [
        _stop_payload(f"LMT-{i}", price_type="LIMIT", stop=None) for i in range(25)
    ]}}

    async def fetch(marker):
        return page

    async def scenario():
        try:
            await main_bot.gather_order_pages(fetch, 25)
        except main_bot.OrdersReadInconclusive:
            return "inconclusive"
        return "clean"

    assert _run(scenario()) == "inconclusive"


def test_orders_shape_log_has_no_account_or_values(caplog):
    import logging
    main_bot._ORDERS_SHAPE_AT = 0.0
    main_bot._RECENT_ORDER_CLIENTS.clear()
    payload = {"OrdersResponse": {"Order": [{
        "orderId": "999888777",
        "accountId": "12345678",
        "OrderDetail": [{
            "accountId": "12345678",
            "status": "OPEN",
            "priceType": "STOP",
            "stopPrice": 590.25,
            "placedTime": 1,
        }],
    }]}}
    with caplog.at_level(logging.INFO):
        main_bot._note_orders_shape(payload)
    text = caplog.text
    assert "accountId" not in text
    assert "12345678" not in text
    assert "590.25" not in text
    assert "999888777" not in text
    assert "cid_rows=0" in text
    assert "orders-shape" in text


def test_stock_sell_waits_until_60s_after_the_stop(monkeypatch):
    store = main_bot.StateStore(None)

    async def book():
        return {"OrdersResponse": {}}

    orders, _c, _a, clock = _lost_stock(
        monkeypatch, store, held={"n": 100}, book=book,
    )

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        guard = _guard("equity", 100)
        guard.update({
            "stop_client_id": "LOST", "stop_reply_lost": True,
            "stop_reply_clean_reads": 1,
            "stop_reply_clean_read_ts": clock["t"] - 10,
            "stop_sent_ts": clock["t"],
            "lost_stop_qty": 100, "lost_stop_price": 90,
            "stop_deadline_ts": 1_000_000.0, "deadline_ts": 1_000_000.0,
        })
        await store.set_guard("QQQ", guard)
        held = await store.get_guard("QQQ")
        held["done"] = False
        await store.set_guard("QQQ", held)
        await main_bot._stop_guard_worker("QQQ")
        early = list(orders)
        clock["t"] += 60
        held = await store.get_guard("QQQ")
        held["done"] = False
        await store.set_guard("QQQ", held)
        await main_bot._stop_guard_worker("QQQ")
        return early, list(orders)

    early, later = _run(scenario())
    assert early == []
    assert later == [("SELL", 100)]


def test_lost_reply_with_the_book_down_sells_at_120s(monkeypatch):
    store = main_bot.StateStore(None)
    orders = []
    alerts = []
    clock = {"t": 5_000.0}

    async def book():
        raise RuntimeError("book down")

    async def smart(kind, common, tokens):
        orders.append((str(common.get("orderAction") or ""), int(common.get("quantity") or 0)))
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
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_exec())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", lambda *a, **k: _async_none())
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_reply_lost": True, "stop_client_id": "LOST",
            "stop_sent_ts": clock["t"], "fill_detected_ts": 1,
            "guarded_qty": 100, "trued_qty": 100, "entry_order_id": "E",
            "stop_deadline_ts": 1_000_000.0, "deadline_ts": 1_000_000.0,
        })
        await main_bot._stop_guard_worker("QQQ")
        early = list(orders)
        clock["t"] += 120
        guard = await store.get_guard("QQQ")
        guard["done"] = False
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        return early, list(orders), list(alerts)

    early, later, events = _run(scenario())
    assert early == []
    assert later == [("SELL", 100)]
    assert "lost_stop_may_be_live" in events
    assert not any(action.startswith("BUY") for action, _q in later)


def test_case_b_cancels_our_close_then_sells_at_60s(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []
    cancelled = []
    clock = {"t": 8_000.0}
    live = {"on": True}

    async def place(*args, **kwargs):
        raise main_bot.ETradeAPIError(400, "rejected")

    async def book():
        if not live["on"]:
            return {"OrdersResponse": {}}
        return {"OrdersResponse": {"Order": [{
            "orderId": "CLOSE-1",
            "OrderDetail": {
                "status": "OPEN", "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL", "orderedQuantity": 100,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }]}}

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

    async def order_state(*args, **kwargs):
        return "EXECUTED", 100

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
    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})

    async def account(*_a, **_k):
        return "acct"

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": clock["t"] + 60,
            "deadline_ts": 1_000_000.0, "stop_sent_ts": clock["t"],
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(cancelled), list(sold)

    cancelled_ids, fills = _run(scenario())
    assert "CLOSE-1" in cancelled_ids
    assert fills and fills[0][0] >= 8_060
    assert fills[0][1:] == ("SELL", 100)


def test_case_b_does_not_sell_when_the_close_cancel_is_refused(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []
    cancelled = []
    clock = {"t": 8_000.0}

    async def place(*args, **kwargs):
        raise main_bot.ETradeAPIError(400, "rejected")

    async def book():
        return {"OrdersResponse": {"Order": [{
            "orderId": "CLOSE-1",
            "OrderDetail": {
                "status": "OPEN", "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL", "orderedQuantity": 100,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }]}}

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        return main_bot.CANCEL_LIVE

    async def smart(kind, common, tokens):
        sold.append((clock["t"], str(common.get("orderAction") or "")))
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
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": clock["t"] + 60,
            "deadline_ts": 1_000_000.0, "stop_sent_ts": clock["t"],
        })
        await main_bot._stop_guard_worker("QQQ")
        guard = await store.get_guard("QQQ")
        return list(cancelled), list(sold), guard.get("own_close_cancel_refused")

    cancelled_ids, fills, refused = _run(scenario())
    assert "CLOSE-1" in cancelled_ids
    assert fills == []
    assert refused is True
    assert clock["t"] > 8_060


def test_case_b_does_not_sell_while_our_close_still_rests(monkeypatch):
    store = main_bot.StateStore(None)
    sold = []
    clock = {"t": 8_000.0}

    async def place(*args, **kwargs):
        raise main_bot.ETradeAPIError(400, "rejected")

    async def book():
        return {"OrdersResponse": {"Order": [{
            "orderId": "CLOSE-1",
            "OrderDetail": {
                "status": "OPEN", "priceType": "MARKET",
                "Instrument": {
                    "orderAction": "SELL", "orderedQuantity": 100,
                    "Product": {"symbol": "QQQ", "securityType": "EQ"},
                },
            },
        }]}}

    async def cancel(order_id, expected_qty=None):
        return main_bot.CANCEL_CONFIRMED

    async def smart(kind, common, tokens):
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
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", lambda *_a, **_k: _async_acct())
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": clock["t"] + 60,
            "deadline_ts": 1_000_000.0,
        })
        await main_bot._stop_guard_worker("QQQ")
        return list(sold)

    assert _run(scenario()) == []
    assert clock["t"] > 8_060


def test_guard_hands_the_lost_stop_to_the_reconciler_after_120s(monkeypatch):
    store = main_bot.StateStore(None)
    guard_cancels = []
    recon_cancels = []
    clock = {"t": 20_000.0}

    async def book():
        return {"OrdersResponse": {"Order": [_stop_payload("LOST-1")]}}

    async def cancel(order_id, expected_qty=None):
        guard_cancels.append(order_id)
        return main_bot.CANCEL_CONFIRMED

    async def recon_cancel(order_id):
        recon_cancels.append(order_id)
        return False

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def held(*args, **kwargs):
        return 100

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_open())
    monkeypatch.setattr(main_bot.alerts, "send_nowait", quiet)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": {"Position": [{
            "symbolDescription": "QQQ", "quantity": 100,
            "Product": {"symbol": "QQQ", "securityType": "EQ"},
        }]}}}

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": True,
            "lost_stop_cancel_pending": True,
            "lost_stop_watch_until": clock["t"] - 1,
            "lost_stop_order_id": "LOST-1", "lost_stop_qty": 100,
            "lost_stop_price": 90, "stop_sent_ts": 1_600_000_000,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": 1_000_000.0,
            "deadline_ts": 1_000_000.0, "stop_reply_bound": True,
        })
        await main_bot._stop_guard_worker("QQQ")
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, book, recon_cancel, alert=quiet,
        )
        guard = await store.get_guard("QQQ")
        return list(guard_cancels), list(recon_cancels), guard.get("lost_stop_cancel_pending")

    skipped, retried, pending = _run(scenario())
    assert skipped == []
    assert retried == ["LOST-1"]
    assert pending is True


def test_reconciler_retries_a_lost_stop_cancel_until_it_confirms(monkeypatch):
    store = main_bot.StateStore(None)
    outcomes = []

    async def book():
        return {"OrdersResponse": {"Order": [_stop_payload("LOST-1")]}}

    async def cancel(order_id):
        outcomes.append(order_id)
        if len(outcomes) == 1:
            raise RuntimeError("cancel down")
        if len(outcomes) == 2:
            return False
        return True

    async def quiet(*args, **kwargs):
        return None

    async def portfolio():
        return {"PortfolioResponse": {"AccountPortfolio": {"Position": [{
            "symbolDescription": "QQQ", "quantity": 100,
            "Product": {"symbol": "QQQ", "securityType": "EQ"},
        }]}}}

    monkeypatch.setattr(main_bot, "state", store)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "lost_stop_cancel_pending": True, "lost_stop_order_id": "LOST-1",
            "lost_stop_qty": 100, "lost_stop_price": 90,
            "stop_sent_ts": 1_600_000_000, "stop_client_id": "LOST",
            "deadline_ts": 1_000_000_000.0,
        })
        pending = []
        for _step in range(3):
            await main_bot.reconciliation.reconcile_once(
                store, portfolio, book, cancel, alert=quiet,
            )
            guard = await store.get_guard("QQQ")
            pending.append(bool(guard.get("lost_stop_cancel_pending")))
        done = bool((await store.get_guard("QQQ")).get("lost_stop_cancel_done"))
        return list(outcomes), pending, done

    tried, pending, done = _run(scenario())
    assert tried == ["LOST-1", "LOST-1", "LOST-1"]
    assert pending == [True, True, False]
    assert done is True


def test_flat_holding_and_a_confirmed_cancel_stops_paging(monkeypatch):
    store = main_bot.StateStore(None)
    pages = []
    clock = {"t": 30_000.0}

    async def book():
        return {"OrdersResponse": {}}

    async def send_nowait(severity, event, message, **kwargs):
        if event == "position_unprotected":
            pages.append(clock["t"])

    async def nap(_seconds):
        await store.update_guard("QQQ", done=True)

    async def quiet(*args, **kwargs):
        return None

    async def held(*args, **kwargs):
        return 0

    monkeypatch.setattr(main_bot.time, "time", lambda: clock["t"])
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.asyncio, "sleep", nap)
    monkeypatch.setattr(main_bot, "_order_state", lambda *a, **k: _async_open())
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", quiet)

    async def once(extra):
        guard = await store.get_guard("QQQ")
        guard.update({"done": False, **extra})
        await store.set_guard("QQQ", guard)
        await main_bot._stop_guard_worker("QQQ")
        fresh = await store.get_guard("QQQ")
        return fresh.get("result"), len(pages)

    async def scenario():
        await store.set_position("QQQ", _position("equity", 100))
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
            "stop_lost_flatten_sent": True, "stop_lost_qty_confirmed": True,
            "lost_stop_cancel_pending": False, "lost_stop_cancel_done": False,
            "guarded_qty": 0, "trued_qty": 100, "entry_order_id": "E",
            "fill_detected_ts": 1, "stop_deadline_ts": 1_000_000.0,
            "deadline_ts": 1_000_000.0, "stop_reply_bound": True,
            "last_status": "OPEN", "last_filled": 0,
        })
        still_open = await once({})
        clock["t"] += 30
        finished = await once({"lost_stop_cancel_done": True})
        await main_bot._stop_guard_worker("QQQ")
        return still_open, finished, len(pages)

    still_open, finished, total = _run(scenario())
    assert still_open[0] != "flattened"
    assert still_open[1] >= 1
    assert finished[0] == "flattened"
    assert total == finished[1]


async def _async_exec():
    return "EXECUTED", 100


async def _async_open():
    return "OPEN", 0


async def _async_none():
    return None


async def _async_acct():
    return "acct"

