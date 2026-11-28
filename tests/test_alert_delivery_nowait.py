"""Webhook delivery must not sit on a stop, flatten, or kill.

A receiver that sleeps does not delay the protective stop, the next
ticker in the flatten replace loop, or the /kill response. The in-app
record is still written. A task that raises is logged and swallowed.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

import main_bot

ET = ZoneInfo("America/New_York")
# Full session, inside the close-10 replace window, before the bell.
_FLATTEN_AT = datetime(2026, 11, 24, 15, 55, tzinfo=ET)
_WEBHOOK = "http://alerts.invalid/hook-secret-do-not-log"


def _rork_secret(monkeypatch) -> str:
    """Header value. Read only through os.environ.get. Never logged."""
    current = os.environ.get("RORK_SECRET")
    if not current:
        monkeypatch.setenv("RORK_SECRET", "rork-test-secret")
        current = os.environ.get("RORK_SECRET")
    return current or ""


class _Resp:
    status_code = 204


class _Client:
    """Stand-in for httpx.AsyncClient. Does not keep the request URL."""

    hang = False
    events: list = []
    started: list = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, **kwargs):
        event = json.get("event") if isinstance(json, dict) else None
        type(self).events.append(event)
        type(self).started.append(time.perf_counter())
        if type(self).hang:
            await asyncio.sleep(30)
        return _Resp()


def _install_client(monkeypatch, *, hang: bool) -> None:
    _Client.hang = hang
    _Client.events = []
    _Client.started = []
    monkeypatch.setattr(httpx, "AsyncClient", _Client)


def _freeze(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when.astimezone(timezone.utc))


async def _quiet_ledger(*_args, **_kwargs):
    return None


def _restore_alerts(saved):
    main_bot.alerts._deliveries_open = False
    while main_bot.alerts._critical_queue:
        coro, _label = main_bot.alerts._critical_queue.popleft()
        close = getattr(coro, "close", None)
        if close is not None:
            close()
    main_bot.alerts._dropped = 0
    for task in list(main_bot.alerts._delivery_tasks) + list(main_bot.alerts._background_tasks):
        task.cancel()
    main_bot.alerts._delivery_tasks.clear()
    main_bot.alerts._background_tasks.clear()
    main_bot.alerts._deliveries_open = True
    main_bot.alerts._manager = saved


def test_hanging_webhook_does_not_delay_the_protective_stop(monkeypatch):
    """Stop is resting before the fill alert's POST, in well under half a second."""
    placed = []
    saved_state = main_bot.state
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=True)
    monkeypatch.setattr(main_bot.trade_ledger, "record", _quiet_ledger)

    async def order_state(_order_id, _client_id):
        return "EXECUTED", 2

    async def true_up(ticker, _guard):
        pos = await main_bot.state.get_position(ticker)
        pos["entry_premium"] = 1.5
        await main_bot.state.set_position(ticker, pos)
        return True

    async def place_stop(ticker, _contract, qty, stop_premium):
        placed.append(time.perf_counter())
        return {"order_id": "STOP-NEW", "client_id": "cid", "qty": qty, "stop": stop_premium}

    async def settle(_ticker):
        return None

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", true_up)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_stop)
    monkeypatch.setattr(main_bot, "settle_heat_after_stop_armed", settle)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        main_bot.alerts.init(store, _WEBHOOK)
        now = time.time()
        await store.set_position("QQQ", {
            "qty": 2,
            "filled_qty": 0,
            "action": "BUY",
            "contract": {"right": "CALL", "strike": 480, "expiration": "2026-12-18"},
        })
        await store.set_guard("QQQ", {
            "ticker": "QQQ",
            "kind": "option",
            "action": "BUY",
            "stop": 1.25,
            "contract": {"right": "CALL", "strike": 480, "expiration": "2026-12-18"},
            "entry_order_id": "ENTRY-1",
            "entry_client_id": "entry-cid",
            "guarded_qty": 0,
            "last_filled": 0,
            "last_status": "OPEN",
            "stop_order_id": None,
            "qty": 2,
            "done": False,
            "deadline_ts": now + 3600,
            "stop_deadline_ts": now + 3600,
            "fill_detected_ts": None,
            "placed_ts": now,
            "armed_ts": now,
            "trued_qty": 0,
        })
        started = time.perf_counter()
        await main_bot._stop_guard_worker("QQQ")
        elapsed = time.perf_counter() - started
        recent = await main_bot.alerts.recent(20)
        guard = await store.get_guard("QQQ")
        pos = await store.get_position("QQQ")
        return elapsed, recent, guard, pos

    try:
        elapsed, recent, guard, pos = asyncio.run(scenario())
    finally:
        main_bot.state = saved_state
        _restore_alerts(saved_alerts)

    assert placed, "protective stop was not placed"
    assert _Client.started, "fill alert never reached the webhook"
    assert placed[0] < _Client.started[0]
    assert elapsed < 0.5
    assert pos is not None and pos["stop_order_id"] == "STOP-NEW"
    assert guard["result"] == "filled_and_protected"
    assert any(item.get("event") == "trade_buy_filled" for item in recent)
    assert _WEBHOOK not in repr(recent)


def test_flatten_loop_does_not_wait_on_the_previous_tickers_alert(monkeypatch):
    """Ticker two is handled while ticker one's webhook is still asleep."""
    saved_state = main_bot.state
    saved_alerts = main_bot.alerts._manager
    _freeze(monkeypatch, _FLATTEN_AT)
    _install_client(monkeypatch, hang=True)
    monkeypatch.setattr(main_bot.trade_ledger, "record", _quiet_ledger)
    reads = []

    async def read_order(order_id):
        reads.append((str(order_id), time.perf_counter()))
        return {"status": "UNKNOWN", "filled": 0, "ordered": 4}

    monkeypatch.setattr(main_bot, "_flatten_read_order", read_order)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        main_bot.alerts.init(store, _WEBHOOK)
        placed = (_FLATTEN_AT.astimezone(timezone.utc)).isoformat()
        for ticker, order_id in (("AAA", "CLOSE-A"), ("BBB", "CLOSE-B")):
            await store.set_position(ticker, {
                "qty": 4,
                "filled_qty": 4,
                "action": "BUY",
                "contract": {"right": "CALL", "expiration": "2026-12-18", "strike": 500},
                "pending_flatten_close": {
                    "order_id": order_id,
                    "qty": 4,
                    "placed_at": placed,
                    "reason": "eod_flatten",
                    "replacements": 0,
                },
            })
        assert main_bot._is_market_open()
        assert main_bot._unfilled_flatten_replace_window(
            main_bot._utcnow().astimezone(ET)
        )
        started = time.perf_counter()
        await main_bot._replace_unfilled_flatten_closes()
        elapsed = time.perf_counter() - started
        recent = await main_bot.alerts.recent(20)
        return elapsed, recent

    try:
        elapsed, recent = asyncio.run(scenario())
    finally:
        main_bot.state = saved_state
        _restore_alerts(saved_alerts)

    assert sorted(item[0] for item in reads) == ["CLOSE-A", "CLOSE-B"]
    assert abs(reads[1][1] - reads[0][1]) < 0.5
    assert elapsed < 0.5
    assert _Client.started, "flatten alert never reached the webhook"
    assert any(item.get("event") == "unfilled_flatten_replace" for item in recent)
    assert _WEBHOOK not in repr(recent)


def test_kill_returns_while_the_webhook_hangs(monkeypatch):
    saved_state = main_bot.state
    saved_alerts = main_bot.alerts._manager
    secret = _rork_secret(monkeypatch)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", secret)
    _install_client(monkeypatch, hang=True)
    monkeypatch.setattr(main_bot.trade_ledger, "record", _quiet_ledger)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        main_bot.alerts.init(store, _WEBHOOK)
        started = time.perf_counter()
        result = await main_bot.kill(x_rork_secret=os.environ.get("RORK_SECRET"))
        elapsed = time.perf_counter() - started
        recent = await main_bot.alerts.recent(10)
        killed = await store.get("killed")
        return result, elapsed, recent, killed

    try:
        result, elapsed, recent, killed = asyncio.run(scenario())
    finally:
        main_bot.state = saved_state
        _restore_alerts(saved_alerts)

    assert result["status"] == "killed"
    assert elapsed < 0.5
    assert killed == "1"
    assert any(item.get("event") == "kill_switch_engaged" for item in recent)
    assert _WEBHOOK not in repr(result)
    assert _WEBHOOK not in repr(recent)


def test_alert_task_exception_is_swallowed_and_logged(monkeypatch, caplog):
    saved_alerts = main_bot.alerts._manager
    caplog.set_level(logging.WARNING, logger="etrade-bot.alerts")

    async def boom():
        raise RuntimeError("delivery broke")

    async def explode(self, record, text_override=None):
        raise RuntimeError("post exploded")

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        assert main_bot.alerts.schedule_alert(boom(), label="boom") is True
        await asyncio.sleep(0)
        monkeypatch.setattr(main_bot.alerts.AlertManager, "_post_webhook", explode)
        await main_bot.alerts.send_nowait(
            "critical", "explode_event", "hello", force_deliver=True,
        )
        await asyncio.sleep(0)
        return await main_bot.alerts.recent(10)

    try:
        recent = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert any("delivery broke" in message for message in warnings)
    assert any("post exploded" in message for message in warnings)
    assert any(item.get("event") == "explode_event" for item in recent)
    joined = "\n".join(warnings)
    assert "hook-secret-do-not-log" not in joined
    assert "http://" not in joined
    assert "https://" not in joined


def _fill_inflight(release: asyncio.Event) -> None:
    async def hold():
        await release.wait()

    for _ in range(main_bot.alerts._MAX_INFLIGHT_DELIVERIES):
        assert main_bot.alerts.schedule_alert(hold(), label="hold") is True


def test_cap_drops_non_critical_and_queues_critical(monkeypatch, caplog):
    """Critical POSTs wait. Only non-critical ones are dropped, and counted."""
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=False)
    caplog.set_level(logging.WARNING, logger="etrade-bot.alerts")
    release = asyncio.Event()

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        _fill_inflight(release)
        await main_bot.alerts.send_nowait(
            "warning", "routine_note", "can wait",
        )
        await main_bot.alerts.send_nowait(
            "critical", "position_unprotected", "still queued",
        )
        await main_bot.alerts.send_nowait(
            "error", "flatten_close_unsaved", "error severity, still queued",
        )
        await main_bot.alerts.send_nowait(
            "warning", "stop_timeout_entry_cancelled", "warning, still queued",
        )
        status = main_bot.alerts.delivery_status()
        recent = await main_bot.alerts.recent(10)
        # Captured before the loop cancels the holders. That cancel frees
        # slots and is what later starts the queue.
        posted_while_full = list(_Client.events)
        main_bot.alerts._deliveries_open = False
        return status, recent, posted_while_full

    try:
        status, recent, posted_while_full = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    assert status == {"in_flight": 50, "dropped": 1, "queued": 3}
    events = {item.get("event") for item in recent}
    assert {"routine_note", "position_unprotected", "flatten_close_unsaved",
            "stop_timeout_entry_cancelled"} <= events
    assert posted_while_full == []
    assert any("alert delivery dropped" in rec.getMessage() for rec in caplog.records)
    assert "hook-secret-do-not-log" not in caplog.text


def test_queued_critical_starts_when_a_slot_frees(monkeypatch):
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=False)
    release = asyncio.Event()

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        _fill_inflight(release)
        await main_bot.alerts.send_nowait("critical", "kill_switch_engaged", "queued")
        await main_bot.alerts.send_nowait("critical", "circuit_breaker_tripped", "second")
        assert main_bot.alerts.delivery_status()["queued"] == 2

        async def release_soon():
            await asyncio.sleep(0.05)
            release.set()

        asyncio.create_task(release_soon())
        await main_bot.alerts.drain(1.0)
        return list(_Client.events), main_bot.alerts.delivery_status()

    try:
        posted, status = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    assert posted[:2] == ["kill_switch_engaged", "circuit_breaker_tripped"]
    assert status["queued"] == 0
    assert status["dropped"] == 0


def test_critical_queue_caps_at_150(monkeypatch, caplog):
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=False)
    caplog.set_level(logging.WARNING, logger="etrade-bot.alerts")
    release = asyncio.Event()
    cap = main_bot.alerts._CRITICAL_QUEUE_MAX

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        _fill_inflight(release)
        for i in range(cap + 1):
            await main_bot.alerts.send_nowait(
                "critical", "native_trail_unprotected", f"queued {i}",
                dedupe_key=f"overflow:{i}",
            )
        posted = list(_Client.events)
        main_bot.alerts._deliveries_open = False
        return main_bot.alerts.delivery_status(), posted

    try:
        status, posted = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    assert cap == 150
    assert status["queued"] == cap
    assert status["dropped"] == 1
    assert status["in_flight"] == 50
    assert posted == []
    assert any("queue full" in rec.getMessage() for rec in caplog.records)
    assert "hook-secret-do-not-log" not in caplog.text


def test_dedupe_seconds_window_suppresses_then_delivers(monkeypatch):
    """A per-call window is accepted, holds the key, and opens again after it."""
    import time as time_mod
    clock = {"t": 1_800_000_000.0}
    monkeypatch.setattr(time_mod, "time", lambda: clock["t"])
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=False)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK, dedupe_seconds=300)
        key = {"dedupe_key": "etrade:GET /orders", "dedupe_seconds": 10}
        await main_bot.alerts.send(
            "error", "etrade_http_error", "first", **key,
        )
        await asyncio.sleep(0)
        await main_bot.alerts.send(
            "error", "etrade_http_error", "second", **key,
        )
        await asyncio.sleep(0)
        mid_posts = list(_Client.events)
        mid_recent = await main_bot.alerts.recent(10)
        clock["t"] += 11
        await main_bot.alerts.send(
            "error", "etrade_http_error", "third", **key,
        )
        await asyncio.sleep(0)
        await main_bot.alerts.send_nowait(
            "error", "etrade_http_error", "other key",
            dedupe_key="etrade:GET /portfolio", dedupe_seconds=10,
        )
        await asyncio.sleep(0)
        return mid_posts, list(_Client.events), mid_recent, await main_bot.alerts.recent(10)

    try:
        mid_posts, posts, mid_recent, recent = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    assert mid_posts == ["etrade_http_error"]
    assert [item.get("message") for item in mid_recent] == ["first"]
    assert posts == ["etrade_http_error", "etrade_http_error", "etrade_http_error"]
    messages = [item.get("message") for item in recent]
    assert "second" not in messages
    assert "first" in messages and "third" in messages and "other key" in messages


def test_drain_finishes_a_fast_post_and_bounds_a_hung_one(monkeypatch):
    saved_alerts = main_bot.alerts._manager
    assert main_bot.alerts._DRAIN_TIMEOUT_S == 3.0

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        _install_client(monkeypatch, hang=False)
        await main_bot.alerts.send_nowait("critical", "eod_flatten_blocked", "fast")
        await main_bot.alerts.drain(1.0)
        fast = list(_Client.events)
        main_bot.alerts._deliveries_open = True
        _install_client(monkeypatch, hang=True)
        await main_bot.alerts.send_nowait("critical", "stop_placement_failed", "slow")
        started = time.perf_counter()
        await main_bot.alerts.drain(0.25)
        elapsed = time.perf_counter() - started
        return fast, elapsed

    try:
        fast, elapsed = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    assert fast == ["eod_flatten_blocked"]
    assert elapsed < 0.6


def test_shutdown_drains_alerts_before_closing_state(monkeypatch):
    saved_stop = main_bot._worker_stop
    order = []

    async def fake_drain(timeout=None):
        order.append(("drain", timeout))

    async def fake_study():
        order.append("study")

    async def fake_close():
        order.append("state")

    monkeypatch.setattr(main_bot.alerts, "drain", fake_drain)
    monkeypatch.setattr(main_bot, "_close_study_page_engine", fake_study)
    monkeypatch.setattr(main_bot.state, "close", fake_close)
    try:
        asyncio.run(main_bot.on_shutdown())
    finally:
        main_bot._worker_stop = saved_stop

    assert order == [("drain", 3.0), "study", "state"]


def test_status_field_reports_delivery_counts():
    import inspect
    assert main_bot.alerts.delivery_status() == {
        "in_flight": 0, "dropped": 0, "queued": 0,
    }
    source = inspect.getsource(main_bot.status)
    assert '"alert_delivery": alerts.delivery_status()' in source


def test_severity_floor_and_force_deliver_are_unchanged(monkeypatch):
    saved_alerts = main_bot.alerts._manager
    _install_client(monkeypatch, hang=False)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.alerts.init(store, _WEBHOOK)
        await main_bot.alerts.send_nowait("info", "below_floor", "logged only")
        await main_bot.alerts.send_nowait(
            "info", "forced_fill", "must post", force_deliver=True,
        )
        await asyncio.sleep(0)
        recent = await main_bot.alerts.recent(10)
        return recent

    try:
        recent = asyncio.run(scenario())
    finally:
        _restore_alerts(saved_alerts)

    events = [item.get("event") for item in recent]
    assert "below_floor" in events
    assert "forced_fill" in events
    assert _Client.events == ["forced_fill"]
