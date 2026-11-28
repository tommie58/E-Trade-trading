"""5.99.21: still-open untracked names, and alerts that do not block trading.

Webhook strings below are fixtures. They are not receivers. Tests assert
those strings never appear in a log line.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

import main_bot


ET = ZoneInfo("America/New_York")
FULL_DAY = datetime(2026, 11, 18, 15, 50, tzinfo=ET)
HALF_DAY = datetime(2026, 11, 27, 12, 50, tzinfo=ET)
# Fixture only. Not a configured receiver.
WEBHOOK = "https://example.invalid/hook/tokfixture"
HEADER = "Bearer-fixture"


def _run(coro):
    return asyncio.run(coro)


class _Resp:
    status_code = 204


class _Client:
    """Stand-in for httpx.AsyncClient. Does not keep the request URL."""

    mode = "hang"

    def __init__(self, *args, **kwargs):
        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, **kwargs):
        if type(self).mode == "raise":
            raise RuntimeError(WEBHOOK + " Authorization: " + HEADER)
        await asyncio.sleep(30)
        return _Resp()


def _install_http(monkeypatch, mode: str) -> None:
    _Client.mode = mode
    monkeypatch.setattr(httpx, "AsyncClient", _Client)


def _freeze(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when.astimezone(timezone.utc))


async def _quiet_balance(*, dry_run=False):
    return {"total": 10_000.0, "available": 10_000.0}


def _learning_transition():
    return {
        "cells": {
            "breakout|high": {
                "history": [{
                    "ts": time.time(),
                    "to": "disabled",
                    "why": "measured loser",
                }],
            },
        },
    }


@pytest.fixture
def isolated(monkeypatch):
    saved_state = main_bot.state
    saved_alerts = main_bot.alerts._manager
    saved_task = main_bot._still_open_alert_task
    main_bot.portfolio_risk.clear_spy_gap_sit_out()
    store = main_bot.StateStore(None)
    main_bot.state = store
    main_bot._still_open_alert_task = None
    yield store
    task = main_bot._still_open_alert_task
    if task is not None and not task.done():
        task.cancel()
    main_bot._still_open_alert_task = saved_task
    main_bot.state = saved_state
    main_bot.alerts._manager = saved_alerts
    main_bot.portfolio_risk.clear_spy_gap_sit_out()


async def _stop_deliveries():
    main_bot.alerts._deliveries_open = False
    pending = [task for task in list(main_bot.alerts._delivery_tasks) if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    while main_bot.alerts._critical_queue:
        coro, _label = main_bot.alerts._critical_queue.popleft()
        close = getattr(coro, "close", None)
        if close is not None:
            close()
    main_bot.alerts._delivery_tasks.clear()
    main_bot.alerts._background_tasks.clear()
    main_bot.alerts._deliveries_open = True


def _assert_secret_absent(text: str) -> None:
    assert WEBHOOK not in text
    assert "tokfixture" not in text
    assert HEADER not in text
    assert "Authorization" not in text


def test_version_pins_the_day_trade_off_parser_and_the_rails():
    """off, no, and 0 are OFF. The parser behavior is unchanged and documented.

    The 5% and 10% rails are unchanged.
    """
    assert main_bot.BOT_VERSION == "5.99.22-pro-day-trader"
    assert main_bot.RISK_PER_TRADE_PCT == 5.0
    assert main_bot.DAILY_LOSS_LIMIT_PCT == 10.0
    for raw in ("off", "OFF", "no", "No", "0"):
        enabled, warning = main_bot.resolve_day_trade_mode(raw)
        assert enabled is False and warning is None
    assert main_bot.resolve_day_trade_mode(None) == (True, None)
    assert main_bot.swing_hold_exempt(True, True) is False
    flat = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=1800, et_minutes=15 * 60 + 45, weekday="Wed",
        expiry="2026-11-20", today="2026-11-18", flatten_all=True,
    )
    assert flat["fire"] is True and flat["reason"] == "eod_flatten"


def _option(expiration: str) -> dict:
    return {
        "qty": 1,
        "filled_qty": 1,
        "action": "BUY",
        "contract": {"right": "CALL", "strike": 500, "expiration": expiration},
    }


def _book(untracked, *, age_s=5.0, positions=None, epoch=True) -> str:
    body = {"untracked": list(untracked)}
    if positions is not None:
        body["positions"] = positions
    if epoch:
        body["epoch"] = time.time() - age_s
    return json.dumps(body)


def _dedupe_key(day: str = "2026-11-18", kind: str = "tracked") -> str:
    return f"alert:dedupe:open_after_flatten:{kind}:{day}"


def test_open_after_flatten_names_include_untracked_broker_legs():
    names = main_bot.open_after_flatten_names(
        {"qqq": {"qty": 1}},
        {
            "untracked": ["AMD", "amd", "QQQ"],
            "positions": [
                {"symbol": "amd", "tracked": False},
                {"symbol": "QQQ", "tracked": False},
                {"symbol": "IWM", "tracked": True},
            ],
        },
    )
    assert names == ["QQQ", "AMD (untracked)"]
    assert "QQQ (untracked)" not in names
    assert len(names) == len(set(names))
    assert main_bot.open_after_flatten_names({}, None) == []
    assert main_bot.open_after_flatten_names({"spy": {}}, None) == ["SPY"]


def test_still_open_alert_names_an_untracked_broker_position(isolated, monkeypatch):
    _freeze(monkeypatch, FULL_DAY)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD"], positions=[{"symbol": "AMD", "tracked": False, "qty": 4}]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    assert [item["event"] for item in recent] == ["position_open_after_flatten"]
    assert "AMD (untracked)" in recent[0]["message"]
    assert "Broker snapshot age" in recent[0]["message"]
    assert "3:50 PM ET" in recent[0]["message"]
    _assert_secret_absent(recent[0]["message"])


def test_still_open_alert_names_tracked_and_untracked_together(isolated, monkeypatch):
    _freeze(monkeypatch, HALF_DAY)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", {"qty": 1, "filled_qty": 1})
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["IWM"]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    text = " ".join(item["message"] for item in recent)
    assert "QQQ" in text
    assert "IWM (untracked)" in text
    assert "QQQ (untracked)" not in text
    assert "Broker snapshot age" in text
    assert "12:50 PM ET" in text
    assert len(recent) == 2


def test_mode_off_alerts_a_tracked_0dte_and_skips_swings_and_untracked(isolated, monkeypatch):
    """Mode off still pages a tracked position that expires today."""
    _freeze(monkeypatch, FULL_DAY)
    monkeypatch.setattr(main_bot, "DAY_TRADE_MODE", False)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _option("2026-11-18"))
        await store.set_position("IWM", _option("2026-11-20"))
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD"]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    message = recent[0]["message"]
    assert recent[0]["event"] == "position_open_after_flatten"
    assert "QQQ" in message
    assert "IWM" not in message
    assert "AMD" not in message
    assert "(untracked)" not in message


def test_mode_off_skips_a_tracked_swing_that_does_not_expire_today(isolated, monkeypatch):
    _freeze(monkeypatch, FULL_DAY)
    monkeypatch.setattr(main_bot, "DAY_TRADE_MODE", False)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _option("2026-11-20"))
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD"]),
        )
        await main_bot._alert_positions_still_open()
        recent = await main_bot.alerts.recent(5)
        key = await store.get(_dedupe_key())
        return recent, key

    recent, key = _run(scenario())
    assert recent == []
    assert key is None


def test_mode_on_names_swings_and_a_fresh_untracked_book(isolated, monkeypatch):
    _freeze(monkeypatch, FULL_DAY)
    monkeypatch.setattr(main_bot, "DAY_TRADE_MODE", True)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _option("2026-11-18"))
        await store.set_position("IWM", _option("2026-11-20"))
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD", "QQQ"]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    text = " ".join(item["message"] for item in recent)
    assert "QQQ" in text and "IWM" in text
    assert "AMD (untracked)" in text
    assert "QQQ (untracked)" not in text
    assert "Broker snapshot age" in text
    assert len(recent) == 2


def test_stale_snapshot_does_not_name_untracked_or_take_the_dedupe_key(isolated, monkeypatch, caplog):
    """A days-old snapshot must not page, and must not spend the day's key."""
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    _freeze(monkeypatch, FULL_DAY)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMZN"], age_s=3 * 24 * 60 * 60),
        )
        await main_bot._alert_positions_still_open()
        quiet = await main_bot.alerts.recent(5)
        free = await store.get(_dedupe_key())
        await store.set_position("QQQ", _option("2026-11-18"))
        await main_bot._alert_positions_still_open()
        recent = await main_bot.alerts.recent(5)
        taken = await store.get(_dedupe_key())
        return quiet, free, recent, taken

    quiet, free, recent, taken = _run(scenario())
    assert quiet == []
    assert free is None
    assert taken == "1"
    message = recent[0]["message"]
    assert "QQQ" in message
    assert "AMZN" not in message
    assert "(untracked)" not in message
    assert "broker snapshot age" in caplog.text
    assert "untracked names skipped" in caplog.text
    assert "AMZN" not in caplog.text
    assert "unknown" not in caplog.text


def test_unknown_snapshot_age_skips_untracked_names(isolated, monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    _freeze(monkeypatch, FULL_DAY)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMZN"], epoch=False),
        )
        await main_bot._alert_positions_still_open()
        recent = await main_bot.alerts.recent(5)
        key = await store.get(_dedupe_key())
        return recent, key

    recent, key = _run(scenario())
    assert recent == []
    assert key is None
    assert "broker snapshot age unknown" in caplog.text
    assert "AMZN" not in caplog.text


def test_tracked_page_does_not_suppress_a_later_untracked_page(isolated, monkeypatch):
    """Each class pages once per day. The tracked key must not eat the other."""
    _freeze(monkeypatch, FULL_DAY)
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", _option("2026-11-18"))
        await main_bot._alert_positions_still_open()
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD"]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    text = " ".join(item["message"] for item in recent)
    assert "QQQ" in text
    assert "AMD (untracked)" in text
    assert len(recent) == 2


def test_still_open_alert_waits_until_350(isolated, monkeypatch):
    _freeze(monkeypatch, FULL_DAY.replace(minute=49))
    store = isolated

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("QQQ", {"qty": 1})
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    assert _run(scenario()) == []


def test_still_open_alert_survives_a_failed_book_read(isolated, monkeypatch, caplog):
    """A tracked-book failure still pages an untracked broker position.

    The log is the exception type. The message is not included.
    """
    caplog.set_level(logging.DEBUG)
    _freeze(monkeypatch, FULL_DAY)
    store = isolated

    async def broken():
        raise RuntimeError(WEBHOOK + " Authorization: " + HEADER)

    monkeypatch.setattr(store, "all_positions", broken)

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            _book(["AMD"]),
        )
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    recent = _run(scenario())
    assert "AMD (untracked)" in recent[0]["message"]
    assert "RuntimeError" in caplog.text
    _assert_secret_absent(caplog.text)


def test_slow_or_failing_webhook_does_not_delay_the_still_open_alert(isolated, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    _freeze(monkeypatch, FULL_DAY)
    store = isolated
    elapsed = {}

    async def scenario(mode):
        _install_http(monkeypatch, mode)
        main_bot.alerts.init(store, WEBHOOK)
        await store.set_position("QQQ", {"qty": 1, "filled_qty": 1})
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            json.dumps({"untracked": ["AMD"]}),
        )
        # Fresh dedupe key per mode: the day key is the session date.
        await store.delete("alert:dedupe:open_after_flatten:tracked:2026-11-18")
        await store.delete("alert:dedupe:open_after_flatten:untracked:2026-11-18")
        started = time.perf_counter()
        await main_bot._alert_positions_still_open()
        elapsed[mode] = time.perf_counter() - started
        await asyncio.sleep(0)
        recent = await main_bot.alerts.recent(5)
        await _stop_deliveries()
        return recent

    try:
        hung = _run(scenario("hang"))
        failed = _run(scenario("raise"))
    finally:
        _run(_stop_deliveries())

    assert elapsed["hang"] < 0.5
    assert elapsed["raise"] < 0.5
    assert any(item["event"] == "position_open_after_flatten" for item in hung)
    assert any(item["event"] == "position_open_after_flatten" for item in failed)
    _assert_secret_absent(caplog.text)
    _assert_secret_absent(json.dumps(hung))
    _assert_secret_absent(json.dumps(failed))


def test_trail_loop_does_not_await_the_still_open_alert(isolated, monkeypatch):
    src = inspect.getsource(main_bot.trailing_worker)
    assert "_kick_positions_still_open_alert()" in src
    assert "await _alert_positions_still_open" not in src
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow():
        started.set()
        await release.wait()

    monkeypatch.setattr(main_bot, "_alert_positions_still_open", slow)

    async def scenario():
        begin = time.perf_counter()
        main_bot._kick_positions_still_open_alert()
        elapsed = time.perf_counter() - begin
        await asyncio.wait_for(started.wait(), timeout=1)
        # A second kick does not start another check while the first runs.
        main_bot._kick_positions_still_open_alert()
        release.set()
        await main_bot._still_open_alert_task
        return elapsed

    assert _run(scenario()) < 0.2


def test_unprotected_alert_is_staged_before_rearm_and_does_not_wait(isolated, monkeypatch, caplog):
    """The webhook may hang or raise. The re-arm still runs, after the alert."""
    caplog.set_level(logging.DEBUG)
    store = isolated
    # reconcile_once is loaded from an embedded string, so inspect cannot
    # read it. The file order is the order that runs.
    src = open(main_bot.__file__, encoding="utf-8").read()
    alert_at = src.index('"position_unprotected"')
    rearm_at = src.index("rearm_guard(ticker, pos)", alert_at)
    assert alert_at < rearm_at
    for fn in (main_bot._start_reconciler, main_bot._startup_reconcile):
        wiring = inspect.getsource(fn)
        assert "alert=alerts.send_nowait" in wiring

    async def portfolio():
        return {
            "PortfolioResponse": {
                "AccountPortfolio": {
                    "Position": [{
                        "quantity": 10,
                        "marketValue": 1000,
                        "Product": {"symbol": "AMD", "securityType": "EQ"},
                    }],
                },
            },
        }

    async def orders():
        return {"OrdersResponse": {}}

    async def cancel(_order_id):
        return True

    async def scenario(mode):
        _install_http(monkeypatch, mode)
        main_bot.alerts.init(store, WEBHOOK)
        await store.delete("alert:dedupe:unprotected:AMD")
        await store.set_position("AMD", {
            "action": "BUY",
            "entry": 100.0,
            "stop": 95.0,
            "filled_qty": 10,
            "qty": 10,
            "ts": "2020-01-01T15:00:00+00:00",
        })
        order = []
        real = main_bot.alerts.send_nowait

        async def watching(severity, event, message, **kwargs):
            if event == "position_unprotected":
                order.append("alert")
            await real(severity, event, message, **kwargs)
            if event == "position_unprotected":
                order.append("alert_done")

        async def rearm(ticker, pos):
            recent = await main_bot.alerts.recent(10)
            order.append((
                "rearm",
                ticker,
                [item.get("event") for item in recent],
            ))
            return "placed"

        started = time.perf_counter()
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, orders, cancel,
            rearm_guard=rearm, alert=watching, auto_heal=True,
        )
        elapsed = time.perf_counter() - started
        await asyncio.sleep(0)
        await _stop_deliveries()
        return elapsed, order

    try:
        hung_elapsed, hung_order = _run(scenario("hang"))
        fail_elapsed, fail_order = _run(scenario("raise"))
    finally:
        _run(_stop_deliveries())

    for elapsed, order in ((hung_elapsed, hung_order), (fail_elapsed, fail_order)):
        assert elapsed < 0.5
        assert order[0] == "alert"
        assert order[1] == "alert_done"
        assert order[2][0] == "rearm"
        assert order[2][1] == "AMD"
        assert "position_unprotected" in order[2][2]
    _assert_secret_absent(caplog.text)


def test_close_and_learning_alerts_do_not_wait_on_the_webhook(isolated, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    store = isolated
    _install_http(monkeypatch, "hang")
    monkeypatch.setattr(main_bot, "_fetch_broker_balance", _quiet_balance)
    monkeypatch.setattr(main_bot, "_learning_snapshot", _learning_transition)
    announce = inspect.getsource(main_bot._announce_learning_transitions)
    assert "alerts.send_nowait(" in announce
    assert "alerts.send(" not in announce

    async def scenario():
        main_bot.alerts.init(store, WEBHOOK)
        await store.set_position("PLTR", {
            "qty": 1,
            "filled_qty": 1,
            "entry": 100.0,
            "action": "BUY",
        })
        started = time.perf_counter()
        await main_bot._announce_learning_transitions("PLTR")
        await main_bot._record_close(
            "PLTR", 101.0, {"ticker": "PLTR", "action": "BUY"},
            close_reason="close_fill",
        )
        elapsed = time.perf_counter() - started
        await asyncio.sleep(0)
        recent = await main_bot.alerts.recent(10)
        await _stop_deliveries()
        return elapsed, [item["event"] for item in recent]

    try:
        elapsed, events = _run(scenario())
    finally:
        _run(_stop_deliveries())

    assert elapsed < 1.0
    assert "learning_transition" in events
    assert "trade_sell_filled" in events
    _assert_secret_absent(caplog.text)


def test_spy_gap_alert_does_not_wait_on_a_failing_webhook(isolated, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    store = isolated
    _install_http(monkeypatch, "raise")
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    monkeypatch.setattr(
        main_bot, "_utcnow",
        lambda: datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc),
    )
    gap = inspect.getsource(main_bot._alert_spy_gap_sit_out_once)
    assert "alerts.send_nowait(" in gap
    assert "alerts.send(" not in gap

    async def scenario():
        main_bot.alerts.init(store, WEBHOOK)
        payload = {
            "ticker": "TSLA",
            "instrument": "option",
            "action": "BUY",
            "option_right": "PUT",
            "risk_context": {"spy_gap_pct": -2.5, "spy_gap_verified": True},
        }
        reason = await main_bot.entry_gap_sit_out_reason(payload)
        started = time.perf_counter()
        await main_bot._alert_spy_gap_sit_out_once(reason or "")
        elapsed = time.perf_counter() - started
        await asyncio.sleep(0)
        recent = await main_bot.alerts.recent(5)
        await _stop_deliveries()
        return elapsed, reason, recent

    try:
        elapsed, reason, recent = _run(scenario())
    finally:
        _run(_stop_deliveries())

    assert reason and "sit-out" in reason
    assert elapsed < 0.5
    assert any(item["event"] == "spy_gap_sit_out" for item in recent)
    _assert_secret_absent(caplog.text)
    _assert_secret_absent(json.dumps(recent))


def test_scrub_fallback_says_unknown_and_drops_the_token_tail(monkeypatch):
    def boom(_message):
        raise RuntimeError(WEBHOOK + " Authorization: " + HEADER)

    monkeypatch.setattr(main_bot.alerts, "_scrub_alert_text", boom)
    line = main_bot.alerts._scrub_webhook_source_line(
        "webhook_source=env " + WEBHOOK,
        [WEBHOOK],
    )
    assert line == "alerting: webhook_source=unknown, effective_min_severity=warning"
    _assert_secret_absent(line)
    masked = main_bot.alerts.mask_webhook_url(WEBHOOK)
    assert masked == "https://example.invalid/…"
    _assert_secret_absent(masked or "")


def test_failure_text_adds_status_or_errno_and_never_a_url(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG)
    called = {"n": 0}

    def boom(_message):
        called["n"] += 1
        raise RuntimeError(WEBHOOK)

    monkeypatch.setattr(main_bot.alerts, "_scrub_alert_text", boom)
    manager = main_bot.alerts.AlertManager(main_bot.StateStore(None))

    class _Http(Exception):
        def __init__(self):
            super().__init__(WEBHOOK + " Authorization: " + HEADER + " body=secret")
            self.response = type("R", (), {
                "status_code": 503,
                "text": WEBHOOK,
                "headers": {"Authorization": HEADER},
            })()

    class _Wrap(Exception):
        pass

    status = manager._failure_text(_Http())
    plain = manager._failure_text(RuntimeError(WEBHOOK + " " + HEADER))
    refused = OSError(111, WEBHOOK + " Authorization: " + HEADER)
    errno = manager._failure_text(refused)
    try:
        raise OSError(111, WEBHOOK)
    except OSError as inner:
        try:
            raise _Wrap(WEBHOOK + " Authorization: " + HEADER) from inner
        except _Wrap as wrapped:
            caused = manager._failure_text(wrapped)
    main_bot.alerts.logger.warning("%s", status)
    assert status == "_Http HTTP 503"
    assert plain == "RuntimeError"
    assert errno == type(refused).__name__ + " errno 111"
    assert caused == "_Wrap errno 111"
    assert called["n"] == 0
    _assert_secret_absent(status + plain + errno + caused)
    _assert_secret_absent(caplog.text)


def test_error_cap_applies_when_no_env_webhook_is_set(isolated, monkeypatch):
    """A saved critical floor still delivers error with no env URL."""
    _install_http(monkeypatch, "ok")

    class _Ok(_Client):
        async def post(self, url, json=None, **kwargs):
            severity = json.get("severity") if isinstance(json, dict) else None
            _Ok.posts.append(severity)
            return _Resp()

    _Ok.posts = []
    monkeypatch.setattr(httpx, "AsyncClient", _Ok)
    store = isolated

    async def scenario():
        await store.set(
            main_bot.alerts.ALERTS_CONFIG_KEY,
            json.dumps({"webhook_url": WEBHOOK, "min_severity": "critical"}),
        )
        main_bot.alerts.init(store, None)
        await main_bot.alerts.load_persisted()
        saved = await main_bot.alerts.get_config()
        await main_bot.alerts.send_nowait("warning", "routine_notice", "below")
        await main_bot.alerts.send_nowait("error", "etrade_http_error", "read failed")
        pending = [task for task in list(main_bot.alerts._delivery_tasks) if not task.done()]
        if pending:
            await asyncio.gather(*pending)
        # Nothing configured: the reported floor is still capped.
        await store.set(
            main_bot.alerts.ALERTS_CONFIG_KEY,
            json.dumps({"webhook_url": "", "min_severity": "critical"}),
        )
        main_bot.alerts.init(store, "")
        await main_bot.alerts.load_persisted()
        empty = await main_bot.alerts.get_config()
        await _stop_deliveries()
        return saved, empty, list(_Ok.posts)

    saved, empty, posts = _run(scenario())
    assert saved["webhook_source"] == "persisted"
    assert saved["effective_min_severity"] == "error"
    assert saved["min_severity"] == "critical"
    assert "no env webhook" in saved["note"]
    assert posts == ["error"]
    assert empty["webhook_source"] == "none"
    assert empty["effective_min_severity"] == "error"
    assert "no env webhook" in empty["note"]
    _assert_secret_absent(json.dumps(saved))
    _assert_secret_absent(json.dumps(empty))
