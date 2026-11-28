"""5.99.17: full-budget fit is covered next door. This file is the 404 inference.

Broker HTTP is not called. Portfolio fetches are callbacks. Nothing here
places an order, and no account id is written into a log line.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import pytest

import main_bot
import etrade_async

ACCT = "82345678"
HTML_404 = (
    "<html><head><title>Apache Tomcat - Error report</title></head>"
    "<body><h1>HTTP Status 404 – Not Found</h1><p>401</p></body></html>"
)
# Nov 25, 2026. Six in-session 404s were between 12:10 and 2:51 PM CT.
# The repo has no per-minute log of those six, so the marks below are the
# window the review named (the endpoints and four interior times), not a
# claim that the broker logged these exact minutes. The three later times
# are the ones on the record. 6:30 and 6:52 PM CT are not.
NOV25_CT = (
    ("in session", 12, 10),
    ("in session", 12, 42),
    ("in session", 13, 14),
    ("in session", 13, 46),
    ("in session", 14, 18),
    ("in session", 14, 51),
    ("after the close", 16, 46),
    ("after the close", 17, 46),
    ("after the close", 18, 1),
)


def _run(coro):
    return asyncio.run(coro)


def _logs(caplog) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def test_bot_version_is_5_99_18():
    assert main_bot.BOT_VERSION == "5.99.22-pro-day-trader"
    assert main_bot.reconciliation.ORDERS_READ_MAX_AGE_SECONDS == 30
    assert main_bot.reconciliation.INFERRED_FLAT_PASS_CAP == 2
    assert main_bot.reconciliation.PORTFOLIO_404_RETRY_SECONDS == 2
    assert main_bot.DAY_TRADE_MODE is True
    assert main_bot.RISK_PER_TRADE_PCT == 5.0
    assert main_bot.DAILY_LOSS_LIMIT_PCT == 10.0


def test_auth_failure_keeps_text_and_rejects_a_404_that_mentions_401():
    assert main_bot._looks_like_auth_failure(Exception("oauth_problem=token_rejected")) is True
    assert main_bot._looks_like_auth_failure(Exception("Unauthorized")) is True
    assert main_bot._looks_like_auth_failure(
        etrade_async.ETradeAPIError(401, "Unauthorized")
    ) is True
    html = etrade_async.ETradeAPIError(
        404, "page text mentions 401", body=HTML_404, method="GET",
        path=f"/v1/accounts/{ACCT}/portfolio.json",
    )
    assert "401" in str(html)
    assert main_bot._looks_like_auth_failure(html) is False


def test_order_log_text_redacts_a_long_account_id():
    err = RuntimeError(f"place failed for account {ACCT} oauth_token=sekret")
    text = main_bot._order_exc_text(err)
    assert text.startswith("RuntimeError:")
    assert ACCT not in text
    assert "sekret" not in text


def test_swing_hold_flattens_while_day_trade_mode_is_on_and_exempts_when_off():
    weekly = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=1800, et_minutes=15 * 60 + 45, weekday="Wed",
        expiry="2026-11-20", today="2026-11-18", flatten_all=True,
    )
    assert weekly["fire"] is True and weekly["reason"] == "eod_flatten"
    kept = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=1800, et_minutes=15 * 60 + 45, weekday="Wed",
        expiry="2026-11-20", today="2026-11-18", flatten_all=False,
    )
    assert kept["fire"] is False
    assert main_bot.swing_hold_exempt(True, True) is False
    on = main_bot.exit_guards.plan_hold_guard(
        enabled=True, minute_et=15 * 60 + 50, close_minute=16 * 60,
        today="2026-11-18", expiry="2026-11-18", ticker="AMZN",
        holidays=[], events_for_day=lambda _day: [],
        swing_hold=main_bot.swing_hold_exempt(True, True),
    )
    assert on["fire"] is True
    assert main_bot.swing_hold_exempt(True, False) is True
    off = main_bot.exit_guards.plan_hold_guard(
        enabled=True, minute_et=15 * 60 + 50, close_minute=16 * 60,
        today="2026-11-18", expiry="2026-11-18", ticker="AMZN",
        holidays=[], events_for_day=lambda _day: [],
        swing_hold=main_bot.swing_hold_exempt(True, False),
    )
    assert off["fire"] is False
    assert "swing_hold" in off["detail"]


def test_day_trade_mode_parse_defaults_on_and_warns_the_name_only(caplog):
    assert main_bot.resolve_day_trade_mode(None) == (True, None)
    assert main_bot.resolve_day_trade_mode("") == (True, None)
    assert main_bot.resolve_day_trade_mode("  ") == (True, None)
    for raw in ("true", "TRUE", "1", "on", "YES"):
        assert main_bot.resolve_day_trade_mode(raw) == (True, None)
    for raw in ("false", "FALSE", "0", "off", "NO"):
        assert main_bot.resolve_day_trade_mode(raw) == (False, None)
    enabled, warning = main_bot.resolve_day_trade_mode("swing-please")
    assert enabled is True and warning == "DAY_TRADE_MODE"
    with caplog.at_level("WARNING", logger="etrade-bot"):
        assert main_bot.note_day_trade_mode("swing-please") is True
    assert "DAY_TRADE_MODE" in caplog.text
    assert "swing-please" not in caplog.text
    # Unset, recognized on, and garbage all keep the 3:40 flatten.
    for raw in (None, "on", "garbage"):
        mode, _warn = main_bot.resolve_day_trade_mode(raw)
        assert mode is True
        flat = main_bot.trailing_engine.plan_scheduled_exit(
            held_seconds=600, et_minutes=15 * 60 + 45, weekday="Wed",
            expiry="2026-11-20", today="2026-11-18", flatten_all=mode,
        )
        assert flat["fire"] is True
    off_mode, off_warn = main_bot.resolve_day_trade_mode("off")
    assert off_mode is False and off_warn is None
    overnight = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600, et_minutes=15 * 60 + 45, weekday="Wed",
        expiry="2026-11-20", today="2026-11-18", flatten_all=off_mode,
    )
    assert overnight["fire"] is False


def test_still_open_alert_starts_at_350_and_1250_on_a_half_day():
    full = datetime(2026, 11, 18, 15, 49, tzinfo=main_bot._ET_ZONE)
    assert main_bot.position_still_open_alert_due(full) is False
    assert main_bot.position_still_open_alert_due(full.replace(minute=50)) is True
    half = datetime(2026, 11, 27, 12, 49, tzinfo=main_bot._ET_ZONE)
    assert main_bot.position_still_open_alert_due(half) is False
    assert main_bot.position_still_open_alert_due(half.replace(minute=50)) is True


def test_status_book_flags_never_call_an_inferred_read_flat():
    now = time.time()
    fresh = datetime.now(timezone.utc).isoformat()
    empty = main_bot.status_book_flags(
        {"ts": fresh, "portfolio_ok": True, "broker_positions": 0}, {}, now,
    )
    assert empty["account_flat"] is True
    assert empty["bot_flat"] is True
    assert empty["portfolio_ok"] is True
    inferred = main_bot.status_book_flags(
        {"ts": fresh, "portfolio_ok": "inferred_flat", "broker_positions": 0}, {}, now,
    )
    assert inferred["account_flat"] is None
    assert inferred["portfolio_ok"] == "inferred_flat"
    assert inferred["portfolio_ok"] != "flat"
    held = main_bot.status_book_flags(
        {"ts": fresh, "portfolio_ok": True, "broker_positions": 1},
        {"AMZN": {"qty": 6}}, now,
    )
    assert held["account_flat"] is False
    assert held["bot_flat"] is False
    failed = main_bot.status_book_flags(
        {"ts": fresh, "portfolio_ok": False, "broker_positions": None}, {}, now,
    )
    assert failed["account_flat"] is None
    stale_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    stale = main_bot.status_book_flags(
        {"ts": stale_ts, "portfolio_ok": True, "broker_positions": 0}, {}, now,
    )
    assert stale["account_flat"] is None


def _html_error(status=404, code="", body=None):
    return etrade_async.ETradeAPIError(
        status, "Not Found", body=HTML_404 if body is None else body,
        method="GET", path=f"/v1/accounts/{ACCT}/portfolio.json", code=code,
    )


def _empty_orders():
    return {"OrdersResponse": {"Order": []}}


def _executed(epoch):
    return {
        "OrdersResponse": {
            "Order": {
                "orderId": "99",
                "OrderDetail": {
                    "status": "EXECUTED",
                    "executedTime": int(epoch * 1000),
                    "priceType": "MARKET",
                    "Instrument": {
                        "orderAction": "BUY_OPEN",
                        "orderedQuantity": 6,
                        "filledQuantity": 6,
                        "averageExecutionPrice": 2.5,
                        "Product": {"symbol": "AMZN", "securityType": "OPTN"},
                    },
                },
            }
        }
    }


async def _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch):
    slept = []

    async def sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(main_bot.reconciliation.asyncio, "sleep", sleep)

    async def cancel(_order_id):
        return True

    async def alert(severity, event, message, dedupe_key=None, dedupe_seconds=None):
        alerts.append({
            "severity": severity, "event": event, "message": message,
            "dedupe_key": dedupe_key,
        })

    report = await main_bot.reconciliation.reconcile_once(
        store, fetch_portfolio, fetch_orders, cancel, alert=alert,
    )
    return report, slept


def _arm_empty(store):
    return store.set(
        main_bot.reconciliation.LAST_PORTFOLIO_READ_KEY,
        json.dumps({"empty": True, "epoch": 1_700_000_000.0, "positions": 0}),
    )


def test_html_404_on_an_empty_book_infers_flat_without_touching_the_snapshot(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="etrade-bot.reconcile")

    async def scenario():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        await store.set(main_bot.reconciliation.RECONCILE_FETCH_FAIL_STREAK, "4")
        await store.set(main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY, "SENTINEL")
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _empty_orders()

        report, slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return (
            report, slept, alerts,
            await store.get(main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY),
            await store.get(main_bot.reconciliation.RECONCILE_FETCH_FAIL_STREAK),
            await store.get(main_bot.reconciliation.INFERRED_FLAT_STREAK_KEY),
        )

    report, slept, alerts, snapshot, fail_streak, inferred_streak = _run(scenario())
    assert report["ok"] is True
    assert report["portfolio_ok"] == "inferred_flat"
    assert report["broker_positions"] == 0
    assert report["fill_booking"] == "inferred_flat"
    assert report["warnings"] == []
    assert slept == [main_bot.reconciliation.PORTFOLIO_404_RETRY_SECONDS]
    assert snapshot == "SENTINEL"
    assert fail_streak == "0"
    assert inferred_streak == "1"
    assert alerts == []
    text = _logs(caplog) + json.dumps(report)
    assert "inferred flat" in text
    assert ACCT not in text


def test_tracked_position_guard_or_open_stop_stays_an_error(monkeypatch):
    async def one(seed):
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        await seed(store)
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            if seed.__name__ == "stop":
                return {"OrdersResponse": {"Order": {
                    "orderId": "STOP-1",
                    "OrderDetail": {
                        "status": "OPEN",
                        "priceType": "STOP",
                        "stopPrice": 1.5,
                        "Instrument": {
                            "orderAction": "SELL_CLOSE",
                            "orderedQuantity": 1,
                            "filledQuantity": 0,
                            "Product": {"symbol": "AMZN", "securityType": "OPTN"},
                        },
                    },
                }}}
            return _empty_orders()

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report

    async def position(store):
        await store.set_position("AMZN", {"qty": 6, "filled_qty": 6, "ts": "2026-11-25T15:00:00+00:00"})

    async def guard(store):
        await store.set_guard("AMZN", {"done": False, "entry_order_id": "E"})

    async def stop(store):
        return None

    position.__name__ = "position"
    guard.__name__ = "guard"
    stop.__name__ = "stop"
    held = _run(one(position))
    assert held["portfolio_ok"] is False
    assert held["ok"] is False
    assert held["fill_booking"] == "deferred"
    assert held["infer_blocked"] == "tracked"
    guarded = _run(one(guard))
    assert guarded["infer_blocked"] == "guard"
    assert guarded["portfolio_ok"] is False
    stopped = _run(one(stop))
    assert stopped["infer_blocked"] == "open_orders"
    assert stopped["portfolio_ok"] is False


def test_orders_failure_or_stale_read_does_not_infer(monkeypatch):
    async def failed():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            raise etrade_async.ETradeAPIError(
                500, "down", method="GET", path="/v1/accounts/{acct}/orders.json",
            )

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report

    assert _run(failed())["infer_blocked"] == "orders_failed"
    assert _run(failed())["portfolio_ok"] is False

    async def stale():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        clock = {"t": 0.0}
        monkeypatch.setattr(main_bot.reconciliation.time, "monotonic", lambda: clock["t"])
        alerts = []
        calls = {"n": 0}

        async def fetch_portfolio():
            calls["n"] += 1
            if calls["n"] >= 2:
                clock["t"] = 100.0
            raise _html_error()

        async def fetch_orders():
            clock["t"] = 10.0
            return _empty_orders()

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report

    stale_report = _run(stale())
    assert stale_report["infer_blocked"] == "orders_stale"
    assert stale_report["ok"] is False
    assert stale_report["portfolio_ok"] is False


def test_400_with_code_404_and_json_404_stay_errors(monkeypatch):
    async def run(err):
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []
        calls = {"n": 0}

        async def fetch_portfolio():
            calls["n"] += 1
            raise err

        async def fetch_orders():
            return _empty_orders()

        report, slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report, slept, calls["n"]

    bad_status, slept, n = _run(run(_html_error(
        status=400, code="404",
        body=json.dumps({"Error": {"code": 404, "message": "Resource not found"}}),
    )))
    assert bad_status["portfolio_ok"] is False
    assert bad_status["ok"] is False
    assert "infer_blocked" not in bad_status
    assert slept == []
    assert n == 1

    coded, slept, n = _run(run(_html_error(
        status=404, code="100",
        body=json.dumps({"Error": {"code": 100, "message": "no"}}),
    )))
    assert coded["portfolio_ok"] is False
    assert coded["ok"] is False
    assert slept == [2.0]
    assert n == 2


def test_executed_order_or_a_nonempty_last_read_blocks_inference(monkeypatch):
    async def executed():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _executed(1_700_000_000.0 + 60)

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report

    assert _run(executed())["infer_blocked"] == "executed_since"

    async def seen():
        store = main_bot.StateStore(None)
        await store.set(
            main_bot.reconciliation.LAST_PORTFOLIO_READ_KEY,
            json.dumps({"empty": False, "epoch": 1_700_000_000.0, "positions": 1}),
        )
        await store.set(
            main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY,
            json.dumps({"untracked": ["AMZN"], "positions": [{"symbol": "AMZN", "qty": -6}]}),
        )
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _empty_orders()

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        snapshot = await store.get(main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY)
        return report, snapshot

    report, snapshot = _run(seen())
    assert report["portfolio_ok"] is False
    assert report["infer_blocked"] == "last_read_not_empty"
    assert "AMZN" in snapshot
    assert report["ok"] is False


def test_retry_uses_a_following_200_and_flatten_does_not_wait(monkeypatch):
    async def retry():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []
        calls = {"n": 0}

        async def fetch_portfolio():
            calls["n"] += 1
            if calls["n"] == 1:
                raise _html_error()
            return {"PortfolioResponse": {"AccountPortfolio": []}}

        async def fetch_orders():
            return _empty_orders()

        report, slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report, slept, calls["n"]

    report, slept, n = _run(retry())
    assert n == 2
    assert slept == [2.0]
    assert report["ok"] is True
    assert report["portfolio_ok"] is True
    assert report["broker_positions"] == 0
    assert report["portfolio_ok"] != "inferred_flat"

    async def flatten():
        async def boom():
            raise _html_error()

        monkeypatch.setattr(main_bot, "_reconcile_fetch_portfolio", boom)
        started = time.monotonic()
        qty = await main_bot._flatten_broker_held_qty("AMZN", {"contract": {"right": "PUT"}})
        return qty, time.monotonic() - started

    qty, elapsed = _run(flatten())
    assert qty is None
    assert elapsed < 0.25


def test_third_consecutive_inferred_pass_is_a_normal_error(monkeypatch):
    async def scenario():
        etrade_async._ETRADE_ALERT_AT.clear()
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _empty_orders()

        reports = []
        for _ in range(3):
            report, _slept = await _pass(
                store, fetch_portfolio, fetch_orders, alerts, monkeypatch,
            )
            reports.append(report)
        await etrade_async.drain_etrade_alerts()
        fail = await store.get(main_bot.reconciliation.RECONCILE_FETCH_FAIL_STREAK)
        cap = await store.get(main_bot.reconciliation.INFERRED_FLAT_STREAK_KEY)
        return reports, alerts, fail, cap

    reports, alerts, fail, cap = _run(scenario())
    assert [item["portfolio_ok"] for item in reports] == ["inferred_flat", "inferred_flat", False]
    assert reports[2]["ok"] is False
    assert reports[2]["infer_blocked"] == "streak_cap"
    assert reports[2]["fill_booking"] == "deferred"
    assert fail == "1"
    assert cap == "2"
    events = [item["event"] for item in alerts]
    assert events.count("etrade_http_error") == 1
    assert "reconcile_fetch_failed" not in events


def test_nov25_flat_passes_infer_and_a_live_position_does_not(monkeypatch, caplog):
    """The nine Nov 25 404s. The clock is not an input; each label is one pass."""
    caplog.set_level(logging.INFO, logger="etrade-bot.reconcile")

    async def flat(label):
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _empty_orders()

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return label, report["portfolio_ok"], report["ok"]

    rows = [_run(flat(item)) for item in NOV25_CT]
    assert len(rows) == 9
    assert sum(1 for kind, _h, _m in NOV25_CT if kind == "in session") == 6
    assert all(ok is True and portfolio_ok == "inferred_flat" for _label, portfolio_ok, ok in rows)
    assert ACCT not in _logs(caplog)

    async def live():
        store = main_bot.StateStore(None)
        await _arm_empty(store)
        await store.set_position("AMZN", {"qty": 6, "filled_qty": 6})
        alerts = []

        async def fetch_portfolio():
            raise _html_error()

        async def fetch_orders():
            return _empty_orders()

        report, _slept = await _pass(store, fetch_portfolio, fetch_orders, alerts, monkeypatch)
        return report

    held = _run(live())
    assert held["portfolio_ok"] is False
    assert held["fill_booking"] == "deferred"
    assert held["infer_blocked"] == "tracked"


def test_sizing_replay_without_a_nov18_25_ledger():
    """No Nov 18–25 ledger or fill fixture is in this repo.

    Native 1-lot unproven size is unchanged. A 2-lot plan cut to 1 by the
    learning weight now skips. NaN and inf skip instead of counting as full
    size. A remainder under the full planned risk now refuses.
    """
    native = main_bot.contract_qty_for_risk_weight(
        1, 0.5, market_weight=1.0, ev_weight=0.5, plan_qty=1,
    )
    assert native["qty"] == 1 and native["skipped"] is False
    cut = main_bot.contract_qty_for_risk_weight(
        1, 0.5, market_weight=1.0, ev_weight=0.5, plan_qty=2,
    )
    assert cut["qty"] == 0 and cut["skipped"] is True
    assert main_bot.contract_qty_for_risk_weight(2, float("nan"))["skipped"] is True
    partial = main_bot.assess_remaining_daily_budget(
        {"remaining_usd": 80, "used_usd": 245, "budget_usd": 325}, 162.56,
    )
    assert partial
    fits = main_bot.assess_remaining_daily_budget(
        {"remaining_usd": 325.12, "used_usd": 0, "budget_usd": 325.12}, 162.56,
    )
    assert fits is None
