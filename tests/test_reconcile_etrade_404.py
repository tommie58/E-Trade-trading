"""E*TRADE reconcile reads: documented empty portfolio vs a real 404.

Broker HTTP is mocked. Nothing in these tests places an order.
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import pytest

import main_bot
import etrade_async

ACCT = "823456789"
TOKEN = "oauth-canary-9f3a7c"
SECRET = "secret-canary-1b2c3d"
TOKEN_SECRET = "oauth-secret-canary-8844"
PORTFOLIO = "/v1/accounts/{acct}/portfolio.json"
ORDERS = "/v1/accounts/{acct}/orders.json"


def _run(coro):
    return asyncio.run(coro)


def _logs(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records)


def _assert_no_secrets(text: str) -> None:
    assert ACCT not in text
    assert TOKEN not in text
    assert SECRET not in text
    assert TOKEN_SECRET not in text
    assert "Authorization" not in text or "Authorization: [redacted]" in text


def _client() -> etrade_async.ETradeAsyncClient:
    return etrade_async.ETradeAsyncClient(
        "consumer-key", SECRET, TOKEN, TOKEN_SECRET, sandbox=False,
    )


def _install_http(monkeypatch, responses):
    """responses is a list of (status, body) consumed in order."""
    seen = []

    class _Resp:
        def __init__(self, status, text):
            self.status_code = status
            self.text = text

        def json(self):
            return json.loads(self.text) if self.text else {}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def request(self, method, url, content=None, headers=None):
            status, text = responses.pop(0)
            seen.append({"method": method, "url": url, "headers": dict(headers or {})})
            return _Resp(status, text)

    monkeypatch.setattr(etrade_async.httpx, "AsyncClient", _Client)
    return seen


def _reset_alert_clock(monkeypatch, now=1_000.0):
    etrade_async._ETRADE_ALERT_AT.clear()
    clock = {"now": now}
    monkeypatch.setattr(etrade_async.time, "monotonic", lambda: clock["now"])
    return clock


async def _capture_alerts():
    sent = []

    async def send(severity, event, message, dedupe_key=None, dedupe_seconds=None, **kwargs):
        sent.append({
            "severity": severity,
            "event": event,
            "message": message,
            "dedupe_key": dedupe_key,
            "dedupe_seconds": dedupe_seconds,
        })

    return sent, send


def _aged_option(qty=1, stop_id="STOP-1", strike=400):
    opened = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    return {
        "ts": opened,
        "qty": qty,
        "filled_qty": qty,
        "action": "BUY",
        "entry_premium": 2.0,
        "stop_premium": 1.5,
        "stop_order_id": stop_id,
        "contract": {"right": "CALL", "strike": strike, "expiration": "2026-12-19"},
    }


def _portfolio_position(symbol, qty, strike):
    return {
        "quantity": qty,
        "Product": {
            "symbol": symbol,
            "securityType": "OPTN",
            "callPut": "CALL",
            "strikePrice": strike,
            "expiryYear": 2026,
            "expiryMonth": 12,
            "expiryDay": 19,
        },
    }


def _filled_stop(order_id="STOP-1", symbol="SPY", price=1.4):
    return {
        "OrdersResponse": {
            "Order": {
                "orderId": order_id,
                "OrderDetail": {
                    "status": "EXECUTED",
                    "priceType": "STOP",
                    "stopPrice": 1.5,
                    "executedTime": int(time.time() * 1000),
                    "Instrument": {
                        "orderAction": "SELL_CLOSE",
                        "orderedQuantity": 1,
                        "filledQuantity": 1,
                        "averageExecutionPrice": price,
                        "Product": {"symbol": symbol, "securityType": "OPTN"},
                    },
                },
            }
        }
    }


def _open_stop(order_id, symbol):
    return {
        "orderId": order_id,
        "OrderDetail": {
            "status": "OPEN",
            "priceType": "STOP",
            "stopPrice": 1.5,
            "Instrument": {
                "orderAction": "SELL_CLOSE",
                "orderedQuantity": 1,
                "filledQuantity": 0,
                "Product": {"symbol": symbol, "securityType": "OPTN"},
            },
        },
    }


async def _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert):
    async def cancel(order_id):
        sinks["cancels"].append(order_id)
        return True

    async def rearm(ticker, pos):
        sinks["rearms"].append(ticker)

    async def record_close(ticker, pos, resolution):
        sinks["closes"].append((ticker, resolution))

    return await main_bot.reconciliation.reconcile_once(
        store, fetch_portfolio, fetch_orders, cancel,
        rearm_guard=rearm, record_close=record_close, alert=alert,
    )


def test_documented_portfolio_204_code_1007_is_an_empty_list(monkeypatch, caplog):
    """HTTP 204 + error code 1007 is the documented empty portfolio.

    Source: https://apisb.etrade.com/docs/api/account/api-portfolio-v1.html
    ("204 / 1007 — Portfolio Information service call success with no data").
    An empty 204 body is the same status with no error document.
    """
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    body = json.dumps({
        "Error": {
            "code": 1007,
            "message": (
                "Portfolio Information service call success with no data. "
                f"Account {ACCT}."
            ),
        }
    })
    seen = _install_http(monkeypatch, [(204, body), (204, "")])

    async def scenario():
        sent, send = await _capture_alerts()
        monkeypatch.setattr("alerts.send", send)
        client = _client()
        documented = await client.get_portfolio(ACCT)
        empty_body = await client.get_portfolio(ACCT)
        store = main_bot.StateStore(None)
        sinks = {"closes": [], "rearms": [], "cancels": []}

        async def fetch_portfolio():
            return documented

        async def fetch_orders():
            return {"OrdersResponse": {}}

        async def alert(*args, **kwargs):
            sent.append({"via": "reconcile", "args": args, "kwargs": kwargs})

        report = await _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert)
        return documented, empty_body, report, sent, sinks

    documented, empty_body, report, sent, sinks = _run(scenario())
    assert documented == etrade_async.empty_portfolio_response()
    assert empty_body == etrade_async.empty_portfolio_response()
    assert main_bot.reconciliation.parse_broker_positions(documented) == {}
    assert main_bot.reconciliation.parse_open_orders({"OrdersResponse": {}}) == []
    assert report["ok"] is True
    assert report["portfolio_ok"] is True
    assert report["orders_ok"] is True
    assert report["broker_positions"] == 0
    assert report["fill_booking"] == "ran"
    assert sinks["closes"] == []
    assert sinks["rearms"] == []
    assert sinks["cancels"] == []
    assert sent == []
    assert "documented no-data, empty list" in _logs(caplog)
    assert "1007" in _logs(caplog)
    _assert_no_secrets(_logs(caplog))
    assert seen[0]["headers"]["Authorization"]
    assert TOKEN not in _logs(caplog)
    assert seen[0]["headers"]["Authorization"] not in _logs(caplog)


def test_portfolio_204_code_53_is_empty_and_153_is_not(monkeypatch, caplog):
    """204/53 is the other documented no-data row. 204/153 is missing lots."""
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    empty = json.dumps({"Error": {"code": 53, "message": f"Data not found {ACCT}"}})
    lots = json.dumps({
        "Error": {"code": 153, "message": f"PositionLots not found {ACCT}"},
    })
    _install_http(monkeypatch, [(204, empty), (204, lots)])

    async def scenario():
        sent, send = await _capture_alerts()
        monkeypatch.setattr("alerts.send", send)
        client = _client()
        parsed = await client.get_portfolio(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError) as exc:
            await client.get_portfolio(ACCT)
        await etrade_async.drain_etrade_alerts()
        return parsed, exc.value, sent

    parsed, err, sent = _run(scenario())
    assert main_bot.reconciliation.parse_broker_positions(parsed) == {}
    assert err.status_code == 204
    assert err.code == "153"
    assert err.path == PORTFOLIO
    assert len(sent) == 1
    text = _logs(caplog)
    assert f"E*TRADE GET {PORTFOLIO} -> 204" in text
    _assert_no_secrets(text)
    _assert_no_secrets(str(err))


def test_generic_404_is_a_failure_logged_with_method_and_path(monkeypatch, caplog):
    """HTTP 404 is not the no-data signature, even when the body says so.

    Code 1007 is documented only with HTTP 204. HTTP 400 + error code 404 is
    "Resource not found", also a failure. List Orders has no empty-book 404.
    """
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    no_positions = json.dumps({
        "Error": {"code": 1007, "message": f"no positions for account {ACCT}"},
    })
    resource = json.dumps({
        "Error": {"code": 404, "message": f"Resource not found {ACCT}"},
    })
    orders_404 = json.dumps({
        "Error": {"code": 999, "message": f"Account {ACCT} blew up"},
    })
    seen = _install_http(monkeypatch, [
        (404, no_positions),
        (400, resource),
        (404, orders_404),
    ])

    async def scenario():
        sent, send = await _capture_alerts()
        monkeypatch.setattr("alerts.send", send)
        client = _client()
        with pytest.raises(etrade_async.ETradeAPIError) as first:
            await client.get_portfolio(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError) as second:
            await client.get_portfolio(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError) as third:
            await client.list_orders(ACCT)
        await etrade_async.drain_etrade_alerts()
        return first.value, second.value, third.value, sent

    portfolio_404, resource_404, orders_err, sent = _run(scenario())

    assert portfolio_404.status_code == 404
    assert portfolio_404.method == "GET"
    assert portfolio_404.path == PORTFOLIO
    assert portfolio_404.code == "1007"
    assert resource_404.status_code == 400
    assert resource_404.code == "404"
    assert orders_err.status_code == 404
    assert orders_err.method == "GET"
    assert orders_err.path == ORDERS
    assert orders_err.code == "999"

    text = _logs(caplog)
    assert f"E*TRADE GET {PORTFOLIO} -> 404" in text
    assert "[1007]" in text
    assert f"E*TRADE GET {PORTFOLIO} -> 400" in text
    assert "[404]" in text
    assert f"E*TRADE GET {ORDERS} -> 404" in text
    assert "[999]" in text
    assert "blew up" in text
    _assert_no_secrets(text)
    _assert_no_secrets(str(portfolio_404) + str(resource_404) + str(orders_err))
    for call in seen:
        assert call["headers"]["Authorization"] not in text
    _assert_no_secrets(text)
    assert all(item["dedupe_seconds"] == 15 * 60 for item in sent)
    assert any(item["dedupe_key"] == f"etrade:GET {PORTFOLIO}" for item in sent)
    assert any(item["dedupe_key"] == f"etrade:GET {ORDERS}" for item in sent)
    for item in sent:
        _assert_no_secrets(item["message"] + str(item["dedupe_key"]))


def test_orders_failure_defers_stop_fill_and_still_uses_portfolio(monkeypatch, caplog):
    """orders.json 404 while portfolio.json succeeds.

    Stop-fill booking is deferred: the fill lives in the orders read, so a
    missing position is not booked and not deleted. The good portfolio is
    still applied (filled quantity sync). Re-arm and cancel do not run.
    """
    caplog.set_level(logging.DEBUG)
    etrade_async._ETRADE_ALERT_AT.clear()

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", _aged_option(qty=1, stop_id="STOP-1", strike=500))
        await store.set_position("QQQ", _aged_option(qty=1, stop_id="STOP-2", strike=400))
        await store.set(main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY, "SENTINEL")
        sinks = {"closes": [], "rearms": [], "cancels": []}
        alerts = []

        async def fetch_portfolio():
            return {
                "PortfolioResponse": {
                    "AccountPortfolio": {
                        "Position": _portfolio_position("QQQ", 4, 400),
                    }
                }
            }

        async def fetch_orders():
            raise etrade_async.ETradeAPIError(
                404,
                f"Account {ACCT} missing",
                body=json.dumps({"Error": {"code": 404, "message": f"Account {ACCT} missing"}}),
                method="GET",
                path=f"/v1/accounts/{ACCT}/orders.json",
                code="404",
            )

        async def alert(severity, event, message, dedupe_key=None, dedupe_seconds=None):
            alerts.append({
                "severity": severity,
                "event": event,
                "message": message,
                "dedupe_key": dedupe_key,
                "dedupe_seconds": dedupe_seconds,
            })

        report = await _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert)
        await etrade_async.drain_etrade_alerts()
        positions = await store.all_positions()
        snapshot = await store.get(main_bot.reconciliation.ACCOUNT_SNAPSHOT_KEY)
        return report, positions, sinks, alerts, snapshot

    report, positions, sinks, alerts, snapshot = _run(scenario())
    assert "SPY" in positions
    assert positions["SPY"]["filled_qty"] == 1
    assert positions["QQQ"]["filled_qty"] == 4
    assert sinks["closes"] == []
    assert sinks["rearms"] == []
    assert sinks["cancels"] == []
    assert report["ok"] is False
    assert report["portfolio_ok"] is True
    assert report["orders_ok"] is False
    assert report["fill_booking"] == "deferred"
    assert report["broker_positions"] == 1
    assert any("filled_qty 1 → 4" in item for item in report["healed"])
    assert any("stop-fill booking deferred" in item for item in report["warnings"])
    assert snapshot == "SENTINEL"
    text = _logs(caplog) + json.dumps(report) + json.dumps(alerts)
    assert f"E*TRADE GET {ORDERS} -> 404" in text
    assert alerts and alerts[0]["event"] == "etrade_http_error"
    assert alerts[0]["dedupe_key"] == f"etrade:GET {ORDERS}"
    assert alerts[0]["dedupe_seconds"] == 15 * 60
    _assert_no_secrets(text)


def test_portfolio_failure_does_not_book_cancel_or_rearm(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    etrade_async._ETRADE_ALERT_AT.clear()

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", _aged_option())
        await store.set_guard("SPY", {
            "done": False,
            "entry_order_id": "ENTRY-1",
            "deadline_ts": time.time() - 10_000,
        })
        sinks = {"closes": [], "rearms": [], "cancels": []}

        async def fetch_portfolio():
            raise etrade_async.ETradeAPIError(
                404, f"no book {ACCT}", method="GET",
                path=f"/v1/accounts/{ACCT}/portfolio.json", code="404",
            )

        async def fetch_orders():
            return {"OrdersResponse": {"Order": _open_stop("ORPHAN-1", "AAPL")}}

        async def alert(severity, event, message, dedupe_key=None, dedupe_seconds=None):
            sinks.setdefault("alerts", []).append(message)

        report = await _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert)
        positions = await store.all_positions()
        guards = await store.all_guards()
        return report, positions, guards, sinks

    report, positions, guards, sinks = _run(scenario())
    assert "SPY" in positions
    assert guards["SPY"]["done"] is False
    assert sinks["closes"] == []
    assert sinks["rearms"] == []
    assert sinks["cancels"] == []
    assert report["fill_booking"] == "deferred"
    assert report["portfolio_ok"] is False
    assert report["orders_ok"] is True
    text = _logs(caplog) + json.dumps(report)
    assert f"E*TRADE GET {PORTFOLIO} -> 404" in text
    _assert_no_secrets(text)


def test_both_reads_still_book_a_stop_fill_and_rearm_once(monkeypatch):
    """A good pair of reads still books a stop fill, and a naked position
    is handed to the existing re-arm callback exactly once."""

    async def book():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", _aged_option(stop_id="STOP-1", strike=500))
        sinks = {"closes": [], "rearms": [], "cancels": []}

        async def fetch_portfolio():
            return {"PortfolioResponse": {"AccountPortfolio": []}}

        async def fetch_orders():
            return _filled_stop()

        async def alert(*args, **kwargs):
            return None

        report = await _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert)
        return report, sinks

    report, sinks = _run(book())
    assert report["ok"] is True
    assert report["fill_booking"] == "ran"
    assert len(sinks["closes"]) == 1
    ticker, resolution = sinks["closes"][0]
    assert ticker == "SPY"
    assert resolution["kind"] == main_bot.reconciliation.GHOST_CLOSED
    assert resolution["exit_price"] == 1.4
    assert sinks["rearms"] == []
    assert sinks["cancels"] == []
    assert any("broker-side fill captured" in item for item in report["healed"])
    assert not any("ghost position removed" in item for item in report["healed"])

    async def rearm_once():
        store = main_bot.StateStore(None)
        await store.set_position("QQQ", _aged_option(qty=1, stop_id=None, strike=400))
        sinks = {"closes": [], "rearms": [], "cancels": []}

        async def fetch_portfolio():
            return {
                "PortfolioResponse": {
                    "AccountPortfolio": {"Position": _portfolio_position("QQQ", 1, 400)},
                }
            }

        async def fetch_orders():
            return {"OrdersResponse": {}}

        async def alert(*args, **kwargs):
            return None

        report = await _reconcile(store, fetch_portfolio, fetch_orders, sinks, alert)
        return report, sinks

    report, sinks = _run(rearm_once())
    assert report["ok"] is True
    assert sinks["rearms"] == ["QQQ"]
    assert sinks["closes"] == []
    assert sinks["cancels"] == []


def test_etrade_error_alert_is_one_per_endpoint_per_15_min(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    clock = _reset_alert_clock(monkeypatch, now=5_000.0)
    body = json.dumps({"Error": {"code": 404, "message": f"missing {ACCT}"}})
    _install_http(monkeypatch, [(404, body)] * 6)

    async def scenario():
        sent, send = await _capture_alerts()
        monkeypatch.setattr("alerts.send", send)
        client = _client()
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.get_portfolio(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.get_portfolio(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.list_orders(ACCT)
        clock["now"] = 5_000.0 + (15 * 60) - 1
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.get_portfolio(ACCT)
        clock["now"] = 5_000.0 + (15 * 60)
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.get_portfolio(ACCT)
        await etrade_async.drain_etrade_alerts()
        return sent

    sent = _run(scenario())
    keys = [item["dedupe_key"] for item in sent]
    assert keys == [
        f"etrade:GET {PORTFOLIO}",
        f"etrade:GET {ORDERS}",
        f"etrade:GET {PORTFOLIO}",
    ]
    assert all(item["dedupe_seconds"] == 15 * 60 for item in sent)
    text = _logs(caplog)
    assert text.count(f"E*TRADE GET {PORTFOLIO} -> 404") == 4
    _assert_no_secrets(text)
    for item in sent:
        _assert_no_secrets(item["message"])


def test_no_account_number_in_any_log_output(monkeypatch, caplog):
    """The account id is in the path, the body, and the OAuth header."""
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    body = json.dumps({
        "Error": {"code": 404, "message": f"Account {ACCT} not found"},
    })
    seen = _install_http(monkeypatch, [(404, body)])

    async def scenario():
        sent, send = await _capture_alerts()
        monkeypatch.setattr("alerts.send", send)
        client = _client()
        with pytest.raises(etrade_async.ETradeAPIError):
            await client.get_portfolio(ACCT)
        await etrade_async.drain_etrade_alerts()
        return sent

    sent = _run(scenario())
    text = _logs(caplog)
    _assert_no_secrets(text)
    assert seen[0]["url"].count(ACCT) >= 1
    assert ACCT not in text
    assert seen[0]["headers"]["Authorization"] not in text
    assert TOKEN not in text
    assert f"E*TRADE GET {PORTFOLIO} -> 404" in text
    _assert_no_secrets(sent[0]["message"])
    _assert_no_secrets(sent[0]["dedupe_key"])


def test_order_path_error_does_not_call_or_wait_on_the_webhook(monkeypatch):
    """preview, place, and cancel log the failure and return without a webhook."""
    _reset_alert_clock(monkeypatch)
    body = json.dumps({"Error": {"code": 400, "message": f"rejected {ACCT}"}})
    _install_http(monkeypatch, [(400, body), (400, body), (400, body)])
    calls = {"n": 0}

    async def slow_send(*args, **kwargs):
        calls["n"] += 1
        await asyncio.sleep(1.0)

    async def scenario():
        monkeypatch.setattr("alerts.send", slow_send)
        client = _client()
        started = time.monotonic()
        for method, path in (
            ("POST", f"/v1/accounts/{ACCT}/orders/preview.json"),
            ("POST", f"/v1/accounts/{ACCT}/orders/place.json"),
            ("PUT", f"/v1/accounts/{ACCT}/orders/cancel.json"),
        ):
            with pytest.raises(etrade_async.ETradeAPIError):
                await client._request(method, path, json_body={"PreviewOrderRequest": {}}, idempotent=False)
        elapsed = time.monotonic() - started
        await asyncio.sleep(0)
        return elapsed

    elapsed = _run(scenario())
    assert calls["n"] == 0
    assert elapsed < 0.25
    assert not etrade_async._PENDING_ALERTS


def test_read_alert_is_scheduled_once_even_if_send_raises_typeerror(monkeypatch):
    """A TypeError inside the sender is not retried."""
    _reset_alert_clock(monkeypatch)
    calls = {"n": 0}

    async def send(*args, **kwargs):
        calls["n"] += 1
        raise TypeError("sender failed internally")

    async def scenario():
        await etrade_async.emit_etrade_error(
            "GET", f"/v1/accounts/{ACCT}/portfolio.json", 404, "404", f"missing {ACCT}",
            alert=send,
        )
        await etrade_async.drain_etrade_alerts()

    _run(scenario())
    assert calls["n"] == 1


def test_orders_204_code_53_or_1007_is_an_empty_book(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    code_53 = json.dumps({"Error": {"code": 53, "message": f"Data not found {ACCT}"}})
    code_1007 = json.dumps({"Error": {"code": 1007, "message": f"no orders {ACCT}"}})
    http_404 = json.dumps({"Error": {"code": 53, "message": f"no orders {ACCT}"}})
    other = json.dumps({"Error": {"code": 404, "message": f"Resource not found {ACCT}"}})
    _install_http(monkeypatch, [
        (204, code_53),
        (204, code_1007),
        (404, http_404),
        (204, other),
    ])

    async def scenario():
        client = _client()
        first = await client.list_orders(ACCT)
        second = await client.list_orders(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError) as missing:
            await client.list_orders(ACCT)
        with pytest.raises(etrade_async.ETradeAPIError) as resource:
            await client.list_orders(ACCT)
        await etrade_async.drain_etrade_alerts()
        return first, second, missing.value, resource.value

    first, second, missing, resource = _run(scenario())
    assert main_bot.reconciliation.parse_open_orders(first) == []
    assert main_bot.reconciliation.parse_open_orders(second) == []
    assert first == etrade_async.empty_orders_response()
    assert missing.status_code == 404
    assert resource.status_code == 204
    assert resource.code == "404"
    text = _logs(caplog)
    _assert_no_secrets(text)
    _assert_no_secrets(str(missing) + str(resource))


def test_long_8400_message_keeps_the_quantity_hint(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    _reset_alert_clock(monkeypatch)
    hint = "the maximum allowable quantity was estimated to be 57."
    long_msg = "insufficient funds. " + ("detail " * 80) + hint + " Code 8400."
    assert len(long_msg) > 300
    body = json.dumps({"Error": {"code": 8400, "message": f"{long_msg} account {ACCT}"}})
    _install_http(monkeypatch, [(400, body)])

    async def scenario():
        client = _client()
        with pytest.raises(etrade_async.ETradeAPIError) as exc:
            await client._request(
                "POST", f"/v1/accounts/{ACCT}/orders/place.json",
                json_body={"PlaceOrderRequest": {}}, idempotent=False,
            )
        return exc.value

    err = _run(scenario())
    assert len(err.message) > 300
    assert hint in err.message
    assert main_bot._max_qty_from_insufficient_funds(str(err)) == 57
    text = _logs(caplog)
    assert hint not in text
    assert f"E*TRADE POST /v1/accounts/{{acct}}/orders/place.json -> 400" in text
    _assert_no_secrets(text)
    _assert_no_secrets(str(err))


def test_reconcile_fetch_failed_names_tickers_escalates_and_resets():
    """Deferred booking with open positions alerts as reconcile_fetch_failed."""

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", _aged_option(qty=1, stop_id="STOP-1", strike=500))
        await store.set_position("QQQ", _aged_option(qty=1, stop_id="STOP-2", strike=400))
        alerts = []
        sinks = {"closes": [], "rearms": [], "cancels": []}

        async def fetch_portfolio():
            return {
                "PortfolioResponse": {
                    "AccountPortfolio": {
                        "Position": _portfolio_position("QQQ", 1, 400),
                    }
                }
            }

        async def fetch_orders_fail():
            raise etrade_async.ETradeAPIError(
                404, f"Account {ACCT} missing", method="GET",
                path=f"/v1/accounts/{ACCT}/orders.json", code="404",
            )

        async def fetch_portfolio_held():
            return {
                "PortfolioResponse": {
                    "AccountPortfolio": {
                        "Position": [
                            _portfolio_position("QQQ", 1, 400),
                            _portfolio_position("SPY", 1, 500),
                        ],
                    }
                }
            }

        async def fetch_orders_ok():
            return {"OrdersResponse": {"Order": [
                _open_stop("STOP-2", "QQQ"),
                _open_stop("STOP-1", "SPY"),
            ]}}

        async def alert(severity, event, message, dedupe_key=None, dedupe_seconds=None):
            alerts.append({
                "severity": severity,
                "event": event,
                "message": message,
                "dedupe_key": dedupe_key,
            })

        async def fail_once():
            return await _reconcile(store, fetch_portfolio, fetch_orders_fail, sinks, alert)

        reports = []
        for _ in range(3):
            reports.append(await fail_once())
            await etrade_async.drain_etrade_alerts()
        good = await _reconcile(store, fetch_portfolio_held, fetch_orders_ok, sinks, alert)
        await etrade_async.drain_etrade_alerts()
        reports.append(await fail_once())
        await etrade_async.drain_etrade_alerts()
        return alerts, reports, good

    alerts, reports, good = _run(scenario())
    fetch = [item for item in alerts if item["event"] == "reconcile_fetch_failed"]
    assert [item["severity"] for item in fetch] == ["error", "error", "critical", "error"]
    first = fetch[0]
    assert "orders.json" in first["message"]
    assert "portfolio.json" not in first["message"]
    assert "SPY" in first["message"] and "QQQ" in first["message"]
    assert ACCT not in first["message"]
    assert first["dedupe_key"] == "reconcile_fetch_failed:error"
    assert fetch[2]["severity"] == "critical"
    assert fetch[2]["dedupe_key"] == "reconcile_fetch_failed:critical"
    assert good["ok"] is True
    assert good["fill_booking"] == "ran"
    assert all(report["fill_booking"] == "deferred" for report in reports)
    for item in fetch:
        _assert_no_secrets(item["message"])

    async def both():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", _aged_option())
        seen = []

        async def fetch_portfolio():
            raise etrade_async.ETradeAPIError(
                404, "down", method="GET", path=PORTFOLIO, code="404",
            )

        async def fetch_orders():
            raise etrade_async.ETradeAPIError(
                404, "down", method="GET", path=ORDERS, code="404",
            )

        async def alert(severity, event, message, dedupe_key=None, dedupe_seconds=None):
            seen.append({"event": event, "message": message, "severity": severity})

        await _reconcile(
            store, fetch_portfolio, fetch_orders,
            {"closes": [], "rearms": [], "cancels": []}, alert,
        )
        await etrade_async.drain_etrade_alerts()
        return seen

    both_alerts = [item for item in _run(both()) if item["event"] == "reconcile_fetch_failed"]
    assert len(both_alerts) == 1
    assert "portfolio.json" in both_alerts[0]["message"]
    assert "orders.json" in both_alerts[0]["message"]
    assert "SPY" in both_alerts[0]["message"]
    assert both_alerts[0]["severity"] == "error"
