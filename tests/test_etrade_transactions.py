"""Read-only GET /etrade/transactions.

TESTACCTKEY123 and 9990001111 are fixtures. Nothing in this module is a
real account number, token, or RORK secret, and the tests never call E*TRADE.
"""
from __future__ import annotations

import logging
import traceback
from datetime import date

import pytest
from fastapi.testclient import TestClient

import main_bot


FAKE_KEY = "TESTACCTKEY123"
FAKE_ACCT = "9990001111"
SECRET = "s3cret"
TXN_EPOCH = 1510624800000
POST_EPOCH = 1510711200000
CT_TXN = "2017-11-13T20:00:00-06:00"
CT_POST = "2017-11-14T20:00:00-06:00"
ROUTE_FRAMES = {
    "get_etrade_transactions",
    "_transactions_account_key",
    "_transactions_error",
    "_transactions_payload",
    "list_transactions",
}
FORBIDDEN_KEYS = {
    "accountId", "accountIdKey", "account_id", "account_id_key",
    "detailsURI", "detailsUri", "DetailsURI", "links",
}


def _run(coro):
    return main_bot.asyncio.run(coro)


def _from_route() -> bool:
    return any(frame.name in ROUTE_FRAMES for frame in traceback.extract_stack())


def _blob(records) -> str:
    parts = []
    for rec in records:
        parts.append(rec.getMessage())
        if rec.exc_text:
            parts.append(rec.exc_text)
        if rec.stack_info:
            parts.append(str(rec.stack_info))
    return "\n".join(parts)


OCC = "INTC  261127P00056000"


def _row(tid, fee, symbol, commission=0):
    return {
        "transactionId": tid,
        "accountId": FAKE_ACCT,
        "accountIdKey": FAKE_KEY,
        "transactionDate": TXN_EPOCH,
        "postDate": POST_EPOCH,
        "amount": -10.5,
        "description": f"Bought {symbol}",
        "transactionType": "Bought",
        "detailsURI": f"https://api.etrade.com/v1/accounts/{FAKE_KEY}/transactions/1.json",
        "links": [{"href": f"/v1/accounts/{FAKE_KEY}/transactions"}],
        "brokerage": {
            "product": {"symbol": symbol, "securityType": "EQ"},
            "quantity": 2,
            "price": 3.5,
            "fee": fee,
            "commission": commission,
            "settlementCurrency": "USD",
            "accountId": FAKE_ACCT,
            "accountIdKey": FAKE_KEY,
        },
    }


def _broker_text(prefix: str) -> str:
    return (
        prefix
        + " https://api.etrade.com/v1/accounts/"
        + FAKE_KEY
        + "/transactions?accountIdKey="
        + FAKE_KEY
        + " account "
        + FAKE_ACCT
        + " "
        + FAKE_KEY
    )


class _Client(main_bot.ETradeAsyncClient):
    """In-process stand-in. _request never opens a socket."""

    def __init__(self, pages=None, accounts=None):
        self.calls = []
        self.pages = list(pages or [{}])
        self.accounts = accounts

    async def list_accounts(self):
        if self.accounts is not None:
            return self.accounts
        return await super().list_accounts()

    async def _request(self, method, path, params=None, json_body=None, idempotent=True):
        self.calls.append({
            "method": method,
            "path": path,
            "params": dict(params or {}),
            "json_body": json_body,
        })
        if not self.pages:
            return {}
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return page


def _install_spies(monkeypatch, hits):
    targets = [
        ("state.set", main_bot.state, "set"),
        ("state.delete", main_bot.state, "delete"),
        ("state.sadd", main_bot.state, "sadd"),
        ("state.incr", main_bot.state, "incr"),
        ("state.queue_push", main_bot.state, "queue_push"),
        ("state.set_position", main_bot.state, "set_position"),
        ("state.delete_position", main_bot.state, "delete_position"),
        ("state.set_guard", main_bot.state, "set_guard"),
        ("state.delete_guard", main_bot.state, "delete_guard"),
        ("state.set_killed", main_bot.state, "set_killed"),
        ("ledger.record", main_bot.trade_ledger, "record"),
        ("alerts.send", main_bot.alerts, "send"),
        ("policy.patch", main_bot, "apply_policy_patch"),
        ("policy.persist", main_bot, "_persist_policy"),
        ("orders.place", main_bot, "_place_order_once"),
        ("orders.smart", main_bot, "_place_order_smart"),
        ("orders.execute", main_bot, "execute_live_order"),
        ("orders.cancel", main_bot, "_cancel_order_call"),
    ]
    for label, obj, attr in targets:
        original = getattr(obj, attr, None)
        if original is None:
            continue

        async def _inner(*args, _label=label, _orig=original, **kwargs):
            if _from_route():
                hits[_label] = hits.get(_label, 0) + 1
            return await _orig(*args, **kwargs)

        monkeypatch.setattr(obj, attr, _inner)

    for label, attr in (("pyetrade.order", "ETradeOrder"), ("pyetrade.accounts", "ETradeAccounts")):
        original = getattr(main_bot.pyetrade, attr)

        def _ctor(*args, _label=label, _orig=original, **kwargs):
            if _from_route():
                hits[_label] = hits.get(_label, 0) + 1
            return _orig(*args, **kwargs)

        monkeypatch.setattr(main_bot.pyetrade, attr, _ctor)


def _walk_keys(value):
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            found.append(key)
            found.extend(_walk_keys(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_keys(item))
    return found


def _assert_clean(text: str):
    assert FAKE_KEY not in text
    assert FAKE_ACCT not in text


@pytest.fixture
def http(monkeypatch, caplog):
    saved = {
        "state": main_bot.state,
        "key": main_bot._resolved_account_id_key,
        "tokens": main_bot._current_tokens,
        "binding": main_bot._account_binding,
        "pinned": main_bot._pinned_account_id,
    }
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", SECRET)
    monkeypatch.setattr(main_bot, "_transactions_today", lambda: date(2026, 11, 25))
    main_bot._reset_transactions_rate_limit()
    caplog.set_level(logging.DEBUG)
    holder = {"client": _Client()}

    def _factory(_tokens):
        return holder["client"]

    monkeypatch.setattr(main_bot, "_raw_client", _factory)
    with TestClient(main_bot.app) as client:
        main_bot._current_tokens = {"oauth_token": "tok", "oauth_token_secret": "sec"}
        main_bot._resolved_account_id_key = FAKE_KEY
        main_bot._pinned_account_id = FAKE_ACCT
        hits = {}
        _install_spies(monkeypatch, hits)
        caplog.clear()
        yield {"client": client, "hits": hits, "holder": holder, "caplog": caplog}
    main_bot.state = saved["state"]
    main_bot._resolved_account_id_key = saved["key"]
    main_bot._current_tokens = saved["tokens"]
    main_bot._account_binding = saved["binding"]
    main_bot._pinned_account_id = saved["pinned"]
    main_bot._reset_transactions_rate_limit()


def _use(http, pages):
    client = _Client(pages)
    http["holder"]["client"] = client
    return client


def test_bot_version_placeholder():
    assert main_bot.BOT_VERSION == "5.99.13-pro-day-trader"


def test_transactions_route_is_get_only():
    found = [r for r in main_bot.app.routes if getattr(r, "path", None) == "/etrade/transactions"]
    assert len(found) == 1
    methods = set(found[0].methods)
    assert "GET" in methods
    assert "POST" not in methods
    assert "PUT" not in methods
    assert "DELETE" not in methods


def test_transactions_require_the_rork_secret(http):
    client = http["client"]
    missing = client.get("/etrade/transactions?start=2026-11-01&end=2026-11-25")
    wrong = client.get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": "nope"},
    )
    assert missing.status_code == 401
    assert wrong.status_code == 401
    _assert_clean(missing.text)
    _assert_clean(wrong.text)
    main_bot.WEBHOOK_SECRET = ""
    closed = client.get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert closed.status_code == 401
    _assert_clean(closed.text)
    assert http["holder"]["client"].calls == []
    assert http["hits"] == {}


def test_transactions_rate_limit(http, monkeypatch):
    clock = {"now": 1_000_000.0}
    monkeypatch.setattr(main_bot, "_transactions_now", lambda: clock["now"])
    monkeypatch.setattr(main_bot, "TRANSACTIONS_RATE_LIMIT", 6)
    broker = _use(http, [{}] * 8)
    headers = {"X-Rork-Secret": SECRET}
    url = "/etrade/transactions?start=2026-11-01&end=2026-11-25"
    codes = [http["client"].get(url, headers=headers).status_code for _ in range(6)]
    assert codes == [200] * 6
    blocked = http["client"].get(url, headers=headers)
    assert blocked.status_code == 429
    assert "rate limit" in blocked.json()["detail"]
    _assert_clean(blocked.text)
    assert len(broker.calls) == 6
    clock["now"] += 60.0
    again = http["client"].get(url, headers=headers)
    assert again.status_code == 200
    assert len(broker.calls) == 7
    assert http["hits"] == {}


def test_transaction_dates_are_validated(http, monkeypatch):
    monkeypatch.setattr(main_bot, "TRANSACTIONS_RATE_LIMIT", 50)
    broker = _use(http, [{}] * 5)
    headers = {"X-Rork-Secret": SECRET}

    def get(query):
        return http["client"].get("/etrade/transactions" + query, headers=headers)

    missing = get("?end=2026-11-25")
    assert missing.status_code == 400
    assert "required" in missing.json()["detail"]
    bad = get("?start=11/01/2026&end=2026-11-25")
    assert bad.status_code == 400
    assert "YYYY-MM-DD" in bad.json()["detail"]
    poisoned = get("?start=" + FAKE_KEY + "&end=2026-11-25")
    assert poisoned.status_code == 400
    _assert_clean(poisoned.text)
    backwards = get("?start=2026-11-26&end=2026-11-25")
    assert backwards.status_code == 400
    assert "on or before end" in backwards.json()["detail"]
    future = get("?start=2026-11-01&end=2026-11-26")
    assert future.status_code == 400
    assert "today" in future.json()["detail"]
    wide = get("?start=2024-11-24&end=2026-11-25")
    assert wide.status_code == 400
    assert "2 years" in wide.json()["detail"]
    exact = get("?start=2024-11-25&end=2026-11-25")
    assert exact.status_code == 200
    assert exact.json()["count"] == 0
    monkeypatch.setattr(main_bot, "_transactions_today", lambda: date(2026, 3, 1))
    leap_ok = get("?start=2024-02-29&end=2026-02-28")
    assert leap_ok.status_code == 200, leap_ok.text
    leap_wide = get("?start=2024-02-29&end=2026-03-01")
    assert leap_wide.status_code == 400
    assert "2 years" in leap_wide.json()["detail"]
    assert len(broker.calls) == 2
    for call in broker.calls:
        assert call["method"] == "GET"
        assert call["json_body"] is None
    assert http["hits"] == {}
    _assert_clean(wide.text + future.text + backwards.text)


def test_transactions_page_sum_fees_and_strip_account_ids(http, caplog):
    broker = _use(http, [
        {"TransactionListResponse": {
            "Transaction": [_row(2128929910, 0.50, "AAPL", commission=0.15)],
            "next": "marker-2",
        }},
        {"TransactionListResponse": {
            "Transaction": [_row(FAKE_KEY, 0.25, "MSFT")],
            "next": "marker-3",
        }},
        {"TransactionListResponse": {
            "Transaction": [_row(FAKE_ACCT, 0.10, "IBM", commission=0.05)],
        }},
    ])
    caplog.clear()
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {
        "ok", "bot_version", "start", "end", "timezone",
        "count", "fee", "commission", "fee_total", "truncated", "transactions",
    }
    assert body["ok"] is True
    assert body["bot_version"] == "5.99.13-pro-day-trader"
    assert body["start"] == "2026-11-01"
    assert body["end"] == "2026-11-25"
    assert body["timezone"] == "America/Chicago"
    assert body["count"] == 3
    assert body["fee"] == 0.85
    assert body["commission"] == 0.20
    assert body["fee_total"] == 1.05
    assert body["truncated"] is False
    first, second, third = body["transactions"]
    assert set(first) == {
        "transactionId", "transactionDate", "postDate", "amount",
        "description", "transactionType", "brokerage",
    }
    assert set(first["brokerage"]) == {
        "symbol", "quantity", "price", "fee", "commission", "settlementCurrency",
    }
    assert first["transactionId"] == 2128929910
    assert second["transactionId"] is None
    assert third["transactionId"] is None
    assert first["transactionDate"] == CT_TXN
    assert first["postDate"] == CT_POST
    assert first["amount"] == -10.5
    assert first["transactionType"] == "Bought"
    assert first["brokerage"] == {
        "symbol": "AAPL",
        "quantity": 2.0,
        "price": 3.5,
        "fee": 0.5,
        "commission": 0.15,
        "settlementCurrency": "USD",
    }
    assert "marker-2" not in response.text
    forbidden = set(_walk_keys(body))
    assert forbidden.isdisjoint(FORBIDDEN_KEYS)
    _assert_clean(response.text)
    _assert_clean(_blob(caplog.records))
    assert len(broker.calls) == 3
    assert [call["method"] for call in broker.calls] == ["GET", "GET", "GET"]
    assert [call["path"] for call in broker.calls] == [
        f"/v1/accounts/{FAKE_KEY}/transactions",
    ] * 3
    assert broker.calls[0]["params"] == {
        "startDate": "11012026", "endDate": "11252026", "count": 50,
    }
    assert broker.calls[1]["params"]["marker"] == "marker-2"
    assert broker.calls[2]["params"]["marker"] == "marker-3"
    assert all(call["params"]["count"] == 50 for call in broker.calls)
    assert all(call["json_body"] is None for call in broker.calls)
    assert http["hits"] == {}


def test_page_cap_stops_at_40():
    client = _Client()

    async def _always_more(method, path, params=None, json_body=None, idempotent=True):
        client.calls.append(params)
        n = len(client.calls)
        return {"TransactionListResponse": {"Transaction": [], "next": "m" + str(n)}}

    client._request = _always_more
    result = _run(client.list_transactions(FAKE_KEY, "11012026", "11252026", page_cap=100))
    assert result["pages"] == 40
    assert result["truncated"] is True
    assert len(client.calls) == 40


def test_empty_204_is_an_empty_list(http, caplog):
    broker = _use(http, [{}])
    caplog.clear()
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["transactions"] == []
    assert body["count"] == 0
    assert body["fee"] == 0
    assert body["commission"] == 0
    assert body["fee_total"] == 0
    assert body["truncated"] is False
    assert len(broker.calls) == 1
    _assert_clean(response.text)
    _assert_clean(_blob(caplog.records))

    raised = _use(http, [main_bot.ETradeAPIError(204, _broker_text("no content"), body=_broker_text("no content"))])
    caplog.clear()
    via_status = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert via_status.status_code == 200
    assert via_status.json()["transactions"] == []
    assert via_status.json()["count"] == 0
    _assert_clean(via_status.text)
    _assert_clean(_blob(caplog.records))
    assert http["hits"] == {}
    assert len(raised.calls) == 1


def test_auth_error_is_503_and_scrubbed(http, caplog):
    _use(http, [main_bot.ETradeAPIError(401, _broker_text("401 unauthorized"), body=_broker_text("401 unauthorized"))])
    caplog.clear()
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 503
    body = response.json()
    assert body["reason"] == "etrade_auth"
    assert body["detail"] == "etrade_auth"
    assert "transactions" not in body
    assert "Traceback" not in _blob(caplog.records)
    _assert_clean(response.text)
    _assert_clean(_blob(caplog.records))
    assert "E*TRADE transactions failed" in _blob(caplog.records)

    _use(http, [RuntimeError("token expired " + FAKE_KEY + " " + FAKE_ACCT)])
    caplog.clear()
    expired = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert expired.status_code == 503
    assert expired.json()["reason"] == "etrade_auth"
    _assert_clean(expired.text)
    _assert_clean(_blob(caplog.records))
    assert http["hits"] == {}


def test_broker_error_is_502_not_an_empty_list(http, caplog):
    _use(http, [main_bot.ETradeAPIError(500, _broker_text("broker failure"), body=_broker_text("broker failure"))])
    caplog.clear()
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 502
    body = response.json()
    assert body["reason"] == "etrade_unavailable"
    assert body["detail"] == "etrade_unavailable"
    assert "transactions" not in body
    assert "ok" not in body
    _assert_clean(response.text)
    _assert_clean(body["reason"])
    _assert_clean(body["detail"])
    _assert_clean(_blob(caplog.records))
    assert http["hits"] == {}


def test_mid_page_error_is_not_returned_as_a_partial_list(http):
    broker = _use(http, [
        {"TransactionListResponse": {
            "Transaction": [_row(2128929910, 1.0, "AAPL")],
            "next": "marker-2",
        }},
        main_bot.ETradeAPIError(500, "broker down " + FAKE_KEY, body=FAKE_ACCT),
    ])
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 502
    assert "transactions" not in response.json()
    assert response.json().get("count") is None
    _assert_clean(response.text)
    assert len(broker.calls) == 2
    assert http["hits"] == {}


def test_uncached_account_resolution_does_not_write_or_stick(http):
    main_bot._resolved_account_id_key = None
    accounts = {"AccountListResponse": {"Accounts": {"Account": [{
        "accountId": FAKE_ACCT,
        "accountIdKey": FAKE_KEY,
        "accountStatus": "ACTIVE",
        "accountDesc": "Individual Brokerage",
        "accountType": "INDIVIDUAL",
        "institutionType": "BROKERAGE",
    }]}}}
    broker = _Client([{}], accounts=accounts)
    http["holder"]["client"] = broker
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 200, response.text
    assert response.json()["count"] == 0
    _assert_clean(response.text)
    assert main_bot._resolved_account_id_key is None
    assert http["hits"] == {}
    assert broker.calls and broker.calls[0]["path"].endswith("/transactions")


def test_route_write_is_visible_to_the_spy(http, monkeypatch):
    async def _noisy(tokens):
        await main_bot.state.set("transactions:should-not", "1")
        return FAKE_KEY

    monkeypatch.setattr(main_bot, "_transactions_account_key", _noisy)
    _use(http, [{}])
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 200
    assert http["hits"].get("state.set") == 1


def test_occ_symbol_string_id_and_description_survive(http):
    description = "opened " + OCC
    string_id = "TXN-261127P00056000"
    _use(http, [{"TransactionListResponse": {"Transaction": [
        {
            "transactionId": 12345678,
            "transactionDate": TXN_EPOCH,
            "amount": 1,
            "description": description,
            "transactionType": "Sold",
            "brokerage": {
                "product": {"symbol": OCC},
                "quantity": 1,
                "price": 0.56,
                "settlementCurrency": "USD",
            },
        },
        {
            "transactionId": string_id,
            "accountId": FAKE_ACCT,
            "transactionDate": TXN_EPOCH,
            "amount": 2,
            "description": FAKE_ACCT,
            "transactionType": "Bought",
            "brokerage": {
                "product": {"symbol": OCC},
                "quantity": 1,
                "price": 0.56,
                "fee": 1.25,
                "commission": 0.75,
                "settlementCurrency": "USD",
            },
        },
    ]}}])
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 200, response.text
    plain, masked = response.json()["transactions"]
    assert plain["transactionId"] == 12345678
    assert plain["brokerage"]["symbol"] == OCC
    assert plain["description"] == description
    assert plain["brokerage"]["symbol"].encode("utf-8") == OCC.encode("utf-8")
    assert plain["description"].encode("utf-8") == description.encode("utf-8")
    assert masked["transactionId"] == string_id
    assert masked["transactionId"].encode("utf-8") == string_id.encode("utf-8")
    assert masked["brokerage"]["symbol"].encode("utf-8") == OCC.encode("utf-8")
    assert masked["description"] == "****1111"
    assert FAKE_ACCT not in response.text
    assert FAKE_KEY not in response.text
    assert "accountId" not in _walk_keys(response.json())
    assert response.json()["fee"] == 1.25
    assert response.json()["commission"] == 0.75
    assert response.json()["fee_total"] == 2.0


def test_uncached_list_accounts_failure_hides_bare_key_and_hint(http, caplog):
    main_bot._resolved_account_id_key = None
    main_bot._account_binding = {
        "resolved_account_id_key": FAKE_KEY,
        "resolved_account_id": FAKE_ACCT,
        "pinned": FAKE_ACCT,
    }

    class _Boom:
        async def list_accounts(self):
            raise RuntimeError("lookup failed ?hint=" + FAKE_KEY + " " + FAKE_KEY)

    http["holder"]["client"] = _Boom()
    caplog.clear()
    response = http["client"].get(
        "/etrade/transactions?start=2026-11-01&end=2026-11-25",
        headers={"X-Rork-Secret": SECRET},
    )
    assert response.status_code == 502
    assert response.json()["reason"] == "etrade_unavailable"
    assert response.json()["detail"] == "etrade_unavailable"
    blob = _blob(caplog.records)
    assert FAKE_KEY not in response.text
    assert FAKE_ACCT not in response.text
    assert FAKE_KEY not in blob
    assert FAKE_ACCT not in blob
    assert "Traceback" not in blob
    assert "RuntimeError" in blob
    assert main_bot._resolved_account_id_key is None
    assert http["hits"] == {}


def test_transaction_rate_limit_is_per_ip_and_evicts_idle_keys(monkeypatch):
    clock = {"now": 5_000.0}
    monkeypatch.setattr(main_bot, "_transactions_now", lambda: clock["now"])
    monkeypatch.setattr(main_bot, "TRANSACTIONS_RATE_LIMIT", 6)
    main_bot._reset_transactions_rate_limit()
    try:
        for _ in range(6):
            assert main_bot._transactions_rate_allow("203.0.113.10") is True
        assert main_bot._transactions_rate_allow("203.0.113.10") is False
        assert main_bot._transactions_rate_allow("203.0.113.11") is True
        assert "203.0.113.10" in main_bot._TXN_RATE_HITS
        clock["now"] += 60
        assert main_bot._transactions_rate_allow("203.0.113.12") is True
        assert "203.0.113.10" not in main_bot._TXN_RATE_HITS
        assert "203.0.113.11" not in main_bot._TXN_RATE_HITS
        assert list(main_bot._TXN_RATE_HITS) == ["203.0.113.12"]
    finally:
        main_bot._reset_transactions_rate_limit()
