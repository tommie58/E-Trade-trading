"""Account id redaction. Logging and HTTP responses only.

The fake key TESTACCTKEY123 and the fake account number 146261816 are
fixtures. Order routing keeps the real value in memory.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging

import pytest
from fastapi.testclient import TestClient

import main_bot


class _Broker:
    def list_accounts(self, **kwargs):
        return None

    def get_quote(self, *args, **kwargs):
        return None


FAKE_KEY = "TESTACCTKEY123"
FAKE_ACCT = "146261816"
KEY_MASK = "****Y123"
ACCT_MASK = "****1816"
URL = (
    "500 Server Error for url: "
    "https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders.json"
    "?accountIdKey=TESTACCTKEY123 "
    'Authorization: OAuth oauth_token="sekret1234"'
)


def _run(coro):
    return asyncio.run(coro)


def _blob(records) -> str:
    parts = []
    for rec in records:
        parts.append(rec.getMessage())
        if rec.args:
            parts.append(repr(rec.args))
    return "\n".join(parts)


def test_scrub_etrade_text_matches_the_shared_signature():
    sig = inspect.signature(main_bot.scrub_etrade_text)
    assert list(sig.parameters) == ["text", "secrets"]
    assert sig.parameters["secrets"].default is None
    cleaned = main_bot.scrub_etrade_text(
        URL, secrets=["sekret1234"],
    )
    assert FAKE_KEY not in cleaned
    assert "/v1/accounts/{acct}/orders.json" in cleaned
    assert "accountIdKey={acct}" in cleaned
    assert "oauth_token=[redacted]" in cleaned
    assert "sekret1234" not in cleaned
    again = main_bot.scrub_etrade_text(cleaned)
    assert FAKE_KEY not in again
    assert "accountIdKey={acct}}" not in again


def test_pyetrade_retry_logs_omit_the_account_key(monkeypatch, caplog):
    """list_orders, cancel, and the place fallback each retry a pyetrade error."""
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(main_bot, "ETRADE_RETRY_ATTEMPTS", 2)

    async def _no_sleep(_delay):
        return None

    monkeypatch.setattr(main_bot.asyncio, "sleep", _no_sleep)

    async def _tokens_ok():
        return None

    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })

    async def _key(_tokens):
        return FAKE_KEY

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", _key)

    class _Orders:
        def list_orders(self, account_id_key, resp_format="json", **kwargs):
            assert account_id_key == FAKE_KEY
            raise RuntimeError(
                "500 Server Error for url: "
                f"https://api.etrade.com/v1/accounts/{account_id_key}/orders.json"
                f"?accountIdKey={account_id_key}"
            )

        def cancel_order(self, account_id_key, order_id, resp_format="json"):
            assert account_id_key == FAKE_KEY
            raise RuntimeError(
                "500 Server Error for url: "
                f"https://api.etrade.com/v1/accounts/{account_id_key}/orders/cancel.json"
                f"?accountIdKey={account_id_key}"
            )

        def place_equity_order(self, **kwargs):
            assert kwargs.get("accountIdKey") == FAKE_KEY
            raise RuntimeError(
                "429 Too Many Requests for url: "
                "https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders/place.json"
                "?accountIdKey=TESTACCTKEY123 "
                'Authorization: OAuth oauth_token="sekret1234"'
            )

    class _RawCancel:
        async def cancel_order(self, account_id_key, order_id):
            assert account_id_key == FAKE_KEY
            raise main_bot.ETradeAPIError(503, "unavailable")

    class _RawPlace:
        async def place_equity(self, *args, **kwargs):
            assert args[0] == FAKE_KEY
            raise main_bot.ETradeAPIError(400, "rejected before send")

    monkeypatch.setattr(main_bot, "_orders_client", lambda _tokens: _Orders())

    async def scenario():
        monkeypatch.setattr(main_bot, "_raw_client", lambda _tokens: _RawCancel())
        with pytest.raises(RuntimeError):
            await main_bot._order_state("1", None)
        with pytest.raises(RuntimeError):
            await main_bot._cancel_order_call("44")
        monkeypatch.setattr(main_bot, "_raw_client", lambda _tokens: _RawPlace())
        with pytest.raises(RuntimeError):
            await main_bot._place_order_once("equity", {
                "accountIdKey": FAKE_KEY,
                "clientOrderId": "cid1",
                "symbol": "SPY",
                "orderAction": "BUY",
                "quantity": 1,
                "priceType": "MARKET",
            }, {"oauth_token": "tok", "oauth_token_secret": "sec"})

    _run(scenario())
    blob = _blob(caplog.records)
    assert FAKE_KEY not in blob
    assert "sekret1234" not in blob
    assert blob.count("retrying in") >= 3
    assert "/v1/accounts/{acct}/" in blob


def test_async_client_logger_scrubs_the_account_path(caplog):
    caplog.set_level(logging.DEBUG)
    logging.getLogger("etrade-bot.async-client").warning(
        "E*TRADE 500 on GET /v1/accounts/TESTACCTKEY123/orders.json "
        "?accountIdKey=TESTACCTKEY123 Authorization: OAuth oauth_token=\"sekret1234\""
    )
    blob = _blob(caplog.records)
    assert FAKE_KEY not in blob
    assert "sekret1234" not in blob
    assert "/v1/accounts/{acct}/orders.json" in blob


def test_status_masks_the_resolved_key_and_keeps_it_internally(monkeypatch):
    saved_state = main_bot.state
    saved_key = main_bot._resolved_account_id_key
    saved_binding = main_bot._account_binding
    saved_crypto = main_bot._crypto_account_binding
    store = main_bot.StateStore(None)
    main_bot.state = store
    binding = {
        "configured": FAKE_ACCT,
        "pinned": FAKE_ACCT,
        "source": "env",
        "resolved_account_id": FAKE_ACCT,
        "resolved_account_id_key": FAKE_KEY,
        "resolved_last4": "1816",
    }
    crypto_binding = {
        "resolved_account_id": "8881038595",
        "resolved_account_id_key": FAKE_KEY,
        "resolved_last4": "8595",
    }

    async def quotes(symbols, *, dry_run=False):
        return {"QuoteResponse": {"QuoteData": []}}

    monkeypatch.setattr(main_bot, "_scanner_fetch_quotes", quotes)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "TARGET_CRYPTO_ACCOUNT_ID", FAKE_ACCT)

    held = {}

    async def seed_live():
        await main_bot.state.set_position("ZZRED", {
            "qty": 1,
            "accountId": FAKE_ACCT,
            "accountIdKey": FAKE_KEY,
        })
        await main_bot.state.set("reconcile:last", json.dumps({
            "ok": False,
            "warnings": [f"broker fetch failed: {URL}"],
        }))

    try:
        with TestClient(main_bot.app) as client:
            main_bot._resolved_account_id_key = FAKE_KEY
            main_bot._account_binding = binding
            main_bot._crypto_account_binding = crypto_binding
            _run(seed_live())
            response = client.get("/status", headers={"X-Rork-Secret": "s3cret"})
        body = response.json()
        held["key"] = main_bot._resolved_account_id_key
        held["binding"] = main_bot._account_binding["resolved_account_id_key"]
        held["stored"] = _run(main_bot.state.get_position("ZZRED"))
        _run(main_bot.state.delete_position("ZZRED"))
    finally:
        main_bot.state = saved_state
        main_bot._resolved_account_id_key = saved_key
        main_bot._account_binding = saved_binding
        main_bot._crypto_account_binding = saved_crypto

    assert response.status_code == 200
    text = response.text
    assert FAKE_KEY not in text
    assert FAKE_ACCT not in text
    assert "sekret1234" not in text
    assert KEY_MASK in text
    assert body["account_binding"]["resolved_account_id_key"] == KEY_MASK
    assert body["account_binding"]["resolved_account_id"] == ACCT_MASK
    assert body["crypto_account_binding"]["resolved_account_id_key"] == KEY_MASK
    assert body["env_crypto_account_id"] == ACCT_MASK
    assert body["open_positions"]["ZZRED"]["accountIdKey"] == KEY_MASK
    assert body["open_positions"]["ZZRED"]["qty"] == 1
    assert "/v1/accounts/{acct}/" in text
    # The process still routes with the real key. The response is a copy.
    assert held["key"] == FAKE_KEY
    assert held["binding"] == FAKE_KEY
    assert held["stored"]["accountIdKey"] == FAKE_KEY
    assert held["stored"]["qty"] == 1


def test_pinned_account_log_uses_last4(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    saved_state = main_bot.state
    saved_pin = main_bot._pinned_account_id
    store = main_bot.StateStore(None)
    main_bot.state = store
    main_bot._pinned_account_id = None

    async def scenario():
        await store.set(main_bot.ACCOUNT_PIN_KEY, FAKE_ACCT)
        return await main_bot._load_pinned_account()

    try:
        loaded = _run(scenario())
    finally:
        main_bot.state = saved_state
        main_bot._pinned_account_id = saved_pin

    assert loaded == FAKE_ACCT
    assert FAKE_ACCT not in _blob(caplog.records)
    assert ACCT_MASK in _blob(caplog.records)


def test_account_mismatch_alert_masks_ids(monkeypatch):
    sent = []
    captured = {}

    async def capture(severity, event, message, **kwargs):
        sent.append({"event": event, "message": message, **kwargs})

    async def list_call(fn, *args, source="etrade", idempotent=True, **kwargs):
        return {"AccountListResponse": {"Accounts": {"Account": [{
            "accountId": "999000111",
            "accountIdKey": FAKE_KEY,
            "accountStatus": "ACTIVE",
            "accountDesc": "Individual Brokerage",
            "accountType": "INDIVIDUAL",
        }]}}}

    async def no_pin():
        return None

    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot.pyetrade, "ETradeAccounts", lambda *a, **k: _Broker())
    monkeypatch.setattr(main_bot, "_etrade_call", list_call)
    monkeypatch.setattr(main_bot, "_load_pinned_account", no_pin)
    monkeypatch.setattr(main_bot.alerts, "send", capture)
    monkeypatch.setattr(main_bot, "TARGET_ACCOUNT_ID", FAKE_ACCT)
    saved_key = main_bot._resolved_account_id_key
    saved_binding = main_bot._account_binding
    main_bot._resolved_account_id_key = None
    main_bot._account_binding = None

    async def scenario():
        resolved = await main_bot._resolve_account_id_key({
            "oauth_token": "tok", "oauth_token_secret": "sec",
        })
        captured["binding"] = dict(main_bot._account_binding or {})
        return resolved

    try:
        resolved = _run(scenario())
    finally:
        main_bot._resolved_account_id_key = saved_key
        main_bot._account_binding = saved_binding

    assert resolved == FAKE_KEY
    assert captured["binding"]["resolved_account_id_key"] == FAKE_KEY
    assert captured["binding"]["resolved_account_id"] == "999000111"
    blob = json.dumps(sent)
    assert FAKE_KEY not in blob
    assert FAKE_ACCT not in blob
    assert ACCT_MASK in blob
    assert any(row["event"] == "account_binding_mismatch" for row in sent)


def test_preflight_detail_masks_the_account_number():
    report = main_bot.build_preflight(
        live_trading=False, sandbox=True, linked=False,
        account_id_raw=FAKE_ACCT, account_id_repaired=True,
    )
    text = json.dumps(report)
    assert FAKE_ACCT not in text
    assert ACCT_MASK in text
    check = next(row for row in report["checks"] if row["key"] == "account_id_env")
    assert check["ok"] is False
    assert check["blocking"] is False


def test_alert_store_scrubs_broker_urls(monkeypatch):
    saved = main_bot.alerts._manager
    store = main_bot.StateStore(None)

    async def scenario():
        main_bot.alerts.init(store, None)
        await main_bot.alerts.send(
            "error", "reconcile_fetch_failed",
            f"Reconciliation could not read broker state: {URL}",
            dedupe_key="redact-test-url",
            data={"detail": URL},
        )
        return await main_bot.alerts.recent(5)

    try:
        recent = _run(scenario())
    finally:
        main_bot.alerts._manager = saved

    blob = json.dumps(recent)
    assert FAKE_KEY not in blob
    assert "sekret1234" not in blob
    assert "/v1/accounts/{acct}/" in blob


def test_etrade_account_response_masks_the_key_and_keeps_the_number(monkeypatch):
    saved_state = main_bot.state
    saved_key = main_bot._resolved_account_id_key
    saved_binding = main_bot._account_binding
    store = main_bot.StateStore(None)
    main_bot.state = store
    main_bot._resolved_account_id_key = None
    main_bot._account_binding = None

    async def list_call(fn, *args, source="etrade", idempotent=True, **kwargs):
        return {"AccountListResponse": {"Accounts": {"Account": [{
            "accountId": FAKE_ACCT,
            "accountIdKey": FAKE_KEY,
            "accountStatus": "ACTIVE",
            "accountDesc": "Individual Brokerage",
            "accountType": "INDIVIDUAL",
        }]}}}

    async def balance():
        return {}

    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot.pyetrade, "ETradeAccounts", lambda *a, **k: _Broker())
    monkeypatch.setattr(main_bot, "_etrade_call", list_call)
    monkeypatch.setattr(main_bot, "_fetch_broker_balance", balance)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "TARGET_ACCOUNT_ID", "")

    try:
        with TestClient(main_bot.app) as client:
            listed = client.get("/etrade/account", headers={"X-Rork-Secret": "s3cret"})
            selected = client.post(
                "/etrade/account/select",
                headers={"X-Rork-Secret": "s3cret"},
                json={"account_id": FAKE_ACCT},
            )
        internal_key = main_bot._resolved_account_id_key
        internal_binding = (main_bot._account_binding or {}).get("resolved_account_id_key")
    finally:
        main_bot.state = saved_state
        main_bot._resolved_account_id_key = saved_key
        main_bot._account_binding = saved_binding

    assert listed.status_code == 200, listed.text
    assert selected.status_code == 200, selected.text
    assert FAKE_KEY not in listed.text
    assert FAKE_KEY not in selected.text
    assert listed.json()["accounts"][0]["accountId"] == FAKE_ACCT
    assert listed.json()["accounts"][0]["accountIdKey"] == KEY_MASK
    assert listed.json()["account_id"] == FAKE_ACCT
    assert listed.json()["account_id_key"] == KEY_MASK
    assert selected.json()["account_id"] == FAKE_ACCT
    assert selected.json()["account_id_key"] == KEY_MASK
    assert selected.json()["resolved_account_id_key"] == KEY_MASK
    assert selected.json()["pinned_account_id"] == FAKE_ACCT
    assert internal_key == FAKE_KEY
    assert internal_binding == FAKE_KEY


def test_quote_error_response_scrubs_the_url(monkeypatch):
    async def boom(fn, *args, source="etrade", idempotent=True, **kwargs):
        raise RuntimeError(URL)

    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", lambda *a, **k: _Broker())
    monkeypatch.setattr(main_bot, "_etrade_call", boom)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    with TestClient(main_bot.app) as client:
        response = client.get("/etrade/quote?symbols=SPY", headers={"X-Rork-Secret": "s3cret"})
    assert response.status_code == 500
    assert FAKE_KEY not in response.text
    assert "sekret1234" not in response.text
    assert "/v1/accounts/{acct}/" in response.text


def test_other_report_payloads_scrub_account_urls():
    poisoned = {
        "version": main_bot.BOT_VERSION,
        "note": URL,
        "resolved_account_id_key": FAKE_KEY,
        "accountId": FAKE_ACCT,
    }
    for path in ("/ledger", "/core/report", "/edge/report", "/explain", "/alerts"):
        redacted = main_bot.redact_http_payload(path, poisoned)
        text = json.dumps(redacted)
        assert FAKE_KEY not in text, path
        assert FAKE_ACCT not in text, path
        assert "sekret1234" not in text, path
        assert "/v1/accounts/{acct}/" in text

    picker = main_bot.redact_http_payload("/etrade/account", {
        "account_id": FAKE_ACCT,
        "accountIdKey": FAKE_KEY,
        "note": URL,
    })
    assert picker["account_id"] == FAKE_ACCT
    assert picker["accountIdKey"] == KEY_MASK
    assert FAKE_KEY not in picker["note"]
