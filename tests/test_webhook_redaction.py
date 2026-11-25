"""Webhook URL redaction. Logging, alert delivery, and HTTP error bodies.

The Discord URL, Slack path, query values, and userinfo below are fixtures.
Nothing here is a live credential.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging

import httpx
import requests
from fastapi.testclient import TestClient
from starlette.responses import Response

import main_bot
import etrade_async


FAKE_WEBHOOK = "https://discord.com/api/webhooks/123456789012345678/FAKE-TOKEN_abc.def"
SNOWFLAKE = "123456789012345678"
TOKEN = "FAKE-TOKEN"
SLACK_WEBHOOK = "https://hooks.slack.com/services/T000/B000/FAKE-TOKEN_abc.def"
TOKEN_QUERY = "https://api.example.com/v1/orders?token=FAKE-TOKEN_abc.def&ok=1"
APIKEY_QUERY = "https://api.example.com/v1/orders?apikey=FAKE-TOKEN_abc.def"
USERINFO_URL = "https://FAKE-TOKEN_abc.def:123456789012345678@alerts.example.com/notify"
AUTH_URL = "https://us.etrade.com/e/t/etws/authorize?key=CONSUMERKEY&token=REQUESTTOKEN"


def _run(coro):
    return asyncio.run(coro)


def _formatted_handler_output(logger_name: str, emit) -> str:
    """What a handler actually writes, including traceback text."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log = logging.getLogger(logger_name)
    previous = log.level
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)
    try:
        emit(log)
    finally:
        log.removeHandler(handler)
        log.setLevel(previous)
    return stream.getvalue()


def _assert_no_webhook_secret(text: str) -> None:
    assert TOKEN not in text
    assert SNOWFLAKE not in text


def test_scrub_etrade_text_redacts_webhook_and_credential_urls():
    discord = main_bot.scrub_etrade_text(FAKE_WEBHOOK)
    _assert_no_webhook_secret(discord)
    assert discord == "https://discord.com/api/webhooks/[redacted]"
    assert main_bot.scrub_etrade_text(discord) == discord

    canary = main_bot.scrub_etrade_text(
        "https://canary.discord.com/api/webhooks/123456789012345678/FAKE-TOKEN_abc.def"
    )
    app_host = main_bot.scrub_etrade_text(
        "https://discordapp.com/api/webhooks/123456789012345678/FAKE-TOKEN_abc.def?wait=true"
    )
    ptb = main_bot.scrub_etrade_text(
        "http://ptb.discordapp.com/api/webhooks/123456789012345678/FAKE-TOKEN_abc.def"
    )
    _assert_no_webhook_secret(canary + app_host + ptb)
    assert canary == "https://canary.discord.com/api/webhooks/[redacted]"
    assert app_host == "https://discordapp.com/api/webhooks/[redacted]"
    assert ptb == "http://ptb.discordapp.com/api/webhooks/[redacted]"

    slack = main_bot.scrub_etrade_text(SLACK_WEBHOOK)
    assert slack == "https://hooks.slack.com/services/[redacted]"
    assert TOKEN not in slack

    token_q = main_bot.scrub_etrade_text(TOKEN_QUERY)
    apikey_q = main_bot.scrub_etrade_text(APIKEY_QUERY)
    assert token_q == "https://api.example.com/v1/orders?token=[redacted]&ok=1"
    assert apikey_q == "https://api.example.com/v1/orders?apikey=[redacted]"
    assert TOKEN not in token_q + apikey_q

    userinfo = main_bot.scrub_etrade_text(USERINFO_URL)
    _assert_no_webhook_secret(userinfo)
    assert userinfo == "https://[redacted]@alerts.example.com/notify"

    both = main_bot.scrub_etrade_text(
        "https://user:pass@discord.com/api/webhooks/123456789012345678/FAKE-TOKEN_abc.def"
    )
    assert both == "https://[redacted]@discord.com/api/webhooks/[redacted]"
    assert "user:pass" not in both

    # A lookalike host is not a Discord webhook. No secret is embedded here.
    untouched = main_bot.scrub_etrade_text("https://notdiscord.com/status")
    assert untouched == "https://notdiscord.com/status"


def test_success_response_keeps_the_authorize_query():
    """Only the auth-start and /link success bodies keep ?key= and ?token=."""
    assert main_bot._preserve_authorize_query("/etrade/auth/start", 200) is True
    assert main_bot._preserve_authorize_query("/link", 200) is True
    assert main_bot._preserve_authorize_query("/etrade/auth/start", 500) is False
    assert main_bot._preserve_authorize_query("/status", 200) is False
    kept = main_bot.redact_http_payload(
        "/etrade/auth/start", {"auth_url": AUTH_URL}, scrub_query_secrets=False,
    )
    assert kept["auth_url"] == AUTH_URL
    hidden = main_bot.redact_http_payload(
        "/status", {"detail": AUTH_URL}, scrub_query_secrets=True,
    )
    assert "CONSUMERKEY" not in hidden["detail"]
    assert "REQUESTTOKEN" not in hidden["detail"]
    assert hidden["detail"] == (
        "https://us.etrade.com/e/t/etws/authorize?key=[redacted]&token=[redacted]"
    )


def test_log_messages_keep_order_ids():
    line = (
        "placed order 123456789 OCC O:SPY261120C00500000 "
        "sig_key=abc123def key_levels=510.25/512.00"
    )

    def emit(log):
        log.info("%s", line)

    text = _formatted_handler_output("etrade-bot.order-id-kept", emit)
    assert text.strip() == line


def test_httpx_info_request_log_redacts_the_discord_webhook():
    httpx_log = logging.getLogger("httpx")
    assert any(isinstance(existing, main_bot._EtradeLogRedactor) for existing in httpx_log.filters)
    httpx_log.setLevel(logging.INFO)

    def emit(log):
        assert any(isinstance(existing, main_bot._EtradeLogRedactor) for existing in log.filters)
        log.info(
            'HTTP Request: %s %s "%s %d %s"',
            "POST",
            FAKE_WEBHOOK,
            "HTTP/1.1",
            204,
            "No Content",
        )

    text = _formatted_handler_output("httpx", emit)
    _assert_no_webhook_secret(text)
    assert 'HTTP Request: POST https://discord.com/api/webhooks/[redacted] "HTTP/1.1 204 No Content"' in text


def _http_status_error(url: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    response = httpx.Response(400, request=request)
    return httpx.HTTPStatusError(
        f"Client error '400 Bad Request' for url '{url}'\n"
        "For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400",
        request=request,
        response=response,
    )


def _requests_http_error(url: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = 500
    response.reason = "Server Error"
    response.url = url
    return requests.HTTPError(
        f"500 Server Error: Server Error for url: {url}",
        response=response,
    )


def test_httpx_status_error_traceback_redacts_the_discord_webhook():
    def emit(log):
        try:
            raise _http_status_error(FAKE_WEBHOOK)
        except httpx.HTTPStatusError:
            log.exception("discord post failed")

    text = _formatted_handler_output("etrade-bot.webhook-httpx", emit)
    assert "Traceback (most recent call last):" in text
    assert "discord post failed" in text
    _assert_no_webhook_secret(text)
    assert "https://discord.com/api/webhooks/[redacted]" in text
    assert "developer.mozilla.org" in text


def test_requests_httperror_traceback_redacts_the_discord_webhook():
    def emit(log):
        try:
            raise _requests_http_error(FAKE_WEBHOOK)
        except requests.HTTPError:
            log.exception("discord post failed")

    text = _formatted_handler_output("etrade-bot.webhook-requests", emit)
    assert "Traceback (most recent call last):" in text
    _assert_no_webhook_secret(text)
    assert "https://discord.com/api/webhooks/[redacted]" in text


def test_slack_query_and_userinfo_logs_are_redacted():
    def emit(log):
        log.info("slack %s", SLACK_WEBHOOK)
        log.info("query %s", TOKEN_QUERY)
        log.info("apikey %s", APIKEY_QUERY)
        log.info("userinfo %s", USERINFO_URL)

    text = _formatted_handler_output("etrade-bot.webhook-shapes", emit)
    _assert_no_webhook_secret(text)
    assert "https://hooks.slack.com/services/[redacted]" in text
    assert "https://api.example.com/v1/orders?token=[redacted]&ok=1" in text
    assert "https://api.example.com/v1/orders?apikey=[redacted]" in text
    assert "https://[redacted]@alerts.example.com/notify" in text


def test_alert_sender_failure_does_not_log_the_webhook(monkeypatch):
    saved = main_bot.alerts._manager
    store = main_bot.StateStore(None)
    seen = {}

    class _Boom:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json=None):
            seen["body"] = json
            raise _http_status_error(url)

    monkeypatch.setattr(httpx, "AsyncClient", _Boom)
    held = {}

    def emit(log):
        async def scenario():
            main_bot.alerts.init(store, FAKE_WEBHOOK)
            await main_bot.alerts.send(
                "error",
                "discord_delivery",
                f"notify failed {FAKE_WEBHOOK}",
                data={"callback": FAKE_WEBHOOK},
                webhook_text=f"body {FAKE_WEBHOOK}",
            )
            held["recent"] = await main_bot.alerts.recent(5)
            held["delivery"] = await store.get(main_bot.alerts.ALERTS_LAST_DELIVERY_KEY)

        _run(scenario())

    try:
        text = _formatted_handler_output("etrade-bot.alerts", emit)
    finally:
        main_bot.alerts._manager = saved

    _assert_no_webhook_secret(text)
    assert "alert webhook delivery failed:" in text
    assert "https://discord.com/api/webhooks/[redacted]" in text
    blob = json.dumps({"recent": held["recent"], "delivery": held["delivery"], "body": seen["body"]})
    _assert_no_webhook_secret(blob)
    assert seen["body"]["content"].endswith("/api/webhooks/[redacted]")
    delivery = json.loads(held["delivery"])
    assert delivery["ok"] is False
    assert TOKEN not in delivery["detail"]
    assert SNOWFLAKE not in delivery["detail"]


def test_http_exception_body_redacts_the_webhook(monkeypatch):
    class _Req:
        class url:
            path = "/alerts/config"

    response = _run(main_bot._redact_http_exception(
        _Req(), main_bot.HTTPException(500, detail=f"upstream {FAKE_WEBHOOK}"),
    ))
    body = response.body.decode()
    _assert_no_webhook_secret(body)
    assert "/api/webhooks/[redacted]" in body

    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    async def boom(url, sev):
        raise ValueError(f"refused {FAKE_WEBHOOK} and {TOKEN_QUERY}")

    monkeypatch.setattr(main_bot.alerts, "set_config", boom)
    with TestClient(main_bot.app) as client:
        error = client.post(
            "/alerts/config",
            headers={"X-Rork-Secret": "s3cret"},
            json={"webhook_url": "https://example.com/hook", "min_severity": "warning"},
        )
        monkeypatch.setattr(main_bot, "load_tokens", lambda: None)
        plain = client.get("/etrade/quote?symbols=SPY", headers={"X-Rork-Secret": "s3cret"})
    _assert_no_webhook_secret(error.text)
    assert error.status_code == 422
    assert "/api/webhooks/[redacted]" in error.text
    assert "token=[redacted]" in error.text
    assert plain.status_code == 401
    assert plain.json()["detail"] == "E*TRADE account not linked"


def test_sync_and_async_scrubbers_are_one_implementation():
    sample = (
        f"{FAKE_WEBHOOK} ?accountIdKey=TESTACCTKEY123"
        "&token=FAKE-TOKEN_abc.def&apikey=FAKE-TOKEN_abc.def"
    )
    assert etrade_async.scrub_etrade_text is main_bot.scrub_etrade_text
    sync = main_bot.scrub_etrade_text(sample)
    async_out = etrade_async.scrub_etrade_text(sample)
    assert sync == async_out
    _assert_no_webhook_secret(sync)
    assert "accountIdKey={acct}" in sync
    assert "token=[redacted]" in sync
    assert "apikey=[redacted]" in sync
    assert "/api/webhooks/[redacted]" in sync


def test_mask_webhook_url_strips_userinfo():
    masked = main_bot.alerts.mask_webhook_url(
        "https://FAKE-TOKEN_abc.def:s3cret@discord.com/api/webhooks/"
        "123456789012345678/FAKE-TOKEN_abc.def"
    )
    assert masked is not None
    _assert_no_webhook_secret(masked)
    assert "s3cret" not in masked
    assert "@" not in masked
    assert masked.startswith("https://discord.com/")


def test_long_webhook_url_is_dropped_before_truncation(monkeypatch):
    marker = "LONGHOOKTOKEN"
    long_url = "https://alerts.example.com/hooks/" + (marker * 40)
    assert len(long_url) > 200
    saved = main_bot.alerts._manager
    store = main_bot.StateStore(None)

    class _Boom:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json=None):
            raise _http_status_error(url)

    monkeypatch.setattr(httpx, "AsyncClient", _Boom)
    held = {}

    def emit(log):
        async def scenario():
            main_bot.alerts.init(store, long_url)
            await main_bot.alerts.send("error", "generic_hook", "delivery failed")
            held["delivery"] = await store.get(main_bot.alerts.ALERTS_LAST_DELIVERY_KEY)

        _run(scenario())

    try:
        text = _formatted_handler_output("etrade-bot.alerts", emit)
    finally:
        main_bot.alerts._manager = saved

    assert marker not in text
    assert long_url not in text
    assert "alert webhook delivery failed: webhook delivery failed" in text
    delivery = json.loads(held["delivery"])
    assert delivery["detail"] == "webhook delivery failed"
    assert marker not in delivery["detail"]


class _Path:
    def __init__(self, path):
        self.path = path


class _Request:
    def __init__(self, path):
        self.url = _Path(path)


def _json_response(body: bytes, status_code: int = 200):
    return Response(content=body, media_type="application/json", status_code=status_code)


def test_authorize_route_keeps_query_and_other_routes_do_not():
    body = json.dumps({"auth_url": AUTH_URL, "authorize_url": AUTH_URL}).encode()

    async def call_next(_request):
        return _json_response(body)

    kept = _run(main_bot._redact_account_response(_Request("/etrade/auth/start"), call_next))
    assert AUTH_URL.encode() in kept.body
    link = _run(main_bot._redact_account_response(_Request("/link"), call_next))
    assert AUTH_URL.encode() in link.body
    other = _run(main_bot._redact_account_response(_Request("/status"), call_next))
    text = other.body.decode()
    assert "CONSUMERKEY" not in text
    assert "REQUESTTOKEN" not in text
    assert "key=[redacted]" in text
    assert "token=[redacted]" in text


def _probe(monkeypatch, path: str, raw: bytes):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    async def endpoint():
        return Response(content=raw, media_type="application/json")

    main_bot.app.add_api_route(path, endpoint, methods=["GET"])
    try:
        with TestClient(main_bot.app) as client:
            return client.get(path, headers={"X-Rork-Secret": "s3cret"})
    finally:
        main_bot.app.router.routes[:] = [
            route for route in main_bot.app.router.routes
            if getattr(route, "path", None) != path
        ]


def test_nan_beside_a_masked_field_returns_the_original_body(monkeypatch, caplog):
    raw = b'{"accountIdKey":"TESTACCTKEY123","mark":NaN}'
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    response = _probe(monkeypatch, "/__redact_probe_nan", raw)
    assert response.status_code == 200
    assert response.content == raw
    warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert any("ValueError" in message for message in warnings)
    assert all("TESTACCTKEY123" not in message for message in warnings)


def test_deeply_nested_body_returns_the_original_body(monkeypatch, caplog):
    # redact walks one Python frame per object. Past the recursion limit
    # (about 900 once the request stack is counted; 1000 from a shallow call)
    # that used to 500 after the endpoint had already produced a body.
    node = {"accountIdKey": "TESTACCTKEY123"}
    for _ in range(1000):
        node = {"n": node}
    raw = json.dumps(node).encode()
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    response = _probe(monkeypatch, "/__redact_probe_deep", raw)
    assert response.status_code == 200
    assert response.content == raw
    warnings = [rec.getMessage() for rec in caplog.records if rec.levelno >= logging.WARNING]
    assert any("RecursionError" in message for message in warnings)
    assert all("TESTACCTKEY123" not in message for message in warnings)
