"""httpx must not write the E*TRADE accountIdKey into the INFO log.

logging.basicConfig(level=INFO) puts a handler on the root logger. httpx logs
each request URL at INFO, and those URLs contain the account key. The bot
raises the httpx and httpcore loggers to WARNING so the key never reaches
that handler.
"""
import asyncio
import logging

import httpx

import main_bot
import etrade_async


ACCOUNT_KEY = "TESTACCTKEY123"


def test_httpx_and_httpcore_loggers_are_warning_or_higher():
    """Importing main_bot runs the logging setup."""
    assert main_bot.logger.name == "etrade-bot"
    assert logging.getLogger("httpx").level >= logging.WARNING
    assert logging.getLogger("httpcore").level >= logging.WARNING


def test_portfolio_request_does_not_log_account_key_at_info(caplog):
    """A real httpx client must not emit the account key on an INFO record."""
    url = f"{etrade_async.PROD_BASE}/v1/accounts/{ACCOUNT_KEY}/portfolio.json"
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"AccountPortfolioResponse": {}})

    async def send():
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await client.get(url)

    # logger="" is the root logger: the stream basicConfig(level=INFO) opens.
    with caplog.at_level(logging.INFO, logger=""):
        assert logging.getLogger("httpx").level >= logging.WARNING
        assert logging.getLogger("httpcore").level >= logging.WARNING
        response = asyncio.run(send())

    assert response.status_code == 200
    assert seen == [url]
    parts = []
    for rec in caplog.records:
        if rec.levelno != logging.INFO:
            continue
        parts.append(rec.getMessage())
        if rec.args:
            parts.append(repr(rec.args))
    assert ACCOUNT_KEY not in "\n".join(parts)
