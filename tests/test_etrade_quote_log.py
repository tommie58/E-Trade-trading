"""E*TRADE quote ground truth. Logging only; the fake key is TESTACCTKEY123."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main_bot


FAKE_KEY = "TESTACCTKEY123"
FIELDS = (
    "source",
    "symbol",
    "option_symbol",
    "bid",
    "ask",
    "last",
    "open",
    "previousClose",
    "quoteStatus",
    "realtime",
    "dateTime",
    "fetched_at",
    "latency_ms",
)


@pytest.fixture(autouse=True)
def _isolate_quote_log():
    main_bot._QUOTE_LOG_MEMORY.clear()
    main_bot._QUOTE_LOG_SEEN.clear()
    main_bot._QUOTE_LOG_READS.clear()
    yield
    main_bot._QUOTE_LOG_MEMORY.clear()
    main_bot._QUOTE_LOG_SEEN.clear()
    main_bot._QUOTE_LOG_READS.clear()


def _equity_payload(symbol="SPY", **overrides):
    entry = {
        "dateTime": "15:30:00 EST 11-25-2026",
        "dateTimeUTC": 1764099000,
        "quoteStatus": "REALTIME",
        "Product": {"symbol": symbol, "securityType": "EQ"},
        "All": {
            "bid": 600.01,
            "ask": 600.05,
            "lastTrade": 600.03,
            "open": 599.10,
            "previousClose": 598.80,
            "companyName": "SPDR",
        },
        "accountIdKey": FAKE_KEY,
        "quoteDetail": (
            "https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders.json"
            "?accountIdKey=TESTACCTKEY123 "
            'Authorization: OAuth oauth_token="sekret1234"'
        ),
    }
    entry.update(overrides)
    return {"QuoteResponse": {"QuoteData": [entry]}}


def _lines():
    return list(main_bot._QUOTE_LOG_MEMORY)


def _records():
    out = []
    for line in _lines():
        assert line.startswith("etrade_quote ")
        out.append(json.loads(line[len("etrade_quote "):]))
    return out


def _blob():
    return "\n".join(_lines())


class _QuoteMarket:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def get_quote(self, symbols, resp_format="json"):
        self.calls += 1
        return self.payload


def _run(coro):
    return asyncio.run(coro)


async def _fetch(payload, symbols, quote_log_as):
    market = _QuoteMarket(payload)
    result = await main_bot._etrade_call(
        market.get_quote, symbols, resp_format="json",
        source="quote", quote_log_as=quote_log_as,
    )
    return result, market.calls


def test_quote_log_defaults_on_and_interval_is_five_seconds(monkeypatch):
    monkeypatch.delenv("ETRADE_QUOTE_LOG_ENABLED", raising=False)
    monkeypatch.delenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", raising=False)
    assert main_bot._etrade_quote_log_enabled() is True
    assert main_bot._etrade_quote_log_interval_s() == 5.0
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "false")
    assert main_bot._etrade_quote_log_enabled() is False
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "off")
    assert main_bot._etrade_quote_log_enabled() is False


def test_record_has_every_field(monkeypatch):
    monkeypatch.delenv("ETRADE_QUOTE_LOG_ENABLED", raising=False)
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "5")
    payload = _equity_payload()
    result, calls = _run(_fetch(payload, ["SPY"], "/etrade/quote"))
    assert calls == 1
    assert result == payload
    recs = _records()
    assert len(recs) == 1
    rec = recs[0]
    assert list(rec.keys()) == list(FIELDS)
    assert list(rec.keys()) == list(main_bot.ETRADE_QUOTE_LOG_FIELDS)
    assert rec["source"] == "/etrade/quote"
    assert rec["symbol"] == "SPY"
    assert rec["option_symbol"] is None
    assert rec["bid"] == 600.01
    assert rec["ask"] == 600.05
    assert rec["last"] == 600.03
    assert rec["open"] == 599.10
    assert rec["previousClose"] == 598.80
    assert rec["quoteStatus"] == "REALTIME"
    assert rec["realtime"] is True
    assert rec["dateTime"] == "15:30:00 EST 11-25-2026"
    assert rec["fetched_at"].endswith("+00:00")
    assert isinstance(rec["latency_ms"], int) and rec["latency_ms"] >= 0
    assert FAKE_KEY not in _blob()
    assert "sekret1234" not in _blob()
    assert "accountIdKey" not in _blob()


def test_sampling_interval_is_per_symbol(monkeypatch):
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "5")
    clock = {"t": 100.0}
    monkeypatch.setattr(main_bot.time, "monotonic", lambda: clock["t"])
    payload = _equity_payload()

    async def twice_then_later():
        await _fetch(payload, ["SPY"], "/etrade/quote")
        clock["t"] = 104.9
        await _fetch(payload, ["SPY"], "/etrade/quote")
        await _fetch(_equity_payload("QQQ"), ["QQQ"], "/etrade/quote")
        clock["t"] = 105.0
        await _fetch(payload, ["SPY"], "/etrade/quote")

    _run(twice_then_later())
    recs = _records()
    spy = [rec for rec in recs if rec["symbol"] == "SPY"]
    qqq = [rec for rec in recs if rec["symbol"] == "QQQ"]
    assert len(spy) == 2
    assert len(qqq) == 1


def test_flag_off_produces_no_records(monkeypatch, caplog):
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "false")
    caplog.set_level(logging.INFO, logger="etrade-bot")
    result, calls = _run(_fetch(_equity_payload(), ["SPY"], "/etrade/quote"))
    assert calls == 1
    assert result["QuoteResponse"]["QuoteData"][0]["All"]["bid"] == 600.01
    assert _lines() == []
    assert not [rec for rec in caplog.records if rec.getMessage().startswith("etrade_quote ")]


def test_option_chain_record_names_the_contract_and_keeps_the_quote(monkeypatch):
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "0")
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)

    class _Chain:
        def get_option_expire_date(self, symbol, resp_format="json"):
            return {"OptionExpireDateResponse": {"ExpirationDate": [
                {"year": 2099, "month": 1, "day": 15},
            ]}}

        def get_option_chains(self, symbol, **kwargs):
            return {"OptionChainResponse": {
                "quoteType": "REALTIME",
                "timeStamp": 1764099000,
                "OptionPair": [{
                    "Call": {
                        "symbol": "SPY",
                        "strikePrice": 600,
                        "bid": 1.2,
                        "ask": 1.35,
                        "lastPrice": 1.28,
                        "open": 1.1,
                        "previousClose": 1.05,
                        "osiKey": "SPY---261128C00600000",
                        "quoteDetail": (
                            "https://api.etrade.com/v1/accounts/TESTACCTKEY123/x"
                            "?accountIdKey=TESTACCTKEY123 "
                            'Authorization: OAuth oauth_token="sekret1234"'
                        ),
                    },
                }],
            }}

    expiry, strike, bid, ask = main_bot._snap_option_contract(
        _Chain(), "SPY", "2099-01-15", 600, "CALL",
    )
    assert (expiry, strike, bid, ask) == ("2099-01-15", 600.0, 1.2, 1.35)
    recs = _records()
    assert len(recs) == 1
    rec = recs[0]
    assert list(rec.keys()) == list(FIELDS)
    assert rec["source"] == "_snap_option_contract"
    assert rec["symbol"] == "SPY"
    assert rec["option_symbol"] == "SPY:2099-01-15:CALL:600"
    assert rec["bid"] == 1.2
    assert rec["ask"] == 1.35
    assert rec["last"] == 1.28
    assert rec["open"] == 1.1
    assert rec["previousClose"] == 1.05
    assert rec["quoteStatus"] == "REALTIME"
    assert rec["realtime"] is True
    assert isinstance(rec["dateTime"], str) and "T" in rec["dateTime"]
    assert FAKE_KEY not in _blob()
    assert "sekret1234" not in _blob()


def test_decision_readers_pass_their_function_name(monkeypatch):
    seen = []

    async def fake_async(fn, *args, source="etrade", idempotent=True, quote_log_as=None, **kwargs):
        seen.append(("async", source, quote_log_as))
        return {"QuoteResponse": {"QuoteData": [{
            "quoteStatus": "REALTIME",
            "Product": {"symbol": "SPY"},
            "All": {"bid": 1.0, "ask": 1.1, "lastTrade": 1.05},
        }]}}

    def fake_sync(fn, *args, source="etrade", quote_log_as=None, **kwargs):
        seen.append(("sync", source, quote_log_as))
        if getattr(fn, "__name__", "") == "get_option_expire_date":
            return {"OptionExpireDateResponse": {"ExpirationDate": [
                {"year": 2099, "month": 1, "day": 15},
            ]}}
        return {"OptionChainResponse": {"quoteType": "DELAYED", "OptionPair": []}}

    monkeypatch.setattr(main_bot, "_etrade_call", fake_async)
    monkeypatch.setattr(main_bot, "_sync_etrade_call", fake_sync)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot.core_book, "weekly_candidates", lambda *a, **k: [date(2099, 1, 15)])
    monkeypatch.setattr(main_bot.core_book, "strike_for_delta", lambda *a, **k: 600)

    class _ChainMarket:
        def get_option_expire_date(self, *args, **kwargs):
            return None

        def get_option_chains(self, *args, **kwargs):
            return None

    async def scenario():
        bid, ask = await main_bot._etrade_bid_ask("SPY")
        assert (bid, ask) == (1.0, 1.1)
        raw = await main_bot._scanner_fetch_etrade_quotes(["SPY"])
        assert raw["QuoteResponse"]["QuoteData"][0]["All"]["bid"] == 1.0

    _run(scenario())
    main_bot._best_expression_contract(_ChainMarket(), "SPY", 600, 0.2, "CALL", date(2026, 11, 25))
    labels = {(kind, source, log_as) for kind, source, log_as in seen}
    assert ("async", "stop_reprice_quote", "_etrade_bid_ask") in labels
    assert ("async", "scanner_quote", "_scanner_fetch_etrade_quotes") in labels
    assert ("sync", "option_chain", "_best_expression_contract") in labels


def test_logging_exception_does_not_break_the_quote_route(monkeypatch, caplog):
    def explode(*args, **kwargs):
        raise RuntimeError("logger failed TESTACCTKEY123 oauth_token=sekret1234")

    monkeypatch.setattr(main_bot, "_observe_etrade_quote_fetch", explode)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", lambda *a, **k: _QuoteMarket(_equity_payload()))
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "DEBUG_QUOTES", False)
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "false")
    caplog.set_level(logging.DEBUG)
    with TestClient(main_bot.app) as client:
        monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "true")
        response = client.get("/etrade/quote?symbols=SPY", headers={"X-Rork-Secret": "s3cret"})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["quotes"][0]["symbol"] == "SPY"
    assert body["quotes"][0]["bid"] == 600.01
    assert body["quotes"][0]["ask"] == 600.05
    assert body["quotes"][0]["price"] == 600.03
    assert "option_symbol" not in body["quotes"][0]
    assert FAKE_KEY not in response.text
    assert "sekret1234" not in response.text
    assert FAKE_KEY not in caplog.text
    assert _lines() == []


def test_get_log_requires_auth_rate_limit_and_limit_cap(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_READ_PER_MIN", "2")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "false")
    poisoned = "etrade_quote " + json.dumps({
        "source": "/etrade/quote",
        "symbol": "SPY",
        "option_symbol": None,
        "bid": 1,
        "note": FAKE_KEY,
    })
    main_bot._QUOTE_LOG_MEMORY.appendleft(poisoned)
    headers = {"X-Rork-Secret": "s3cret"}
    with TestClient(main_bot.app) as client:
        missing = client.get("/etrade/quotes/log")
        wrong = client.get("/etrade/quotes/log", headers={"X-Rork-Secret": "nope"})
        monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", None)
        unset = client.get("/etrade/quotes/log", headers=headers)
        monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
        over = client.get("/etrade/quotes/log?limit=1001", headers=headers)
        first = client.get("/etrade/quotes/log?limit=1000&symbol=SPY", headers=headers)
        second = client.get("/etrade/quotes/log?symbol=QQQ", headers=headers)
        third = client.get("/etrade/quotes/log", headers=headers)
    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert unset.status_code == 401
    for denied in (missing, wrong, unset):
        assert FAKE_KEY not in denied.text
        assert "quotes" not in denied.json()
    assert over.status_code == 422
    assert first.status_code == 200, first.text
    assert first.json()["limit"] == 1000
    assert FAKE_KEY not in first.text
    assert second.status_code == 200, second.text
    assert third.status_code == 429
    assert FAKE_KEY not in third.text


def test_get_log_filters_symbol_and_since(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "true")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "0")

    async def load():
        await _fetch(_equity_payload("SPY"), ["SPY"], "/etrade/quote")
        await _fetch(_equity_payload("QQQ"), ["QQQ"], "_scanner_fetch_etrade_quotes")

    _run(load())
    spy = next(rec for rec in _records() if rec["symbol"] == "SPY")
    with TestClient(main_bot.app) as client:
        headers = {"X-Rork-Secret": "s3cret"}
        only = client.get("/etrade/quotes/log?symbol=QQQ&limit=10", headers=headers)
        after = client.get(
            "/etrade/quotes/log?since=2999-01-01T00:00:00Z&limit=10",
            headers=headers,
        )
        bad = client.get("/etrade/quotes/log?since=not-a-time", headers=headers)
    assert only.status_code == 200, only.text
    assert [row["symbol"] for row in only.json()["quotes"]] == ["QQQ"]
    assert after.status_code == 200
    assert after.json()["quotes"] == []
    assert bad.status_code == 400
    assert spy["bid"] == 600.01


def test_redis_list_is_capped_and_scrubbed(monkeypatch):
    fakeredis = pytest.importorskip("fakeredis.aioredis")
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(main_bot, "state", main_bot.StateStore(client))
    monkeypatch.setattr(main_bot, "_etrade_quote_log_cap", lambda: 3)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "0")
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)

    async def scenario():
        for i in range(5):
            payload = _equity_payload(f"S{i}")
            payload["QuoteResponse"]["QuoteData"][0]["dateTime"] = (
                "https://api.etrade.com/v1/accounts/TESTACCTKEY123/q"
            )
            await _fetch(payload, [f"S{i}"], "/etrade/quote")
        pending = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}
        if pending:
            await asyncio.gather(*pending)
        stored = await client.lrange(main_bot.ETRADE_QUOTE_LOG_KEY, 0, -1)
        assert len(stored) == 3
        assert FAKE_KEY not in "\n".join(stored)
        assert "sekret1234" not in "\n".join(stored)
        main_bot._QUOTE_LOG_MEMORY.clear()
        body = await main_bot.etrade_quotes_log(
            symbol="S4", since=None, limit=10, x_rork_secret="s3cret",
        )
        assert body["count"] == 1
        assert body["quotes"][0]["symbol"] == "S4"
        assert list(body["quotes"][0].keys()) == list(FIELDS)
        assert FAKE_KEY not in json.dumps(body)

    _run(scenario())


OCC = "INTC  261127P00056000"
ORDER_ID = "9988776655443"
SIGNAL_KEY = "sig:12345678abcdef"


def test_occ_symbol_and_order_id_survive_byte_identical(tmp_path, monkeypatch, caplog):
    """Structured rows keep OCC symbols and order ids. The plain log line still uses the full scrub."""
    monkeypatch.setattr(main_bot, "_resolved_account_id_key", FAKE_KEY)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "tok", "oauth_token_secret": "sec",
    })
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "DEBUG_QUOTES", False)
    monkeypatch.setenv("ETRADE_QUOTE_LOG_ENABLED", "true")
    monkeypatch.setenv("ETRADE_QUOTE_LOG_MIN_INTERVAL_S", "0")
    main_bot._OPTION_SNAPSHOT_PENDING.clear()
    main_bot._OPTION_SNAPSHOT_SEEN.clear()
    main_bot._OPTION_SNAPSHOT_STOP_TOUCHED.clear()
    caplog.set_level(logging.INFO, logger="etrade-bot")

    payload = _equity_payload("INTC")
    payload["QuoteResponse"]["QuoteData"][0]["dateTime"] = (
        "15:30:00 EST 11-25-2026 https://api.etrade.com/v1/accounts/TESTACCTKEY123/orders.json"
        "?accountIdKey=TESTACCTKEY123"
    )

    class _Market:
        def get_quote(self, symbols, resp_format="json"):
            assert symbols == [OCC]
            return payload

    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", lambda *a, **k: _Market())
    saved_state = main_bot.state
    saved_ledger = main_bot.trade_ledger._ledger
    saved_mirror = main_bot.trade_ledger._mirror
    saved_snap = main_bot._snap_log
    main_bot.state = main_bot.StateStore(None)
    path = Path(tmp_path) / "ledger.jsonl"
    try:
        with TestClient(main_bot.app) as client:
            main_bot._QUOTE_LOG_MEMORY.clear()
            main_bot._QUOTE_LOG_SEEN.clear()
            quoted = client.get(
                "/etrade/quote",
                params={"symbols": OCC},
                headers={"X-Rork-Secret": "s3cret"},
            )
            logged = client.get(
                "/etrade/quotes/log",
                params={"symbol": OCC, "limit": 10},
                headers={"X-Rork-Secret": "s3cret"},
            )
        assert quoted.status_code == 200, quoted.text
        assert logged.status_code == 200, logged.text
        rows = logged.json()["quotes"]
        assert len(rows) == 1
        assert rows[0]["option_symbol"] == OCC
        assert rows[0]["symbol"] == "INTC"
        assert OCC in logged.text
        assert FAKE_KEY not in logged.text
        assert "/v1/accounts/{acct}/" in logged.text

        log_lines = [rec.getMessage() for rec in caplog.records if rec.getMessage().startswith("etrade_quote ")]
        assert log_lines
        assert OCC not in log_lines[-1]
        assert "{redacted}" in log_lines[-1]

        main_bot._OPTION_SNAPSHOT_PENDING.clear()
        main_bot._OPTION_SNAPSHOT_SEEN.clear()
        main_bot.init_quote_snapshot_log(path)
        preset = {
            "status": "ok",
            "bid": 1.25,
            "ask": 1.35,
            "last": 1.28,
            "dateTime": "15:30:00 EST 11-25-2026",
            "option_symbol": OCC,
            "_quote_time_source": "broker_quote",
            "_fetched_at": "2026-11-25T20:30:00+00:00",
        }
        main_bot._enqueue_option_snapshot(
            SIGNAL_KEY, None, datetime(2026, 11, 25, 20, 30, tzinfo=timezone.utc),
            {"ticker": "INTC", "option_symbol": OCC},
            offset_kind="exit_fill", order_id=ORDER_ID, preset_quote=preset,
        )

        async def write():
            return await main_bot._option_snapshot_tick(datetime(2099, 1, 1, tzinfo=timezone.utc))

        assert _run(write()) == 1
        raw = path.read_text(encoding="utf-8")
        ledger = json.loads(raw)
    finally:
        main_bot.state = saved_state
        main_bot.trade_ledger._ledger = saved_ledger
        main_bot.trade_ledger._mirror = saved_mirror
        main_bot._snap_log = saved_snap

    data = ledger["data"]
    assert data["quote"]["option_symbol"] == OCC
    assert data["order_id"] == ORDER_ID
    assert data["signal_key"] == SIGNAL_KEY
    assert OCC.encode() in raw.encode()
    assert ORDER_ID.encode() in raw.encode()
    assert SIGNAL_KEY.encode() in raw.encode()
    assert FAKE_KEY not in raw
