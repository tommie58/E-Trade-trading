"""POST /explain must be read-only. The live filter path still persists."""
from __future__ import annotations

import asyncio
import copy
import inspect
import json
import tempfile
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

import main_bot
import pro_trader


def _run(coro):
    return asyncio.run(coro)


def _save():
    return {
        "state": main_bot.state,
        "client": main_bot.massive_client,
        "source": dict(main_bot._scanner_quote_source),
        "desk": main_bot._PRO_TRADER,
        "ledger": main_bot.trade_ledger._ledger,
        "policy": dict(main_bot._LIVE_POLICY),
    }


def _restore(saved):
    main_bot.state = saved["state"]
    main_bot.massive_client = saved["client"]
    main_bot._scanner_quote_source.clear()
    main_bot._scanner_quote_source.update(saved["source"])
    main_bot._PRO_TRADER = saved["desk"]
    main_bot.trade_ledger._ledger = saved["ledger"]
    main_bot._LIVE_POLICY.clear()
    main_bot._LIVE_POLICY.update(saved["policy"])


def _payload():
    return {
        "ticker": "NVDA",
        "action": "BUY",
        "instrument": "option",
        "option_right": "CALL",
        "setup": "breakout",
        "score": 90,
        "rvol": 2.0,
        "mode": "paper",
        "option_bid": 1.20,
        "option_ask": 1.30,
        "option_limit_price": 1.25,
        "strike_hint": 100,
        "option_expiration": "2026-12-18",
    }


class _Resp:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


class _QuoteClient:
    """Stand-in for httpx.AsyncClient. Never reads request headers."""

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None, headers=None):
        if "/v2/snapshot/" in str(url):
            now_ns = int(time.time() * 1_000_000_000)
            tickers = []
            for sym, last in (("SPY", 510.0), ("QQQ", 440.0)):
                tickers.append({
                    "ticker": sym,
                    "lastTrade": {"p": last, "t": now_ns},
                    "lastQuote": {"p": last - 0.01, "P": last + 0.01, "s": 10, "S": 10},
                    "day": {"o": last - 1, "h": last + 1, "l": last - 2, "c": last, "v": 1_000_000},
                    "prevDay": {"c": last - 2},
                    "todaysChange": 1.5,
                    "todaysChangePerc": 0.3,
                })
            return _Resp({"tickers": tickers})
        return _Resp({"results": []})


def _arm_feed(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(httpx, "AsyncClient", _QuoteClient)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: None)


def _memory_snapshot(store):
    """Every memory-backend kv, hash, set, and list key.

    Lists live in the kv map as JSON arrays. They are also returned on their
    own so a list write is visible even if a caller only diffs `lists`.
    """
    mem = store._memory
    kv = {}
    lists = {}
    for key, item in mem._kv.items():
        value, exp = item
        kv[key] = (value, exp)
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, list):
            lists[key] = parsed
    return {
        "kv": copy.deepcopy(kv),
        "hashes": copy.deepcopy(mem._hashes),
        "sets": copy.deepcopy(mem._sets),
        "lists": copy.deepcopy(lists),
    }


def _fresh_client():
    client = main_bot.massive.MassiveClient()
    client.calls = 4
    client.failures = 2
    client.last_ok_at = 111.0
    client.last_error = "seed"
    client.auth_status = None
    client._adv_day = "seed-day"
    client._adv = {"NVDA": 10.0}
    client.on_health = main_bot._persist_feed_health
    return client


def _fresh_desk():
    desk = pro_trader.Desk()
    desk.memory = {"regime": "SEEDED", "pending": "CHOP_RANGE", "pending_count": 2}
    desk.decisions = [{"verdict": "ALLOW", "playbook": "trend_continuation"}]
    desk.last = {"verdict": "ALLOW", "summary": "seed"}
    desk.trades_completed = 7
    desk.min_trades = 9
    return desk


async def _seed_store(store):
    await store.set(main_bot.DESK_STATE_KEY, "sentinel-desk")
    await store.set(main_bot.FEED_HEALTH_KEY, "sentinel-feed")
    await store.set("seed:kv", "alpha")
    await store._memory.sadd("seed:set", "beta")
    await store._memory.hincrbyfloat("seed:hash", "gamma", 1.25, 86_400)
    await store.queue_push("seed:list", "delta")


def test_bot_version_is_pro_day_trader_59913():
    assert main_bot.BOT_VERSION == "5.99.13-pro-day-trader"


def test_dry_run_is_keyword_only_and_defaults_false():
    for fn in (
        main_bot._passes_entry_filters,
        main_bot._fold_pro_trader,
        main_bot._scanner_fetch_quotes,
        main_bot.massive.MassiveClient.fetch_quote_response,
    ):
        param = inspect.signature(fn).parameters["dry_run"]
        assert param.kind is inspect.Parameter.KEYWORD_ONLY
        assert param.default is False
    with pytest.raises(TypeError):
        main_bot._passes_entry_filters({}, True)
    with pytest.raises(TypeError):
        main_bot._fold_pro_trader({}, [], True)
    with pytest.raises(TypeError):
        main_bot._scanner_fetch_quotes([], True)
    with pytest.raises(TypeError):
        main_bot.massive.MassiveClient().fetch_quote_response([], True)


def test_explain_leaves_memory_desk_and_ledger_unchanged(monkeypatch):
    saved = _save()
    _arm_feed(monkeypatch)
    seen = []
    orig_fetch = main_bot._scanner_fetch_quotes

    async def counting_fetch(symbols, *, dry_run=False):
        seen.append((tuple(symbols), dry_run))
        if dry_run:
            return await orig_fetch(symbols, dry_run=True)
        return await orig_fetch(symbols)

    monkeypatch.setattr(main_bot, "_scanner_fetch_quotes", counting_fetch)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        main_bot._LIVE_POLICY["pro_trader_mode"] = "shadow"
        client = _fresh_client()
        main_bot.massive_client = client
        desk = _fresh_desk()
        main_bot._PRO_TRADER = desk
        main_bot._scanner_quote_source.clear()
        main_bot._scanner_quote_source.update({
            "last": "seeded", "at": "seed-at", "fallback_reason": "seed-reason",
        })
        await _seed_store(store)
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        main_bot.portfolio_risk.note_spy_gap_session(0.4, True, main_bot._session_day_et())
        with tempfile.TemporaryDirectory() as tmp:
            ledger_path = Path(tmp) / "ledger.jsonl"
            ledger_path.write_bytes(b'{"event":"seed"}\n')
            main_bot.trade_ledger.init(ledger_path)
            before_bytes = ledger_path.read_bytes()
            before_memory = _memory_snapshot(store)
            before_desk = copy.deepcopy(desk.snapshot())
            memory_id = id(desk.memory)
            before_counters = copy.deepcopy(client.export_counters())
            before_source = dict(main_bot._scanner_quote_source)

            result = await main_bot.explain_signal(_payload())

            assert result["bot_version"] == "5.99.13-pro-day-trader"
            assert result["dry_run"] is True
            assert result["placed"] is False
            assert isinstance(result.get("pro_trader"), dict)
            assert seen, "quote fetch was not reached"
            assert all(flag is True for _syms, flag in seen)
            assert any("SPY" in syms or "QQQ" in syms for syms, _flag in seen)
            assert ledger_path.read_bytes() == before_bytes
            assert _memory_snapshot(store) == before_memory
            assert main_bot._PRO_TRADER is desk
            assert id(main_bot._PRO_TRADER.memory) == memory_id
            assert copy.deepcopy(main_bot._PRO_TRADER.snapshot()) == before_desk
            assert client.export_counters() == before_counters
            assert dict(main_bot._scanner_quote_source) == before_source
            assert await store.get(main_bot.DESK_STATE_KEY) == "sentinel-desk"
            assert await store.get(main_bot.FEED_HEALTH_KEY) == "sentinel-feed"

    try:
        _run(scenario())
    finally:
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        _restore(saved)


def test_explain_reports_filter_exception_as_refused(monkeypatch):
    async def boom(_payload, *, dry_run=False):
        raise RuntimeError("filter blew up")

    monkeypatch.setattr(main_bot, "_passes_entry_filters", boom)
    result = _run(main_bot.explain_signal({
        "ticker": "NVDA",
        "action": "BUY",
        "instrument": "stock",
        "mode": "paper",
    }))
    assert result["dry_run"] is True
    assert result["placed"] is False
    assert result["filters"]["passed"] is False
    assert result["filters"]["blocked_by"] == ["filters_error: RuntimeError"]
    assert result["verdict"] == "refused"
    assert result["refused_at"] == "filters"
    assert result["reason"] == "filters_error: RuntimeError"


def test_live_filter_path_still_persists_desk_and_feed(monkeypatch):
    saved = _save()
    _arm_feed(monkeypatch)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        main_bot._LIVE_POLICY["pro_trader_mode"] = "shadow"
        client = _fresh_client()
        main_bot.massive_client = client
        desk = _fresh_desk()
        main_bot._PRO_TRADER = desk
        main_bot._scanner_quote_source.clear()
        main_bot._scanner_quote_source.update({
            "last": None, "at": None, "fallback_reason": None,
        })
        before_calls = client.calls
        before_desk = copy.deepcopy(desk.snapshot())
        payload = _payload()
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        main_bot.portfolio_risk.note_spy_gap_session(0.4, True, main_bot._session_day_et())

        await main_bot._passes_entry_filters(payload)

        raw_desk = await store.get(main_bot.DESK_STATE_KEY)
        raw_feed = await store.get(main_bot.FEED_HEALTH_KEY)
        assert raw_desk and raw_desk != "sentinel-desk"
        persisted = json.loads(raw_desk)
        assert persisted["trades_completed"] == main_bot._PRO_TRADER.trades_completed
        assert persisted["trades_completed"] != before_desk["trades_completed"]
        assert main_bot._PRO_TRADER.min_trades != before_desk["min_trades"]
        assert raw_feed
        blob = json.loads(raw_feed)
        assert blob["calls"] == client.calls
        assert client.calls > before_calls
        assert main_bot._scanner_quote_source["last"] == "massive"
        assert isinstance(payload.get("_pro_trader"), dict)
        assert isinstance(main_bot._PRO_TRADER.last, dict)

    try:
        _run(scenario())
    finally:
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        _restore(saved)


def test_status_exposes_call_index_vwap_gate(monkeypatch):
    frozen_enabled = main_bot.CALL_INDEX_VWAP_GATE
    frozen_symbols = main_bot.CALL_INDEX_VWAP_SYMBOLS
    monkeypatch.setenv("CALL_INDEX_VWAP_GATE", "false")
    monkeypatch.setenv("CALL_INDEX_VWAP_SYMBOLS", "dia, spy")
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    with TestClient(main_bot.app) as client:
        response = client.get("/status", headers={"X-Rork-Secret": "s3cret"})
    assert response.status_code == 200
    gate = response.json()["filters"]["call_index_vwap_gate"]
    assert gate["enabled"] is frozen_enabled
    assert gate["symbols"] == list(frozen_symbols)
    assert gate["mode"] == ("fail_closed" if frozen_enabled else "off")
    assert gate["observe"]["read_only"] is True
    assert isinstance(gate["observe"]["indexes"], list)
    assert main_bot.CALL_INDEX_VWAP_GATE is frozen_enabled
    assert main_bot.CALL_INDEX_VWAP_SYMBOLS == frozen_symbols

    monkeypatch.setenv("CALL_INDEX_VWAP_GATE", "TRUE")
    monkeypatch.delenv("CALL_INDEX_VWAP_SYMBOLS", raising=False)
    default = main_bot.call_index_vwap_gate_status()
    assert default == {
        "enabled": frozen_enabled,
        "symbols": list(frozen_symbols),
        "mode": "fail_closed" if frozen_enabled else "off",
        "vwap_source": "session_sampled",
        "vwap_min_today_samples": main_bot.scanner_mod.vwap_min_today_samples(),
        "vwap_crosscheck_max_bps": main_bot.vwap_crosscheck_max_bps(),
        "vwap_crosscheck_required": main_bot.vwap_crosscheck_required(),
        "vwap_crosscheck_max_age_s": main_bot.vwap_crosscheck_max_age_s(),
    }
    monkeypatch.setenv("CALL_INDEX_VWAP_GATE", "yes")
    assert main_bot.call_index_vwap_gate_status()["enabled"] is frozen_enabled
    assert main_bot.CALL_INDEX_VWAP_GATE is frozen_enabled
    assert main_bot.CALL_INDEX_VWAP_SYMBOLS == frozen_symbols
