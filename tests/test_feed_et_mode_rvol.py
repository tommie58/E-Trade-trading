"""Feed persistence, ET-day pacing, mode rejection, and bounded RVOL fallback."""
import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

import main_bot
import pro_trader
import scanner
import state_store

ET = ZoneInfo("America/New_York")
SESSION = datetime(2026, 11, 23, 15, 58, tzinfo=ET)


def _run(coro):
    return asyncio.run(coro)


def _restore_globals(saved):
    main_bot.state = saved["state"]
    main_bot.massive_client = saved["client"]
    main_bot._scanner_quote_source.clear()
    main_bot._scanner_quote_source.update(saved["source"])
    main_bot._PRO_TRADER = saved["desk"]


def _save_globals():
    return {
        "state": main_bot.state,
        "client": main_bot.massive_client,
        "source": dict(main_bot._scanner_quote_source),
        "desk": main_bot._PRO_TRADER,
    }


def test_invalid_pro_trader_mode_is_rejected_not_coerced():
    mode, warning = main_bot.resolve_pro_trader_mode("live")
    assert mode == "shadow"
    assert warning and "rejected" in warning and "live" in warning
    assert main_bot.resolve_pro_trader_mode(None) == ("shadow", None)
    assert main_bot.resolve_pro_trader_mode("") == ("shadow", None)
    assert main_bot.resolve_pro_trader_mode("active") == ("active", None)
    # Internal callers still coerce so a bad decision cannot change an order.
    assert pro_trader.normalize_mode("nope") == "shadow"
    assert pro_trader.normalize_mode(None) == "shadow"

    current = dict(main_bot._LIVE_POLICY)
    kept = main_bot.plan_policy_sync(current, {"pro_trader_mode": "live", "risk_per_trade_pct": 8})
    assert kept["policy"]["pro_trader_mode"] == current["pro_trader_mode"]
    assert kept["policy"]["risk_per_trade_pct"] == current["risk_per_trade_pct"]
    assert kept["policy"]["daily_loss_limit_pct"] == current["daily_loss_limit_pct"]
    assert any("live" in item and "rejected" in item for item in kept["ignored"])
    assert not any("pro_trader_mode" in item and "→" in item for item in kept["changes"])


def test_feed_health_survives_restart_and_cold_start_is_not_healthy(monkeypatch):
    saved = _save_globals()

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        client = main_bot.massive.MassiveClient()
        client.calls = 8
        client.failures = 3
        client.last_error = "Massive rejected the API key (HTTP 403)"
        client.auth_status = 403
        client.last_ok_at = 1_700_000_000.0
        client._adv = {"NVDA": 1_000_000.0}
        client._adv_day = "2026-11-23"
        main_bot.massive_client = client
        main_bot._scanner_quote_source.update({
            "last": "etrade",
            "at": "2026-11-23T20:58:00+00:00",
            "fallback_reason": "Massive rejected the API key (HTTP 403)",
        })
        await main_bot._persist_feed_health()

        fresh = main_bot.massive.MassiveClient()
        main_bot.massive_client = fresh
        main_bot._scanner_quote_source.update({"last": None, "at": None, "fallback_reason": None})
        await main_bot._restore_feed_health()
        assert fresh.calls == 8
        assert fresh.failures == 3
        assert fresh.auth_status == 403
        assert fresh.last_ok_at == 1_700_000_000.0
        assert "403" in (fresh.last_error or "")
        assert fresh._adv.get("NVDA") == 1_000_000.0
        assert main_bot._scanner_quote_source["last"] == "etrade"
        assert "403" in (main_bot._scanner_quote_source.get("fallback_reason") or "")

        monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
        health = main_bot._pro_trader_feed_health()
        assert health["ok"] is False
        assert health["http_status"] == 403
        assert health["fallback"] == "etrade"
        classified, _memory = pro_trader.classify(
            {
                "spy_vs_vwap_pct": 0.45, "qqq_vs_vwap_pct": 0.55, "efficiency": 0.72,
                "vol_ratio": 1.1, "minute_et": 14 * 60 + 58, "event_active": False,
                "feed": health,
            },
            {"regime": "TREND_UP", "pending": None, "pending_count": 0},
        )
        assert classified["regime"] == "DEGRADED_DATA"
        assert "massive_http_403" in classified["reason_codes"]

        cold = main_bot.massive.MassiveClient()
        main_bot.massive_client = cold
        main_bot._scanner_quote_source.update({"last": None, "at": None, "fallback_reason": None})
        cold_health = main_bot._pro_trader_feed_health()
        assert cold_health["ok"] is False
        assert cold_health["proven"] is False
        assert cold.last_ok_at is None

        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        assert main_bot._pro_trader_feed_health()["ok"] is True

    try:
        _run(scenario())
    finally:
        _restore_globals(saved)


def test_desk_state_survives_restart_without_redis():
    saved = _save_globals()

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        desk = pro_trader.Desk()
        decision = desk.evaluate(
            {
                "spy_vs_vwap_pct": 0.45, "qqq_vs_vwap_pct": 0.55, "efficiency": 0.72,
                "vol_ratio": 1.1, "minute_et": 11 * 60, "event_active": False,
                "feed": {"ok": True, "provider": "massive"},
            },
            {"ticker": "NVDA", "setup": "breakout", "score": 90, "rvol": 1.6,
             "option_right": "CALL", "instrument": "option", "spread_pct": 4.0},
            mode="shadow", trades_completed=2, min_trades=4,
        )
        desk.remember(decision, accepted=True)
        regime = desk.memory.get("regime")
        main_bot._PRO_TRADER = desk
        await main_bot._persist_desk()
        raw = await store.get(main_bot.DESK_STATE_KEY)
        assert raw and json.loads(raw)["memory"]["regime"] == regime

        main_bot._PRO_TRADER = pro_trader.Desk()
        await main_bot._restore_desk()
        assert main_bot._PRO_TRADER.memory.get("regime") == regime
        assert main_bot._PRO_TRADER.decisions
        assert main_bot._PRO_TRADER.decisions[-1]["verdict"] == decision["verdict"]
        assert main_bot._PRO_TRADER.trades_completed == 2

    try:
        _run(scenario())
    finally:
        _restore_globals(saved)


def test_et_day_counters_survive_utc_midnight(monkeypatch):
    saved = _save_globals()
    days = {"utc": "2026-11-23", "et": "2026-11-23"}
    monkeypatch.setattr(state_store, "_today", lambda: days["utc"])
    monkeypatch.setattr(state_store, "_today_et", lambda: days["et"])

    async def observation(_payload):
        return {
            "minute_et": 18 * 60, "event_active": False,
            "feed": {"ok": True, "provider": "massive"},
            "spy_vs_vwap_pct": 0.4, "qqq_vs_vwap_pct": 0.4, "efficiency": 0.7, "vol_ratio": 1.0,
        }

    monkeypatch.setattr(main_bot, "_pro_trader_observation", observation)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        for _ in range(4):
            await store.incr_trades_today()
        await store.add_realized_pnl(-1.5)
        assert (await store.get_daily())["trades_today"] == 4

        days["utc"] = "2026-11-24"  # 00:00 UTC == 7:00 PM ET, session still 2026-11-23
        utc = await store.get_daily()
        session = await store.get_trading_day()
        assert utc["trades_today"] == 0
        assert utc["realized_pnl_today_pct"] == 0
        assert session["day_et"] == "2026-11-23"
        assert session["trades_today"] == 4
        assert session["realized_pnl_today_pct"] == pytest.approx(-1.5)

        await store.incr_trades_today()
        assert (await store.get_daily())["trades_today"] == 1
        assert (await store.get_trading_day())["trades_today"] == 4

        # Pre-mirror data still lives on daily:{ET date}. Evening reads use it.
        await store.delete("daily_et:2026-11-23")
        legacy = await store.get_trading_day()
        assert legacy["trades_today"] == 4
        assert legacy["realized_pnl_today_pct"] == pytest.approx(-1.5)

        payload = {
            "ticker": "NVDA", "instrument": "option", "setup": "breakout",
            "score": 90, "rvol": 1.6, "option_right": "CALL", "action": "BUY",
        }
        blocked = await main_bot._fold_pro_trader(payload, [])
        assert blocked == []
        pacing = payload["_pro_trader"]["pacing"]
        assert pacing["trades_completed"] == 4
        assert pacing["min_trades_per_day"] == 4
        assert pacing["state"] == "met"

    try:
        _run(scenario())
    finally:
        _restore_globals(saved)


def test_status_and_pro_trader_pacing_read_the_et_day(monkeypatch):
    async def fake_trading_day(self):
        return {"day_et": "2026-11-23", "trades_today": 4, "realized_pnl_today_pct": -1.25}

    monkeypatch.setattr(main_bot.StateStore, "get_trading_day", fake_trading_day)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    headers = {"X-Rork-Secret": "s3cret"}
    with TestClient(main_bot.app) as client:
        status = client.get("/status", headers=headers)
        desk = client.get("/pro-trader/status", headers=headers)
        report = client.get("/pro-trader/report", headers=headers)
    assert status.status_code == 200
    body = status.json()
    assert body["trades_today"] == 4
    assert body["realized_pnl_today_pct"] == pytest.approx(-1.25)
    assert body["pro_trader"]["trades_completed"] == 4
    assert body["pro_trader"]["pacing"]["trades_completed"] == 4
    assert desk.status_code == 200
    assert desk.json()["trades_completed"] == 4
    assert desk.json()["pacing"]["trades_completed"] == 4
    assert report.status_code == 200
    assert report.json()["trades_completed"] == 4
    assert report.json()["pacing"]["trades_completed"] == 4


def test_rvol_fallback_is_bounded_and_fail_closed():
    progress = scanner.session_progress(SESSION)
    prior = {"rvol": 2.4, "ts": SESSION.timestamp() - 10 * 60, "day_et": "2026-11-23"}
    direct = scanner.resolve_rvol(
        {"total_volume": 2_000_000, "avg_daily_volume": 1_000_000},
        [], progress, SESSION, prior=prior,
    )
    assert direct["basis"] == "measured"
    assert direct["rvol"] != 2.4

    zero = scanner.resolve_rvol(
        {"total_volume": 0, "avg_daily_volume": 1_000_000},
        [], progress, SESSION, prior=prior,
    )
    assert zero["rvol"] is None
    assert zero["forget"] is True

    series = [{
        "ts": (SESSION - timedelta(minutes=5)).isoformat(),
        "total_volume": 1_500_000,
    }]
    historic = scanner.resolve_rvol(
        {"symbol": "NVDA"}, series, progress, SESSION, cached_adv=1_000_000,
    )
    assert historic["basis"] == "broker_history"
    assert historic["rvol"] is not None and historic["rvol"] >= 0.9

    carried = scanner.resolve_rvol({}, [], progress, SESSION, prior=prior)
    assert carried["basis"] == "carried"
    assert carried["rvol"] == 2.4
    assert carried["refresh"] is False

    stale_prior = {
        "rvol": 4.35, "day_et": "2026-11-23",
        "ts": SESSION.timestamp() - scanner.RVOL_FALLBACK_TTL_SECONDS - 5,
    }
    assert scanner.resolve_rvol({}, [], progress, SESSION, prior=stale_prior)["rvol"] is None
    other_day = {"rvol": 4.35, "ts": SESSION.timestamp() - 30, "day_et": "2026-11-22"}
    assert scanner.resolve_rvol({}, [], progress, SESSION, prior=other_day)["rvol"] is None
    old_series = [{
        "ts": (SESSION - timedelta(minutes=20)).isoformat(),
        "total_volume": 5_000_000,
    }]
    assert scanner.resolve_rvol({}, old_series, progress, SESSION, cached_adv=1_000_000)["rvol"] is None

    cfg = {"min_rvol": 0.9, "min_price": 1, "max_spread_pct": 5, "min_score": 0}
    quote = {
        "symbol": "NVDA", "last": 100.0, "bid": 99.9, "ask": 100.1, "open": 99.0,
        "total_volume": None, "avg_daily_volume": None,
    }
    low = scanner.evaluate_quote(
        quote, [], cfg, progress,
        {"now_et": SESSION, "rvol_prior": {"NVDA": {"rvol": 0.2, "ts": SESSION.timestamp() - 60, "day_et": "2026-11-23"}}},
    )
    assert low["rvol"] == 0.2
    assert low["qualifies"] is False
    assert any(reason.startswith("rvol 0.2 <") for reason in low["blocked_by"])

    expired = scanner.evaluate_quote(
        quote, [], cfg, progress,
        {"now_et": SESSION, "rvol_prior": {"NVDA": stale_prior}},
    )
    assert expired["rvol"] is None
    assert any("not measurable" in reason for reason in expired["blocked_by"])
    assert expired["qualifies"] is False


def test_rvol_memory_round_trips_on_the_memory_backend():
    async def scenario():
        store = main_bot.StateStore(None)

        async def _noop(*_args, **_kwargs):
            return None

        desk = scanner.Scanner(
            store, _noop, _noop, lambda: True, lambda: False, lambda: SESSION,
        )
        priors = {}
        assert desk._absorb_rvol(
            "NVDA", {"total_volume": None}, {"rvol": 2.4, "rvol_basis": "carried"}, SESSION, priors,
        ) is False
        assert priors == {}
        assert desk._absorb_rvol(
            "nvda",
            {"total_volume": 1_500_000, "avg_daily_volume": 1_000_000},
            {"rvol": 1.8, "rvol_basis": "broker_history"},
            SESSION, priors,
        ) is True
        await desk._save_rvol_priors(priors)
        restarted = scanner.Scanner(
            store, _noop, _noop, lambda: True, lambda: False, lambda: SESSION,
        )
        loaded = await restarted._load_rvol_priors()
        assert loaded["NVDA"]["rvol"] == 1.8
        assert loaded["NVDA"]["day_et"] == "2026-11-23"

    _run(scenario())
