"""Pure gates for the 5.85 fail-closed edits. No broker, no network."""
import asyncio

import pytest
from fastapi import HTTPException

import main_bot


def test_broker_env_ignores_an_inline_note():
    assert main_bot.broker_env("sandbox            # sandbox | production") == "sandbox"
    assert main_bot.broker_env("  SANDBOX  ") == "sandbox"
    assert main_bot.broker_env("production") == "production"
    assert main_bot.broker_env("sandbox # comment") == "sandbox"
    assert main_bot.broker_env("") == "production"
    assert main_bot.broker_env(None) == "production"
    assert main_bot.broker_env("live") == "live"


def test_secrets_match_is_exact():
    assert main_bot._secrets_match("same-secret", "same-secret")
    assert not main_bot._secrets_match("same-secret", "same-secret ")
    assert not main_bot._secrets_match("short", "same-secret")
    assert not main_bot._secrets_match(None, "same-secret")
    assert not main_bot._secrets_match("", "same-secret")


def test_webhook_auth_fails_closed_without_secret(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", None)
    with pytest.raises(HTTPException) as exc:
        main_bot._verify_webhook_auth(b"{}", "anything", "anything", None)
    assert exc.value.status_code == 401


def test_webhook_auth_accepts_secret_when_signature_is_wrong(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    main_bot._verify_webhook_auth(b"{}", "s3cret", None, "sha256=not-the-signature")
    main_bot._verify_webhook_auth(b"{}", None, "s3cret", "")


def test_webhook_auth_rejects_a_bad_secret(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    with pytest.raises(HTTPException) as exc:
        main_bot._verify_webhook_auth(b"{}", "nope", None, None)
    assert exc.value.status_code == 401


def test_force_execute_requires_the_host_flag(monkeypatch):
    monkeypatch.setattr(main_bot, "FORCE_EXECUTE_ALLOWED", False)
    assert not main_bot._force_execute({"force_execute": True})
    monkeypatch.setattr(main_bot, "FORCE_EXECUTE_ALLOWED", True)
    assert main_bot._force_execute({"force_execute": True})
    assert not main_bot._force_execute({"force_execute": False})


def test_signal_id_is_stable_and_ignores_timestamp():
    first = {"signal_id": "sig-1", "ticker": "NVDA", "timestamp": "1"}
    retry = {"signal_id": "sig-1", "ticker": "NVDA", "timestamp": "2"}
    other = {"signal_id": "sig-2", "ticker": "NVDA", "timestamp": "1"}
    assert main_bot.signal_identity(first) == "sig-1"
    assert main_bot._signal_key(first) == main_bot._signal_key(retry)
    assert main_bot._signal_key(first) != main_bot._signal_key(other)
    assert main_bot.client_order_id_for("sig-1") == main_bot.client_order_id_for("sig-1")
    assert main_bot.client_order_id_for("sig-1") != main_bot.client_order_id_for("sig-2")
    assert len(main_bot.client_order_id_for("sig-1")) == 18
    assert main_bot.client_order_id_for("sig-1").isdigit()


def test_scanner_signal_id_is_one_idea_per_session():
    import scanner

    morning = scanner.scanner_signal_id("nvda", "long", "vwap reclaim", "2026-11-11T14:33:38+00:00")
    rescan = scanner.scanner_signal_id("NVDA", "LONG", "vwap   reclaim", "2026-11-11T14:51:33Z")
    assert morning == rescan
    assert morning == "scanner:2026-11-11:NVDA:long:vwap reclaim"
    assert len(morning) <= 80
    # 04:30 UTC is still the previous ET date (EST, UTC-5).
    late = scanner.scanner_signal_id("NVDA", "long", "vwap reclaim", "2026-11-12T04:30:00+00:00")
    assert late == morning
    other_setup = scanner.scanner_signal_id("NVDA", "long", "breakout", "2026-11-11T14:33:38+00:00")
    other_side = scanner.scanner_signal_id("NVDA", "short", "vwap reclaim", "2026-11-11T14:33:38+00:00")
    next_day = scanner.scanner_signal_id("NVDA", "long", "vwap reclaim", "2026-11-12T15:00:00+00:00")
    assert len({morning, other_setup, other_side, next_day}) == 4
    long_setup = scanner.scanner_signal_id("NVDA", "long", "x" * 200, "2026-11-11T14:33:38+00:00")
    assert len(long_setup) <= 80
    assert long_setup == scanner.scanner_signal_id("NVDA", "long", "x" * 200, "2026-11-11T18:00:00+00:00")


def test_build_signal_stamps_a_stable_signal_id(monkeypatch):
    import scanner

    monkeypatch.setattr(scanner, "ITM_EXPRESSION", False)
    candidate = {
        "ticker": "MU",
        "side": "long",
        "entry": 783.86,
        "stop": 773.04,
        "target": 800.0,
        "score": 93,
        "rvol": 3.04,
        "setup": "vwap reclaim",
        "ts": "2026-11-11T14:51:33+00:00",
    }
    first = scanner.build_signal({"bid": 1.0, "ask": 1.2}, candidate, "live")
    candidate["entry"] = 790.0
    candidate["ts"] = "2026-11-11T15:10:00+00:00"
    second = scanner.build_signal({}, candidate, "live")
    assert first["signal_id"] == second["signal_id"]
    assert first["signal_id"] == "scanner:2026-11-11:MU:long:vwap reclaim"
    assert main_bot.signal_identity(first) == first["signal_id"]
    assert main_bot.client_order_id_for(first["signal_id"]) == main_bot.client_order_id_for(second["signal_id"])


def test_fallback_key_keeps_intent_when_signal_id_is_missing():
    entry = {"ticker": "NVDA", "action": "BUY", "timestamp": "1", "intent": "open"}
    close = {"ticker": "NVDA", "action": "BUY", "timestamp": "1", "intent": "close"}
    assert main_bot._signal_key(entry) != main_bot._signal_key(close)


def test_score_floor_fails_closed():
    assert main_bot.assess_score_floor(39, 40) is not None
    assert main_bot.assess_score_floor(40, 40) is None
    assert main_bot.assess_score_floor(None, 40) is not None
    assert "unreadable" in main_bot.assess_score_floor(float("nan"), 40)
    assert main_bot.assess_score_floor(10, 0) is None


def test_live_intent_needs_the_secret(monkeypatch):
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", None)
    assert not main_bot._live_intent("live")
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    assert main_bot._live_intent("live")
    assert not main_bot._live_intent("paper")


def test_memory_queue_claim_and_park_drain():
    store = main_bot.StateStore(None)

    async def scenario():
        await store.queue_push("q", "job-a")
        await store.queue_push("q", "job-b")
        claimed = await store.lmove_fifo("q", "processing")
        assert claimed == "job-a"
        assert await store.list_all("q") == ["job-b"]
        assert await store.list_all("processing") == ["job-a"]
        assert await store.list_remove_value("processing", "job-a") == 1
        assert await store.list_all("processing") == []
        await store.queue_push("parked", "one")
        await store.queue_push("parked", "two")
        assert await store.list_drain("parked") == ["one", "two"]
        assert await store.list_all("parked") == []

    asyncio.run(scenario())


def test_public_health_has_no_account_fields():
    from fastapi.testclient import TestClient

    with TestClient(main_bot.app) as client:
        health = client.get("/health")
        healthz = client.get("/healthz")
        status = client.get("/status")
    assert health.status_code == 200
    assert set(health.json()) == {"status", "version", "secret_configured"}
    assert healthz.status_code == 200
    assert "guardian" not in healthz.json()
    assert "open_positions" not in healthz.json()
    webhook = client.post("/webhook", json={"ticker": "NVDA", "action": "BUY", "mode": "live"})
    assert status.status_code == 401
    assert webhook.status_code == 401


def test_native_handoff_arms_when_the_trigger_clears_the_stop():
    plan = main_bot.trailing_engine.plan_native_handoff(
        3.40, 0.10, 0.80, 2.40, tick=0.05,
    )
    assert plan["arm"] is True
    assert plan["reason"] == "arm"
    assert plan["offset"] == 0.20
    assert plan["trigger"] == 3.20


def test_native_handoff_keeps_a_tighter_fixed_stop():
    plan = main_bot.trailing_engine.plan_native_handoff(
        2.50, 0.10, 0.80, 2.40, tick=0.05,
    )
    assert plan["arm"] is False
    assert plan["reason"] == "would_loosen"
    assert plan["trigger"] is None


def test_native_handoff_refuses_a_wide_or_unmeasured_book():
    wide = main_bot.trailing_engine.plan_native_handoff(
        3.40, 0.50, 0.80, 2.40, tick=0.05,
    )
    blind = main_bot.trailing_engine.plan_native_handoff(
        3.40, None, 0.80, 2.40, tick=0.05,
    )
    assert wide["arm"] is False and wide["reason"] == "offset"
    assert blind["arm"] is False and blind["reason"] == "offset"


def test_native_handoff_needs_a_long_with_a_resting_stop():
    no_order = main_bot.trailing_engine.plan_native_handoff(
        3.40, 0.10, 0.80, 2.40, has_stop_order=False,
    )
    short = main_bot.trailing_engine.plan_native_handoff(
        3.40, 0.10, 0.80, 2.40, long_side=False,
    )
    covered = main_bot.trailing_engine.plan_native_handoff(
        3.40, 0.10, 0.80, 2.40, already_native=True,
    )
    assert no_order["reason"] == "no_stop_order"
    assert short["reason"] == "short"
    assert covered["reason"] == "already_native"
    assert not any(p["arm"] for p in (no_order, short, covered))


def test_native_trail_retry_waits_until_the_stamp():
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    assert main_bot._native_trail_retry_waiting({"native_trail_retry_after": future})
    assert not main_bot._native_trail_retry_waiting({"native_trail_retry_after": past})
    assert not main_bot._native_trail_retry_waiting({})
    assert not main_bot._native_trail_retry_waiting({"native_trail_retry_after": "not-a-time"})
