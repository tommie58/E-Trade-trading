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
