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


def test_native_handoff_waits_for_breakeven():
    # The 2026-11-19 defect: a small bounce armed a 2×spread trail and replaced
    # the ~20% hard stop. Until breakeven locks, the fixed stop stays.
    early = main_bot.trailing_engine.plan_native_handoff(
        3.10, 0.10, 0.60, 2.40, tick=0.05,
        breakeven_locked=False, option=True, mark=3.10,
    )
    assert early["arm"] is False
    assert early["reason"] == "pre_breakeven"


def test_native_handoff_arms_when_the_trigger_clears_the_stop():
    # After breakeven, options trail at 12.5% of premium — not 2×spread.
    # Bid 3.80, 12.5% → 0.475 → 0.50 tick; trigger 3.30 clears a BE stop at 3.00.
    plan = main_bot.trailing_engine.plan_native_handoff(
        3.80, 0.10, 0.60, 3.00, tick=0.05,
        breakeven_locked=True, option=True, mark=3.80,
    )
    assert plan["arm"] is True
    assert plan["reason"] == "arm"
    assert plan["offset"] == 0.50
    assert plan["trigger"] == 3.30


def test_native_option_trail_offset_is_percent_of_premium():
    plan = main_bot.trailing_engine.native_trail_offset(
        0.10, risk=0.80, tick=0.05, mark=4.00, option=True,
    )
    assert plan["ok"] is True
    assert plan["offset"] == 0.50  # 12.5% of 4.00
    assert "12.5%" in plan["detail"]
    # Equities keep the spread multiple.
    eq = main_bot.trailing_engine.native_trail_offset(0.10, risk=5.0, tick=0.01)
    assert eq["ok"] is True and eq["offset"] == 0.20


def test_native_handoff_keeps_a_tighter_fixed_stop():
    # At breakeven the resting stop is entry; a 12.5% trail from a small run
    # would sit below it — keep the fixed stop.
    plan = main_bot.trailing_engine.plan_native_handoff(
        3.24, 0.10, 0.60, 3.00, tick=0.05,
        breakeven_locked=True, option=True, mark=3.24,
    )
    assert plan["arm"] is False
    assert plan["reason"] == "would_loosen"
    assert plan["trigger"] is None


def test_native_handoff_refuses_a_wide_or_unmeasured_book():
    # Percent gap wider than the position's own risk → refuse.
    wide = main_bot.trailing_engine.plan_native_handoff(
        10.00, 0.10, 0.80, 8.00, tick=0.05,
        breakeven_locked=True, option=True, mark=10.00,
    )
    blind = main_bot.trailing_engine.plan_native_handoff(
        3.40, None, 0.80, 2.40, tick=0.05,
        breakeven_locked=True, option=True, mark=0,
    )
    # Equity path still needs a measured spread.
    eq_blind = main_bot.trailing_engine.plan_native_handoff(
        100.0, None, 2.0, 98.0, tick=0.01,
        breakeven_locked=True, option=False,
    )
    assert wide["arm"] is False and wide["reason"] == "offset"
    assert blind["arm"] is False and blind["reason"] == "offset"
    assert eq_blind["arm"] is False and eq_blind["reason"] == "offset"


def test_native_handoff_needs_a_long_with_a_resting_stop():
    no_order = main_bot.trailing_engine.plan_native_handoff(
        3.80, 0.10, 0.60, 3.00, has_stop_order=False,
        breakeven_locked=True, option=True, mark=3.80,
    )
    short = main_bot.trailing_engine.plan_native_handoff(
        3.80, 0.10, 0.60, 3.00, long_side=False,
        breakeven_locked=True, option=True, mark=3.80,
    )
    covered = main_bot.trailing_engine.plan_native_handoff(
        3.80, 0.10, 0.60, 3.00, already_native=True,
        breakeven_locked=True, option=True, mark=3.80,
    )
    assert no_order["reason"] == "no_stop_order"
    assert short["reason"] == "short"
    assert covered["reason"] == "already_native"
    assert not any(p["arm"] for p in (no_order, short, covered))


def test_engine_trail_stays_at_hard_stop_until_breakeven():
    te = main_bot.trailing_engine
    early = te.compute_trail_level("BUY", 3.0, 2.4, 3.1, 2.9)
    assert early["trail_price"] == 2.4
    assert early["breakeven_locked"] is False
    assert early["tier"] == "initial"
    locked = te.compute_trail_level("BUY", 3.0, 2.4, 3.25, 2.9)
    assert locked["breakeven_locked"] is True
    assert locked["trail_price"] >= 3.0


def test_estimate_atr_from_eyes_uses_series_and_session():
    te = main_bot.trailing_engine
    series = [{"last": 100.0}, {"last": 101.0}, {"last": 102.5}, {"last": 101.5}]
    out = te.estimate_atr_from_eyes(series=series, session_high=105.0, session_low=99.0)
    # Session range 6 > series span 2.5 → session wins.
    assert out["atr"] == 6.0
    assert out["source"] == "session"
    series_only = te.estimate_atr_from_eyes(series=series)
    assert series_only["atr"] == 2.5
    assert series_only["source"] == "series"
    # Too few samples and no session → nothing invented.
    thin = te.estimate_atr_from_eyes(series=[{"last": 100.0}, {"last": 101.0}])
    assert thin["atr"] is None


def test_estimate_atr_from_eyes_scales_options_by_delta():
    te = main_bot.trailing_engine
    out = te.estimate_atr_from_eyes(
        session_high=210.0, session_low=200.0, is_option=True, delta=0.5, mark=5.0,
    )
    # Underlying range 10 × 0.5 delta = 5.0 premium ATR (capped at mark).
    assert out["atr"] == 5.0
    assert out["source"] == "session*delta"
    # Prior already on premium basis is not re-scaled.
    prior = te.estimate_atr_from_eyes(
        session_high=210.0, session_low=200.0, is_option=True, delta=0.5,
        prior_atr=8.0, mark=12.0,
    )
    assert prior["atr"] == 8.0
    assert prior["source"] == "prior"


def test_eyes_atr_widens_trail_noise_floor():
    te = main_bot.trailing_engine
    # +1R open profit → "protect" tier trails 0.75R (= 0.45) without ATR.
    bare = te.compute_trail_level("BUY", 3.0, 2.4, 3.6, 2.9)
    assert bare["tier"] == "protect"
    with_atr = te.compute_trail_level("BUY", 3.0, 2.4, 3.6, 2.9, atr=2.0)
    # 0.45 × ATR=2.0 = 0.90 > 0.75R=0.45 → wider floor, lower trail price.
    assert with_atr["trail_distance"] >= bare["trail_distance"]
    assert with_atr["trail_price"] <= bare["trail_price"]


def test_apply_eyes_freezes_trail_in_stressed_regime():
    te = main_bot.trailing_engine
    pos = te.apply_eyes_to_trail_position(
        {"entry_premium": 3.0, "trail_mode": "full"},
        atr=1.2, atr_source="series*delta", vol_regime="stressed",
    )
    assert pos["atr"] == 1.2
    assert pos["atr_source"] == "series*delta"
    assert pos["trail_mode"] == "breakeven_only"
    # Calm must not unlock a frozen entry mode.
    kept = te.apply_eyes_to_trail_position(
        {"trail_mode": "breakeven_only"}, vol_regime="calm",
    )
    assert kept["trail_mode"] == "breakeven_only"


def test_build_trade_review_fields_keeps_decision_context():
    pos = {
        "origin": "app",
        "signal_id": "sig-42",
        "entry_premium": 3.5,
        "stop_premium": 2.8,
        "tactical_stop_premium": 2.8,
        "trail": 3.1,
        "atr": 0.45,
        "atr_source": "series*delta",
        "trail_mode": "breakeven_only",
        "trail_tier": "protect",
        "trail_mode_source": "eyes_stressed",
        "breakeven_locked": True,
        "native_trail_order_id": "99",
        "native_trail_offset": 0.4,
        "option_delta": 0.55,
        "contract": {
            "right": "CALL", "strike": 200.0, "expiration": "2026-11-21",
            "occ_symbol": "AAPL261121C00200000",
        },
        "edge": {"vol_regime": "stressed"},
        "app_decides": True,
    }
    review = main_bot.build_trade_review_fields(pos)
    assert review["origin"] == "app"
    assert review["signal_id"] == "sig-42"
    assert review["instrument"] == "option"
    assert review["final_stop"] == 3.1
    assert review["atr"] == 0.45
    assert review["native_trail"] is True
    assert review["contract"]["strike"] == 200.0
    assert review["vol_regime"] == "stressed"
    # Empty position invents nothing.
    empty = main_bot.build_trade_review_fields({})
    assert empty["atr"] is None
    assert empty["signal_id"] is None
    assert empty["instrument"] == "equity"


def test_call_index_vwap_gate_blocks_calls_below_vwap():
    below = main_bot.assess_call_index_vwap_gate(
        "CALL",
        [
            {"symbol": "SPY", "last": 500.0, "vwap": 505.0},
            {"symbol": "QQQ", "last": 400.0, "vwap": 410.0},
        ],
    )
    assert below is not None and "below day VWAP" in below
    above = main_bot.assess_call_index_vwap_gate(
        "CALL",
        [
            {"symbol": "SPY", "last": 510.0, "vwap": 505.0},
            {"symbol": "QQQ", "last": 400.0, "vwap": 410.0},
        ],
    )
    assert above is None
    assert main_bot.assess_call_index_vwap_gate("PUT", [
        {"symbol": "SPY", "last": 500.0, "vwap": 505.0},
    ]) is None
    assert main_bot.assess_call_index_vwap_gate("CALL", [], enabled=False) is None
    # Unmeasured → fail open.
    assert main_bot.assess_call_index_vwap_gate("CALL", [
        {"symbol": "SPY", "last": 500.0, "vwap": None},
    ]) is None


def test_native_trail_retry_waits_until_the_stamp():
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    assert main_bot._native_trail_retry_waiting({"native_trail_retry_after": future})
    assert not main_bot._native_trail_retry_waiting({"native_trail_retry_after": past})
    assert not main_bot._native_trail_retry_waiting({})
    assert not main_bot._native_trail_retry_waiting({"native_trail_retry_after": "not-a-time"})


def test_native_trail_order_prices_support_counter_swap():
    documented = main_bot.trailing_engine.native_trail_order_prices(0.80, 7.60)
    swapped = main_bot.trailing_engine.native_trail_order_prices(
        0.80, 7.60, counter_swap=True,
    )
    assert documented == {"stop_price": 7.60, "offset_value": 0.80}
    assert swapped == {"stop_price": 0.80, "offset_value": 7.60}


def test_classify_resting_trail_offset_detects_the_etrade_swap():
    # Live 2026-11-13 GOOGL shape: intended trail ~0.80, initial trigger ~7.60,
    # broker Stop Val came back as 7.6 (the trigger).
    ok = main_bot.trailing_engine.classify_resting_trail_offset(0.80, 7.60, 0.80)
    swapped = main_bot.trailing_engine.classify_resting_trail_offset(0.80, 7.60, 7.60)
    wrong = main_bot.trailing_engine.classify_resting_trail_offset(0.80, 7.60, 1.85)
    unknown = main_bot.trailing_engine.classify_resting_trail_offset(0.80, 7.60, None)
    assert ok["status"] == "ok"
    assert swapped["status"] == "swapped"
    assert wrong["status"] == "wrong"
    assert unknown["status"] == "unknown"


def test_native_trail_offset_looks_inflated_on_live_googl_smci_distances():
    assert main_bot.trailing_engine.native_trail_offset_looks_inflated(7.6, 0.80)
    assert main_bot.trailing_engine.native_trail_offset_looks_inflated(1.85, 0.30)
    assert not main_bot.trailing_engine.native_trail_offset_looks_inflated(0.80, 0.80)
    assert not main_bot.trailing_engine.native_trail_offset_looks_inflated(0.30, 0.30)


def test_parse_open_orders_captures_trail_offset_value():
    resp = {
        "OrdersResponse": {
            "Order": {
                "orderId": 1598,
                "OrderDetail": {
                    "status": "OPEN",
                    "priceType": "TRAILING_STOP_CNST",
                    "stopPrice": 3.35,
                    "offsetValue": 7.6,
                    "Instrument": {
                        "orderAction": "SELL_CLOSE",
                        "orderedQuantity": 1,
                        "filledQuantity": 0,
                        "Product": {"symbol": "GOOGL", "securityType": "OPTN"},
                    },
                },
            }
        }
    }
    orders = main_bot.reconciliation.parse_open_orders(resp)
    assert len(orders) == 1
    assert orders[0]["order_id"] == "1598"
    assert orders[0]["stop_price"] == 3.35
    assert orders[0]["offset_value"] == 7.6
    assert main_bot.reconciliation.trail_offset_from_order_detail(
        {"trailPrice": 1.85}
    ) == 1.85


def test_order_detail_sends_offset_and_stop_together():
    detail = main_bot.ETradeAsyncClient.order_detail(
        [{"orderAction": "SELL_CLOSE", "quantity": 1}],
        "TRAILING_STOP_CNST",
        stop_price=7.60,
        offset_value=0.80,
    )
    assert detail["priceType"] == "TRAILING_STOP_CNST"
    assert detail["stopPrice"] == 7.60
    assert detail["offsetValue"] == 0.80
    assert detail["trailPrice"] == 0.80
    assert detail["offsetType"] == "TRAILING_STOP_CNST"
