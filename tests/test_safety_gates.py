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


def _entry_floor_option(ticker_extra=None):
    pos = {
        "action": "BUY",
        "entry_premium": 3.0,
        "stop_premium": 2.4,
        "stop_order_id": "STOP-1",
        "filled_qty": 1,
        "qty": 1,
        "breakeven_locked": True,
        "contract": {
            "right": "CALL",
            "strike": 450.0,
            "expiration": "2026-12-18",
        },
    }
    if ticker_extra:
        pos.update(ticker_extra)
    return pos


def test_native_handoff_declines_a_trigger_under_entry():
    # Repro: entry 3.00, stop 2.40, bid 3.24. 12.5% of 3.24 rounds up to a
    # 0.45 trail on the 0.05 tick, and the trigger floors to 2.79 — under
    # the fill, above the still-unmoved hard stop.
    declined = main_bot.trailing_engine.plan_native_handoff(
        3.24, 0.10, 0.60, 2.40, tick=0.05,
        breakeven_locked=True, option=True, mark=3.24, entry=3.00,
    )
    assert declined["arm"] is False
    assert declined["reason"] == "below_entry"
    assert declined["trigger"] is None
    assert "native handoff declined: below_entry" in declined["detail"]

    # Bid 3.45, same 0.45 trail, trigger lands on the fill.
    armed = main_bot.trailing_engine.plan_native_handoff(
        3.45, 0.10, 0.60, 2.40, tick=0.05,
        breakeven_locked=True, option=True, mark=3.45, entry=3.00,
    )
    assert armed["arm"] is True
    assert armed["reason"] == "arm"
    assert armed["trigger"] >= 3.00

    # entry defaults to None. Callers that omit it keep the stop-only check.
    legacy = main_bot.trailing_engine.plan_native_handoff(
        3.24, 0.10, 0.60, 2.40, tick=0.05,
        breakeven_locked=True, option=True, mark=3.24,
    )
    assert legacy["arm"] is True
    assert legacy["trigger"] == 2.79


def test_maybe_hand_off_floors_at_entry_and_leaves_the_stop(monkeypatch, caplog):
    caplog.set_level("INFO", logger="etrade-bot")
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "NATIVE_TRAIL_AUTO", True)
    calls = []

    async def arm(ticker, offset=None, min_trigger=None, source="operator", allow_below_entry=False, floor_price=None):
        calls.append({
            "min_trigger": min_trigger,
            "source": source,
            "allow_below_entry": allow_below_entry,
        })
        return {"ticker": ticker}

    monkeypatch.setattr(main_bot, "_arm_native_trail", arm)

    async def scenario():
        pos = _entry_floor_option()
        await store.set_position("QQQ", pos)
        declined = await main_bot._maybe_hand_off_native_trail("QQQ", pos, 3.24, 0.10)
        assert declined is False
        assert calls == []
        assert pos.get("native_trail_skip") == "below_entry"
        held = await store.get_position("QQQ")
        assert held["stop_order_id"] == "STOP-1"

        # Above $3 the live grid is $0.10, so 3.45 still trails to 2.95.
        # 3.50 minus the 0.50 offset lands on the fill and may hand off.
        later = _entry_floor_option()
        await store.set_position("QQQ", later)
        armed = await main_bot._maybe_hand_off_native_trail("QQQ", later, 3.50, 0.10)
        assert armed is True

    asyncio.run(scenario())
    assert calls == [{
        "min_trigger": pytest.approx(3.0),
        "source": "auto",
        "allow_below_entry": False,
    }]
    assert "native handoff declined: below_entry" in caplog.text


def test_arm_native_trail_refuses_below_entry_before_cancel(monkeypatch, caplog):
    caplog.set_level("INFO", logger="etrade-bot")
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    cancelled = []

    async def cancel(*args, **kwargs):
        cancelled.append((args, kwargs))
        raise AssertionError("cancel ran on a below-entry refusal")

    async def quote(ticker, contract=None):
        return 3.24, 3.34

    def tokens():
        raise AssertionError("token load ran on a below-entry refusal")

    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)

    async def scenario():
        await store.set_position("QQQ", _entry_floor_option())
        for source, min_trigger in (("auto", 3.0), ("repair", 3.0), ("operator", None)):
            with pytest.raises(main_bot.NativeTrailRefused) as exc:
                await main_bot._arm_native_trail(
                    "QQQ", min_trigger=min_trigger, source=source,
                )
            text = str(exc.value)
            assert "native handoff declined: below_entry" in text
            assert "oauth" not in text.lower()
        held = await store.get_position("QQQ")
        assert held["stop_order_id"] == "STOP-1"

    asyncio.run(scenario())
    assert cancelled == []
    assert "native handoff declined: below_entry" in caplog.text


def _inflated_trail_book(stop_price, order_id="1598", offset=7.6, symbol="QQQ"):
    return {
        "OrdersResponse": {
            "Order": {
                "orderId": int(order_id) if str(order_id).isdigit() else order_id,
                "OrderDetail": {
                    "status": "OPEN",
                    "priceType": "TRAILING_STOP_CNST",
                    "stopPrice": stop_price,
                    "offsetValue": offset,
                    "Instrument": {
                        "orderAction": "SELL_CLOSE",
                        "orderedQuantity": 1,
                        "filledQuantity": 0,
                        "Product": {"symbol": symbol, "securityType": "OPTN"},
                    },
                },
            }
        }
    }


def _install_repair_fakes(monkeypatch, book, steps):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    armed = {}

    async def read_offset(order_id):
        return 7.6

    async def cancel(order_id, expected_qty=0):
        steps.append("cancel")
        return main_bot.CANCEL_CONFIRMED

    async def fetch_orders():
        if isinstance(book, Exception):
            raise book
        return book

    async def arm(ticker, offset=None, min_trigger=None, source="operator", allow_below_entry=False, floor_price=None):
        steps.append("place")
        armed["min_trigger"] = min_trigger
        armed["source"] = source
        armed["allow_below_entry"] = allow_below_entry
        return {"ticker": ticker}

    monkeypatch.setattr(main_bot, "_read_resting_trail_offset", read_offset)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)
    monkeypatch.setattr(main_bot, "_arm_native_trail", arm)
    return store, armed


def test_repair_floor_failure_does_not_cancel(monkeypatch, caplog):
    caplog.set_level("WARNING")
    steps = []
    store, armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(2.50), steps,
    )

    async def scenario():
        pos = _entry_floor_option({"native_trail_order_id": "1598", "stop_order_id": None})
        await store.set_position("QQQ", pos)
        # Bid 3.24 trails to about 2.75, under the 3.00 fill.
        preview = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.24, 0.10, "1598", dry_run=True,
        )
        owned = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.24, 0.10, "1598",
        )
        held = await store.get_position("QQQ")
        return preview, owned, held

    preview, owned, held = asyncio.run(scenario())
    assert owned is True
    assert steps == []
    assert armed == {}
    assert held["native_trail_order_id"] == "1598"
    assert preview["dry_run"] is True
    assert preview["floor_check"] == "fail"
    assert preview["floor"] == pytest.approx(3.0)
    assert preview["trigger"] is not None and preview["trigger"] < preview["floor"]
    assert "floor_check=fail" in caplog.text
    assert f"trigger={preview['trigger']}" in caplog.text
    assert f"floor={preview['floor']}" in caplog.text


def test_repair_floor_uncomputable_does_not_cancel(monkeypatch, caplog):
    caplog.set_level("WARNING")
    steps = []
    store, armed = _install_repair_fakes(
        monkeypatch, Exception("order book unreadable"), steps,
    )

    async def scenario():
        pos = _entry_floor_option({"native_trail_order_id": "1598", "stop_order_id": None})
        await store.set_position("QQQ", pos)
        preview = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.50, 0.10, "1598", dry_run=True,
        )
        owned = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.50, 0.10, "1598",
        )
        held = await store.get_position("QQQ")
        return preview, owned, held

    preview, owned, held = asyncio.run(scenario())
    assert owned is True
    assert steps == []
    assert armed == {}
    assert held["native_trail_order_id"] == "1598"
    assert preview["floor_check"] == "uncomputable"
    assert preview["trigger"] is None or preview["floor"] is None
    assert "floor_check=uncomputable" in caplog.text
    assert "order book unreadable" not in caplog.text


def test_repair_floor_pass_cancels_then_places(monkeypatch):
    steps = []
    store, armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(2.50), steps,
    )

    async def scenario():
        pos = _entry_floor_option({"native_trail_order_id": "1598", "stop_order_id": None})
        await store.set_position("QQQ", pos)
        # Bid 3.50, 0.50 offset, trigger lands on the 3.00 fill.
        preview = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.50, 0.10, "1598", dry_run=True,
        )
        owned = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.50, 0.10, "1598",
        )
        return preview, owned

    preview, owned = asyncio.run(scenario())
    assert owned is True
    assert preview["floor_check"] == "pass"
    assert preview["floor"] == pytest.approx(3.0)
    assert preview["trigger"] == pytest.approx(preview["floor"])
    assert preview["trigger"] >= preview["floor"]
    assert steps == ["cancel", "place"]
    assert armed["source"] == "repair"
    assert armed["allow_below_entry"] is False
    # Old stop 2.50, journaled stop 2.40, entry 3.00. The fill wins.
    assert armed["min_trigger"] == pytest.approx(3.0)


def test_repair_googl_shape_replay_leaves_a_tighter_stop(monkeypatch):
    """No saved repair-order fixture. Replay the open-order shape already
    used for the 2026-11-13 GOOGL Stop Val (stopPrice 3.35, offset 7.6)."""
    steps = []
    store, _armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(3.35, symbol="GOOGL"), steps,
    )

    async def scenario():
        pos = _entry_floor_option({
            "native_trail_order_id": "1598",
            "stop_order_id": None,
        })
        await store.set_position("GOOGL", pos)
        low = await main_bot._maybe_repair_inflated_native_trail(
            "GOOGL", pos, 3.24, 0.10, "1598", dry_run=True,
        )
        return low

    low = asyncio.run(scenario())
    assert steps == []
    assert low["floor_check"] == "fail"
    assert low["floor"] == pytest.approx(3.35)
    assert low["trigger"] < low["floor"]


def test_trail_native_refuses_below_entry_without_override(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    cancelled = []

    async def cancel(*args, **kwargs):
        cancelled.append(args)
        raise AssertionError("cancel ran on a below-entry refusal")

    async def quote(ticker, contract=None):
        return 3.24, 3.34

    def tokens():
        return None

    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)

    async def scenario():
        await store.set_position("QQQ", _entry_floor_option())
        with pytest.raises(HTTPException) as refused:
            await main_bot.arm_native_trail({"ticker": "QQQ"}, "s3cret")
        assert refused.value.status_code == 400
        assert "native handoff declined: below_entry" in str(refused.value.detail)

        with pytest.raises(HTTPException) as explicit_false:
            await main_bot.arm_native_trail(
                {"ticker": "QQQ", "allow_below_entry": False}, "s3cret",
            )
        assert "below_entry" in str(explicit_false.value.detail)

        with pytest.raises(HTTPException) as stringy:
            await main_bot.arm_native_trail(
                {"ticker": "QQQ", "allow_below_entry": "true"}, "s3cret",
            )
        assert "below_entry" in str(stringy.value.detail)

        with pytest.raises(HTTPException) as bypass:
            await main_bot.arm_native_trail(
                {"ticker": "QQQ", "allow_below_entry": True}, "s3cret",
            )
        assert bypass.value.status_code == 400
        detail = str(bypass.value.detail)
        assert "below_entry" not in detail
        assert "token" in detail.lower()

    asyncio.run(scenario())
    assert cancelled == []


def _long_premium_option(stop):
    return {
        "action": "BUY",
        "entry_premium": 8.0,
        "stop_premium": stop,
        "stop_order_id": "STOP-LONG",
        "filled_qty": 1,
        "qty": 1,
        "breakeven_locked": True,
        "contract": {
            "right": "CALL",
            "strike": 180.0,
            "expiration": "2026-12-18",
        },
    }


def test_trail_native_long_premium_floors_at_the_resting_stop(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    calls = []

    async def quote(ticker, contract=None):
        return 10.00, 10.10

    async def arm(ticker, offset=None, min_trigger=None, source="operator", allow_below_entry=False, floor_price=None):
        calls.append({
            "min_trigger": min_trigger,
            "allow_below_entry": allow_below_entry,
            "source": source,
        })
        return {"ticker": ticker, "order_id": "N-1"}

    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "_arm_native_trail", arm)

    async def scenario():
        await store.set_position("QQQ", _long_premium_option(6.40))
        preview = await main_bot.arm_native_trail(
            {"ticker": "QQQ", "dry_run": True}, "s3cret",
        )
        armed = await main_bot.arm_native_trail({"ticker": "QQQ"}, "s3cret")
        held = await store.get_position("QQQ")
        return preview, armed, held

    preview, armed, held = asyncio.run(scenario())
    assert preview["dry_run"] is True
    assert preview["floor_check"] == "pass"
    assert set(("trigger", "floor", "floor_check")).issubset(preview)
    # 12.5% of a 10.00 premium, up to the 0.10 grid, is a 1.30 trail.
    # 10.00 - 1.30 = 8.70, above the 6.40 resting stop and the 8.00 fill.
    assert preview["trigger"] == pytest.approx(8.70)
    assert preview["floor"] == pytest.approx(8.00)
    assert preview["trigger"] >= 6.40
    assert preview["floor"] >= 6.40
    assert calls == [{
        "min_trigger": pytest.approx(8.00),
        "allow_below_entry": False,
        "source": "operator",
    }]
    assert armed["status"] == "armed"
    assert held["stop_order_id"] == "STOP-LONG"


def test_trail_native_refuses_when_the_resting_stop_is_tighter(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    cancelled = []

    async def quote(ticker, contract=None):
        return 10.00, 10.10

    async def cancel(*args, **kwargs):
        cancelled.append(args)
        raise AssertionError("cancel ran when the resting stop was tighter")

    def tokens():
        raise AssertionError("token load ran when the resting stop was tighter")

    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)

    async def scenario():
        await store.set_position("QQQ", _long_premium_option(9.20))
        preview = await main_bot.arm_native_trail(
            {"ticker": "QQQ", "dry_run": True}, "s3cret",
        )
        with pytest.raises(HTTPException) as refused:
            await main_bot.arm_native_trail({"ticker": "QQQ"}, "s3cret")
        with pytest.raises(HTTPException) as bypass:
            await main_bot.arm_native_trail(
                {"ticker": "QQQ", "allow_below_entry": True}, "s3cret",
            )
        held = await store.get_position("QQQ")
        return preview, refused.value, bypass.value, held

    preview, refused, bypass, held = asyncio.run(scenario())
    assert preview["floor_check"] == "fail"
    assert preview["trigger"] == pytest.approx(8.70)
    assert preview["floor"] == pytest.approx(9.20)
    assert preview["trigger"] < preview["floor"]
    assert refused.status_code == 400
    assert "would loosen the resting stop" in str(refused.detail)
    assert "below_entry" not in str(refused.detail)
    # allow_below_entry skips the fill, not the resting stop.
    assert bypass.status_code == 400
    assert "would loosen the resting stop" in str(bypass.detail)
    assert cancelled == []
    assert held["stop_order_id"] == "STOP-LONG"


def test_trail_native_floor_uses_the_live_broker_stop(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    async def quote(ticker, contract=None):
        return 10.00, 10.10

    async def read_stop(order_id):
        assert str(order_id) == "STOP-LONG"
        return 9.20

    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "_read_open_order_stop", read_stop)

    async def scenario():
        pos = _long_premium_option(6.40)
        pos["trail"] = 7.10
        await store.set_position("QQQ", pos)
        return await main_bot.arm_native_trail(
            {"ticker": "QQQ", "dry_run": True}, "s3cret",
        )

    preview = asyncio.run(scenario())
    # Saved floor is max(6.40, trail 7.10, entry 8.00) = 8.00.
    # The live stopPrice 9.20 is tighter, so it is the floor.
    assert preview["floor"] == pytest.approx(9.20)
    assert preview["trigger"] == pytest.approx(8.70)
    assert preview["floor_check"] == "fail"
    assert preview["floor"] > 8.00


def test_trail_native_live_read_failure_keeps_the_saved_floor(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    cancelled = []

    async def quote(ticker, contract=None):
        return 10.00, 10.10

    async def read_stop(order_id):
        raise RuntimeError("book unreadable")

    async def cancel(*args, **kwargs):
        cancelled.append(args)
        raise AssertionError("cancel ran after a live-stop read failure")

    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "_read_open_order_stop", read_stop)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)

    async def scenario():
        pos = _long_premium_option(6.40)
        pos["trail"] = 7.10
        await store.set_position("QQQ", pos)
        preview = await main_bot.arm_native_trail(
            {"ticker": "QQQ", "dry_run": True}, "s3cret",
        )
        # Trigger 8.70 clears the saved floor 8.00, so a live call may arm,
        # but the floor it passes must not be under 8.00.
        calls = []

        async def arm(ticker, offset=None, min_trigger=None, source="operator", allow_below_entry=False, floor_price=None):
            calls.append(min_trigger)
            return {"ticker": ticker, "order_id": "N-2"}

        monkeypatch.setattr(main_bot, "_arm_native_trail", arm)
        await main_bot.arm_native_trail({"ticker": "QQQ"}, "s3cret")
        return preview, calls

    preview, calls = asyncio.run(scenario())
    assert preview["floor"] == pytest.approx(8.00)
    assert preview["floor_check"] == "pass"
    assert preview["floor"] >= 8.00
    assert calls == [pytest.approx(8.00)]
    assert cancelled == []


def test_repair_floor_fail_alert_is_not_awaited(monkeypatch):
    steps = []
    store, _armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(2.50), steps,
    )
    release = asyncio.Event()
    started = asyncio.Event()

    async def send_nowait(*args, **kwargs):
        started.set()
        await release.wait()

    async def send(*args, **kwargs):
        raise AssertionError("floor-fail path awaited alerts.send")

    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def scenario():
        pos = _entry_floor_option({"native_trail_order_id": "1598", "stop_order_id": None})
        await store.set_position("QQQ", pos)
        task = asyncio.create_task(main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.24, 0.10, "1598",
        ))
        await asyncio.wait_for(started.wait(), timeout=1)
        done, _pending = await asyncio.wait({task}, timeout=0.2)
        assert task in done
        assert task.result() is True
        release.set()
        pending = [item for item in asyncio.all_tasks() if item is not asyncio.current_task()]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    asyncio.run(scenario())
    assert steps == []


def test_rearm_price_is_never_looser_for_longs_or_shorts():
    long_opt = {
        "action": "BUY",
        "stop_premium": 2.4,
        "trail": 2.8,
        "contract": {"right": "CALL", "strike": 450.0, "expiration": "2026-12-18"},
    }
    assert main_bot._rearm_protection_price(long_opt, floor_price=2.5) == pytest.approx(2.8)
    assert main_bot._rearm_protection_price(long_opt, floor_price=3.1) == pytest.approx(3.1)
    short = {"action": "SELL", "stop": 10.0, "trail": 9.4}
    assert main_bot._rearm_protection_price(short, floor_price=9.8) == pytest.approx(9.4)
    assert main_bot._rearm_protection_price(short, floor_price=9.0) == pytest.approx(9.0)


def test_rearm_uses_the_tight_price_and_one_place(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []
    release = asyncio.Event()
    started = asyncio.Event()

    async def place_opt(ticker, contract, qty, premium):
        placed.append(("opt", qty, premium))
        if not started.is_set():
            started.set()
            await release.wait()
        return {"order_id": f"S-{len(placed)}"}

    async def place_eq(ticker, action, qty, stop):
        placed.append(("eq", qty, stop, action))
        return {"order_id": "E-1"}

    async def fetch_orders():
        if not placed:
            return {"OrdersResponse": {}}
        return {
            "OrdersResponse": {
                "Order": {
                    "orderId": "S-1",
                    "OrderDetail": {
                        "status": "OPEN",
                        "priceType": "STOP",
                        "stopPrice": 3.1,
                        "Instrument": {
                            "orderAction": "SELL_CLOSE",
                            "orderedQuantity": 2,
                            "filledQuantity": 0,
                            "Product": {
                                "symbol": "QQQ",
                                "securityType": "OPTN",
                                "callPut": "CALL",
                                "strikePrice": 450,
                                "expiryYear": 2026,
                                "expiryMonth": 12,
                                "expiryDay": 18,
                            },
                        },
                    },
                }
            }
        }

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "_place_protective_stop", place_eq)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)

    async def scenario():
        pos = _entry_floor_option({"trail": 2.8, "stop_order_id": None, "filled_qty": 2, "qty": 2})
        await store.set_position("QQQ", pos)
        first = asyncio.create_task(main_bot._reconcile_rearm_guard(
            "QQQ", dict(pos), floor_price=3.1,
        ))
        await asyncio.wait_for(started.wait(), timeout=1)
        second = asyncio.create_task(main_bot._reconcile_rearm_guard(
            "QQQ", dict(pos), floor_price=3.1,
        ))
        await asyncio.sleep(0.05)
        release.set()
        await first
        await second
        short = {"action": "SELL", "stop": 10.0, "trail": 9.4, "filled_qty": 4, "qty": 4, "stop_order_id": None}
        await store.set_position("IWM", short)
        await main_bot._reconcile_rearm_guard("IWM", dict(short), floor_price=9.0)

    asyncio.run(scenario())
    assert placed[0] == ("opt", 2, pytest.approx(3.1))
    assert len([row for row in placed if row[0] == "opt"]) == 1
    assert ("eq", 4, pytest.approx(9.0), "SELL") in placed


def test_repair_partial_rearms_only_the_remainder(monkeypatch):
    steps = []
    store, _armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(2.50), steps,
    )
    seen = {}

    async def cancel(order_id, expected_qty=0):
        steps.append("cancel")
        return main_bot.CANCEL_PARTIAL

    async def partial(ticker, order_id, price, reason):
        steps.append("book")
        seen["book_price"] = price
        pos = await store.get_position(ticker)
        pos["filled_qty"] = 1
        pos["qty"] = 1
        pos["stop_order_id"] = None
        pos["native_trail_order_id"] = None
        await store.set_position(ticker, pos)
        return 1

    async def rearm(ticker, pos, floor_price=None):
        steps.append("rearm")
        seen["qty"] = int(pos.get("filled_qty") or 0)
        seen["floor"] = floor_price
        return None

    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_handle_partial_stop_fill", partial)
    monkeypatch.setattr(main_bot, "_reconcile_rearm_guard", rearm)

    async def scenario():
        pos = _entry_floor_option({
            "native_trail_order_id": "1598",
            "stop_order_id": None,
            "filled_qty": 3,
            "qty": 3,
        })
        await store.set_position("QQQ", pos)
        owned = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.50, 0.10, "1598",
        )
        return owned

    assert asyncio.run(scenario()) is True
    assert steps == ["cancel", "book", "rearm"]
    assert seen["qty"] == 1
    assert seen["book_price"] == pytest.approx(2.50)
    assert seen["floor"] == pytest.approx(2.50)


def _dead_stop_option():
    return _entry_floor_option({
        "stop_order_id": "DEAD-1",
        "trail": 2.8,
        "filled_qty": 1,
        "qty": 1,
        "ts": "2020-01-01T15:00:00+00:00",
    })


def _qqq_portfolio(qty=1):
    return {
        "PortfolioResponse": {
            "AccountPortfolio": {
                "Position": [{
                    "quantity": qty,
                    "Product": {
                        "symbol": "QQQ",
                        "securityType": "OPTN",
                        "callPut": "CALL",
                        "strikePrice": 450,
                        "expiryYear": 2026,
                        "expiryMonth": 12,
                        "expiryDay": 18,
                    },
                }],
            }
        }
    }


def _stop_order(
    order_id, status, qty, stop, symbol="QQQ",
    right="CALL", strike=450, expiration="2026-12-18",
):
    year, month, day = str(expiration).split("-")
    return {
        "orderId": order_id,
        "OrderDetail": {
            "status": status,
            "priceType": "STOP",
            "stopPrice": stop,
            "Instrument": {
                "orderAction": "SELL_CLOSE",
                "orderedQuantity": qty,
                "filledQuantity": 0,
                "Product": {
                    "symbol": symbol,
                    "securityType": "OPTN",
                    "callPut": right,
                    "strikePrice": strike,
                    "expiryYear": int(year),
                    "expiryMonth": int(month),
                    "expiryDay": int(day),
                },
            },
        },
    }


def test_dead_stop_id_still_rearms_on_unprotected_surplus_and_trail_failure(monkeypatch):
    """A non-empty stored stop id that is not live must not block a re-arm."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []

    async def place_opt(ticker, contract, qty, premium):
        placed.append(round(float(premium), 2))
        return {"order_id": "NEW-STOP"}

    async def fetch_orders():
        return {"OrdersResponse": {"Order": _stop_order("DEAD-1", "CANCELLED", 1, 2.4)}}

    async def alert(*args, **kwargs):
        return None

    async def cancel(order_id):
        return True

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)

    async def portfolio_one():
        return _qqq_portfolio(1)

    async def unprotected():
        await store.set_position("QQQ", _dead_stop_option())
        report = await main_bot.reconciliation.reconcile_once(
            store,
            portfolio_one,
            fetch_orders,
            cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )
        return report

    report = asyncio.run(unprotected())
    assert placed == [2.8]
    assert any("stop guard re-armed" in item for item in report["healed"])

    async def surplus():
        placed.clear()
        pos = _dead_stop_option()
        pos["filled_qty"] = 2
        pos["qty"] = 2
        await store.set_position("QQQ", pos)

        async def orders():
            return {"OrdersResponse": {"Order": _stop_order("SMALL-1", "OPEN", 1, 2.4)}}

        async def book():
            # After the surplus cancel, the re-arm read sees no covering stop.
            return {"OrdersResponse": {}}

        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", book)

        async def place_and_check(ticker, contract, qty, premium):
            current = await store.get_position(ticker)
            assert not current.get("stop_order_id")
            placed.append(round(float(premium), 2))
            return {"order_id": "NEW-SURPLUS"}

        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_and_check)

        async def portfolio_two():
            return _qqq_portfolio(2)

        report = await main_bot.reconciliation.reconcile_once(
            store,
            portfolio_two,
            orders,
            cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )
        return report

    surplus_report = asyncio.run(surplus())
    assert placed == [2.8]
    assert any("single stop re-armed" in item for item in surplus_report["healed"])

    async def trail_failure():
        placed.clear()
        await store.set_position("QQQ", _dead_stop_option())

        async def quote(ticker, contract=None):
            return 4.00, 4.10

        async def empty_book():
            return {"OrdersResponse": {}}

        async def place_trail(*args, **kwargs):
            raise RuntimeError("trail rejected")

        def tokens():
            return {"oauth_token": "t", "oauth_token_secret": "s"}

        async def account(tokens):
            return "acct"

        async def cancel_order(order_id, expected_qty=0):
            return main_bot.CANCEL_CONFIRMED

        monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", empty_book)
        monkeypatch.setattr(main_bot, "_place_verified_native_trail", place_trail)
        monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
        monkeypatch.setattr(main_bot, "load_tokens", tokens)
        monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
        monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel_order)
        with pytest.raises(Exception, match="protective stop restored"):
            await main_bot._arm_native_trail("QQQ")

    asyncio.run(trail_failure())
    assert placed == [2.8]


def test_live_resting_stop_is_not_placed_twice(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []

    async def place_opt(*args, **kwargs):
        placed.append(args)
        return {"order_id": "SHOULD-NOT"}

    async def fetch_orders():
        return {"OrdersResponse": {"Order": _stop_order("LIVE-1", "OPEN", 1, 2.8)}}

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)

    async def scenario():
        pos = _dead_stop_option()
        await store.set_position("QQQ", pos)
        return await main_bot._reconcile_rearm_guard("QQQ", dict(pos))

    assert asyncio.run(scenario()) == "skipped_live"
    assert placed == []


def test_different_contract_stop_does_not_cover(monkeypatch):
    """A stop on another SPY/QQQ contract is not this position's protection."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []

    async def place_opt(ticker, contract, qty, premium):
        placed.append((ticker, round(float(premium), 2)))
        return {"order_id": "NEW-1"}

    async def alert(*args, **kwargs):
        return None

    async def cancel(order_id):
        return True

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)

    async def arm_against(book_order):
        placed.clear()
        await store.set_position("QQQ", _dead_stop_option())

        async def fetch_orders():
            return {"OrdersResponse": {"Order": book_order}}

        async def quote(ticker, contract=None):
            return 4.00, 4.10

        def tokens():
            return {"oauth_token": "t", "oauth_token_secret": "s"}

        async def account(tokens):
            return "acct"

        async def place_trail(*args, **kwargs):
            raise RuntimeError("trail rejected")

        async def cancel_order(order_id, expected_qty=0):
            return main_bot.CANCEL_CONFIRMED

        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)
        monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
        monkeypatch.setattr(main_bot, "load_tokens", tokens)
        monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
        monkeypatch.setattr(main_bot, "_place_verified_native_trail", place_trail)
        monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel_order)
        with pytest.raises(Exception, match="protective stop restored"):
            await main_bot._arm_native_trail("QQQ")

    async def heal_against(book_order):
        placed.clear()
        await store.set_position("QQQ", _dead_stop_option())

        async def fetch_orders():
            return {"OrdersResponse": {"Order": book_order}}

        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)
        await main_bot.reconciliation.reconcile_once(
            store,
            _portfolio,
            fetch_orders,
            cancel,
            rearm_guard=main_bot._reconcile_rearm_guard,
            alert=alert,
        )

    async def _portfolio():
        return _qqq_portfolio(1)

    async def scenario():
        other_strike = _stop_order("OTHER-1", "OPEN", 1, 2.4, strike=410)
        await arm_against(other_strike)
        assert placed == [("QQQ", 2.8)]
        await heal_against(other_strike)
        assert placed == [("QQQ", 2.8)]
        await arm_against(_stop_order("OTHER-2", "OPEN", 1, 2.4, expiration="2026-12-19"))
        assert placed == [("QQQ", 2.8)]
        await arm_against(_stop_order("OTHER-3", "OPEN", 1, 2.4, right="PUT"))
        assert placed == [("QQQ", 2.8)]

    asyncio.run(scenario())


def test_repair_rearm_is_not_looser_than_the_native_trail(monkeypatch):
    """R3, R3i, and C1f through the repair path, CALL and PUT.

    stop_order_id is None. The native trail rests at 2.50. The restore
    must not come back at the saved 2.40.
    """
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []
    phase = {"cancelled": False}

    async def read_offset(order_id):
        return 7.6

    async def place_opt(ticker, contract, qty, premium):
        placed.append((str(contract.get("right")), round(float(premium), 2)))
        return {"order_id": "RESTORED"}

    async def place_trail(*args, **kwargs):
        raise RuntimeError("trail rejected")

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(tokens):
        return "acct"

    async def cancel_order(order_id, expected_qty=0):
        phase["cancelled"] = True
        return main_bot.CANCEL_CONFIRMED

    monkeypatch.setattr(main_bot, "_read_resting_trail_offset", read_offset)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "_place_verified_native_trail", place_trail)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel_order)

    async def run_case(name, right, trail, floor):
        placed.clear()
        phase["cancelled"] = False
        pos = _entry_floor_option({
            "stop_order_id": None,
            "native_trail_order_id": "1598",
            "trail": trail,
            "contract": {
                "right": right,
                "strike": 450.0,
                "expiration": "2026-12-18",
            },
        })
        await store.set_position("QQQ", pos)

        async def quote(ticker, contract=None):
            return 4.00, 4.10

        async def fetch_orders():
            status = "CANCELLED" if phase["cancelled"] else "OPEN"
            return {"OrdersResponse": {"Order": _stop_order(
                "1598", status, 1, 2.50, right=right,
            )}}

        monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)
        await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 4.00, 0.10, "1598",
        )
        assert placed, name
        assert all(price + 1e-9 >= floor for _right, price in placed), (name, placed)
        assert placed[0][0] == right

    async def scenario():
        for right in ("CALL", "PUT"):
            await run_case(f"R3 {right}", right, None, 2.50)
            await run_case(f"R3i {right}", right, 2.45, 2.50)
            await run_case(f"C1f {right}", right, 2.40, 2.50)

    asyncio.run(scenario())


def test_trail_failure_rearm_is_not_looser_than_the_resting_stop(monkeypatch):
    """R3, R3i, and C1f restore at least 2.50. M10D restores at least 8.60."""
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []
    phase = {"cancelled": False}

    async def place_opt(ticker, contract, qty, premium):
        placed.append(round(float(premium), 2))
        return {"order_id": "RESTORED"}

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(tokens):
        return "acct"

    async def place_trail(*args, **kwargs):
        raise RuntimeError("trail rejected")

    async def cancel_order(order_id, expected_qty=0):
        phase["cancelled"] = True
        return main_bot.CANCEL_CONFIRMED

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_place_verified_native_trail", place_trail)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel_order)

    async def run_case(name, pos, broker_stop, bid, floor):
        placed.clear()
        phase["cancelled"] = False
        pos = dict(pos)
        pos["stop_order_id"] = "REST-1"
        await store.set_position("QQQ", pos)

        async def quote(ticker, contract=None):
            return bid, round(bid + 0.10, 2)

        async def fetch_orders():
            status = "CANCELLED" if phase["cancelled"] else "OPEN"
            return {"OrdersResponse": {"Order": _stop_order(
                "REST-1", status, 1, broker_stop,
                strike=pos["contract"]["strike"],
                expiration=pos["contract"]["expiration"],
                right=pos["contract"]["right"],
            )}}

        monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)
        with pytest.raises(Exception, match="protective stop restored"):
            await main_bot._arm_native_trail("QQQ")
        assert placed, name
        assert placed[-1] + 1e-9 >= floor, name

    async def scenario():
        base = _entry_floor_option()
        # R3: saved 2.40, no trail, broker 2.50.
        r3 = _entry_floor_option({"trail": None})
        await run_case("R3", r3, 2.50, 4.00, 2.50)
        # R3i: trail 2.45 still under the broker 2.50.
        r3i = _entry_floor_option({"trail": 2.45})
        await run_case("R3i", r3i, 2.50, 4.00, 2.50)
        # C1f: trail sits on the saved 2.40; the broker 2.50 still binds.
        c1f = _entry_floor_option({"trail": 2.40})
        await run_case("C1f", c1f, 2.50, 4.00, 2.50)
        # M10D: saved and trail 8.00, broker 8.60.
        m10 = _entry_floor_option({
            "entry_premium": 10.0,
            "stop_premium": 8.0,
            "trail": 8.0,
        })
        await run_case("M10D", m10, 8.60, 12.00, 8.60)

    asyncio.run(scenario())


def test_cancel_requested_stop_does_not_block_rearm(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    placed = []

    async def place_opt(ticker, contract, qty, premium):
        placed.append(round(float(premium), 2))
        return {"order_id": "NEW-1"}

    async def fetch_orders():
        return {"OrdersResponse": {"Order": _stop_order("GONE-1", "CANCEL_REQUESTED", 1, 2.4)}}

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch_orders)

    async def scenario():
        pos = _dead_stop_option()
        await store.set_position("QQQ", pos)
        return await main_bot._reconcile_rearm_guard("QQQ", dict(pos))

    assert asyncio.run(scenario()) == "placed"
    assert placed == [2.8]


def test_rearm_lock_timeout_is_not_success(monkeypatch, caplog):
    caplog.set_level("ERROR")
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    alerts = []

    class Busy:
        async def acquire(self):
            raise main_bot.LockNotAcquired("busy")

        async def release(self):
            return None

    store.position_lock = lambda *args, **kwargs: Busy()

    async def place_opt(*args, **kwargs):
        raise AssertionError("lock timeout must not place")

    async def send_nowait(*args, **kwargs):
        alerts.append(args)

    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_opt)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)

    async def scenario():
        pos = _dead_stop_option()
        await store.set_position("QQQ", pos)
        result = await main_bot._reconcile_rearm_guard("QQQ", dict(pos))
        await asyncio.sleep(0)
        return result

    assert asyncio.run(scenario()) == "lock_timeout"
    assert "re-armed" not in caplog.text
    assert alerts and alerts[0][1] == "rearm_lock_timeout"
    assert alerts[0][0] == "critical"


def test_partial_fill_handler_clears_the_native_trail_id(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)

    async def filled_qty(order_id):
        assert order_id == "TRAIL-1"
        return 1

    async def resolve(order_id, timeout_s=3.0):
        return 2.6

    async def scaled(usd, pct):
        return {"basis": "equity", "pct": pct, "denominator": 100000}

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "_filled_qty_of", filled_qty)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", resolve)
    monkeypatch.setattr(main_bot, "_account_scaled_pct", scaled)
    monkeypatch.setattr(main_bot, "_alert_fill", noop)
    monkeypatch.setattr(main_bot, "settle_heat_reservation", noop)
    monkeypatch.setattr(main_bot.trade_ledger, "record", noop)

    async def scenario():
        pos = _dead_stop_option()
        pos["native_trail_order_id"] = "TRAIL-1"
        pos["native_trail_price_type"] = "TRAILING_STOP_CNST"
        pos["native_trail_field_mode"] = "documented"
        pos["filled_qty"] = 2
        pos["qty"] = 2
        await store.set_position("QQQ", pos)
        remaining = await main_bot._handle_partial_stop_fill(
            "QQQ", "TRAIL-1", 2.5, "native_trail_repair_partial",
        )
        held = await store.get_position("QQQ")
        return remaining, held

    remaining, held = asyncio.run(scenario())
    assert remaining == 1
    assert held["native_trail_order_id"] is None
    assert held["native_trail_field_mode"] is None
    assert held["stop_order_id"] is None
    assert held["filled_qty"] == 1


def test_manual_floor_failed_read_uses_the_highest_known_stop(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")

    async def quote(ticker, contract=None):
        return 10.00, 10.10

    async def read_stop(order_id):
        raise RuntimeError("book unreadable")

    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "_read_open_order_stop", read_stop)

    async def scenario():
        pos = _long_premium_option(6.40)
        pos["trail"] = 7.10
        pos["last_broker_stop_price"] = 9.50
        await store.set_position("QQQ", pos)
        known = await main_bot.arm_native_trail(
            {"ticker": "QQQ", "dry_run": True}, "s3cret",
        )
        bare = _long_premium_option(0)
        bare["stop_premium"] = None
        bare["entry_premium"] = 8.0
        await store.set_position("IWM", bare)
        refused = await main_bot.arm_native_trail(
            {"ticker": "IWM", "dry_run": True}, "s3cret",
        )
        return known, refused

    known, refused = asyncio.run(scenario())
    assert known["floor"] == pytest.approx(9.50)
    assert known["floor"] >= 9.50
    assert refused["floor"] is None
    assert refused["floor_check"] == "uncomputable"


def test_repair_does_not_send_a_floor_at_or_above_the_bid(monkeypatch):
    steps = []
    store, _armed = _install_repair_fakes(
        monkeypatch, _inflated_trail_book(3.50), steps,
    )
    alerts = []

    async def send_nowait(*args, **kwargs):
        alerts.append((args, kwargs))

    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)

    async def scenario():
        pos = _entry_floor_option({"native_trail_order_id": "1598", "stop_order_id": None})
        await store.set_position("QQQ", pos)
        first = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.24, 0.10, "1598",
        )
        second = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 3.24, 0.10, "1598",
        )
        await asyncio.sleep(0)
        return first, second

    first, second = asyncio.run(scenario())
    assert first is True and second is True
    assert steps == []
    assert len(alerts) == 1
    assert alerts[0][0][0] == "error"
    assert alerts[0][0][1] == "native_trail_repair_floor_at_market"
    assert alerts[0][1]["dedupe_seconds"] == 300


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
        "entry_spread": 0.10,
        "entry_spread_pct": 2.85,
        "entry_index_vwap": [
            {"symbol": "SPY", "last": 510.0, "vwap": 505.0, "vs_vwap_pct": 0.9901, "above_vwap": True},
        ],
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
    assert review["entry_spread"] == 0.10
    assert review["entry_index_vwap"][0]["symbol"] == "SPY"
    # Empty position invents nothing.
    empty = main_bot.build_trade_review_fields({})
    assert empty["atr"] is None
    assert empty["signal_id"] is None
    assert empty["instrument"] == "equity"
    assert empty["entry_spread"] is None


def test_native_trail_survives_eod_cancel_before_close():
    """3:40 flatten clears native_trail_order_id before booking — must not
    mislabel the trade as having had no native trail."""
    after_cancel = {
        "entry_premium": 4.0,
        "stop_premium": 3.2,
        "trail": 3.5,
        "native_trail_order_id": None,  # wiped by cancel-before-close
        "had_native_trail": True,
        "native_trail_offset": 0.50,
        "native_trail_source": "auto",
    }
    review = main_bot.build_trade_review_fields(after_cancel)
    assert review["native_trail"] is True
    assert review["native_trail_offset"] == 0.50
    assert review["final_stop"] == 3.5
    # Offset alone (legacy positions without the sticky flag) still counts.
    legacy = {
        "entry_premium": 4.0,
        "native_trail_order_id": None,
        "native_trail_offset": 0.40,
        "native_trail_source": "auto",
    }
    assert main_bot.position_had_native_trail(legacy) is True
    assert main_bot.build_trade_review_fields(legacy)["native_trail"] is True
    assert main_bot.position_had_native_trail({}) is False


def test_observe_water_mark_tracks_worst_drop_mae():
    pos = {
        "entry_premium": 4.00,
        "stop_premium": 3.20,
        "initial_stop": 3.20,
        "contract": {"right": "CALL"},
    }
    # Favourable first.
    up = main_bot.trailing_engine.observe_water_mark(pos, 4.80)
    assert up is not None
    assert up["mfe_percent"] == pytest.approx(0.2, abs=1e-4)
    assert up["mae_percent"] == 0.0
    pos["trail_high"] = up["high_since"]
    pos["trail_low"] = up["low_since"]
    pos["trail_r"] = up["r_multiple"]
    pos["trail_mfe_pct"] = up["mfe_percent"]
    pos["trail_mae_r"] = up["mae_r"]
    pos["trail_mae_pct"] = up["mae_percent"]
    # Then a drawdown under entry — MAE must rise and stay monotonic.
    down = main_bot.trailing_engine.observe_water_mark(pos, 3.60)
    assert down is not None
    assert down["mae_percent"] == pytest.approx(0.1, abs=1e-4)
    assert down["mae_r"] == pytest.approx(0.5, abs=1e-2)
    assert down["mfe_percent"] == pytest.approx(0.2, abs=1e-4)  # peak kept
    pos["trail_high"] = down["high_since"]
    pos["trail_low"] = down["low_since"]
    pos["trail_mae_pct"] = down["mae_percent"]
    pos["trail_mae_r"] = down["mae_r"]
    # A bounce must not erase the trough.
    bounce = main_bot.trailing_engine.observe_water_mark(pos, 4.10)
    assert bounce["mae_percent"] == pytest.approx(0.1, abs=1e-4)


def test_snapshot_index_vs_vwap_and_entry_spread():
    rows = main_bot.snapshot_index_vs_vwap([
        {"symbol": "SPY", "last": 510.0, "vwap": 500.0, "vwap_source": "quote"},
        {"symbol": "QQQ", "last": 400.0, "vwap": 410.0},
        {"symbol": "BAD", "last": 0, "vwap": 10},
    ])
    assert len(rows) == 2
    spy = rows[0]
    assert spy["symbol"] == "SPY"
    assert spy["above_vwap"] is True
    assert spy["vs_vwap_pct"] == pytest.approx(2.0, abs=1e-4)
    assert rows[1]["above_vwap"] is False
    spread = main_bot.snapshot_entry_spread(2.00, 2.10)
    assert spread["spread"] == pytest.approx(0.10)
    assert spread["spread_pct"] == pytest.approx(4.878, abs=1e-2)
    assert main_bot.snapshot_entry_spread(None, 2.10)["spread"] is None


def test_exit_followup_plan_and_recovery_move():
    from datetime import datetime, timezone

    closed = datetime(2026, 11, 21, 20, 0, tzinfo=timezone.utc)
    plans = main_bot.plan_exit_followups(closed)
    assert [p["horizon_min"] for p in plans] == [15, 30]
    assert plans[0]["due_at"].startswith("2026-11-21T20:15:00")
    long_up = main_bot.assess_exit_followup_move(3.00, 3.30, entry_action="BUY")
    assert long_up["recovery"] is True
    assert long_up["move_pct"] == pytest.approx(10.0)
    long_down = main_bot.assess_exit_followup_move(3.00, 2.70, entry_action="BUY")
    assert long_down["recovery"] is False
    short_down = main_bot.assess_exit_followup_move(100.0, 95.0, entry_action="SELL")
    assert short_down["recovery"] is True
    assert main_bot.assess_exit_followup_move(None, 3.0)["mark"] is None


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
    # Unmeasured → fail closed. A missing VWAP is not permission to buy calls.
    unmeasured = main_bot.assess_call_index_vwap_gate("CALL", [
        {"symbol": "SPY", "last": 500.0, "vwap": None},
        {"symbol": "QQQ", "last": 400.0},
    ])
    assert unmeasured is not None and "unmeasured" in unmeasured
    assert main_bot.assess_call_index_vwap_gate("CALL", []) is not None


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
