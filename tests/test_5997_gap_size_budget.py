"""5.99.7: full-day SPY gap sit-out, no round-up, app budget parity, 5/10 policy."""
from __future__ import annotations

import asyncio
import json

import pytest

import main_bot
import pro_trader


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def quiet_gap():
    main_bot.portfolio_risk.clear_spy_gap_sit_out()
    yield
    main_bot.portfolio_risk.clear_spy_gap_sit_out()


def _ctx(gap, verified=True):
    return {
        "ticker": "TSLA",
        "instrument": "option",
        "action": "BUY",
        "option_right": "PUT",
        "risk_context": {
            "spy_gap_pct": gap,
            "spy_gap_verified": verified,
        },
    }


def test_gap_at_least_two_percent_sits_out_scanner_and_app_all_day(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    sent = []

    async def send(*_args, **_kwargs):
        sent.append(_kwargs.get("dedupe_key"))

    async def not_killed():
        return False

    monkeypatch.setattr(main_bot.alerts, "send", send)
    saved_state = main_bot.state
    store = main_bot.StateStore(None)
    monkeypatch.setattr(store, "is_killed", not_killed)
    main_bot.state = store

    for origin in (main_bot.DISPATCH_ORIGIN_SCANNER, main_bot.DISPATCH_ORIGIN_APP):
        payload = _ctx(-2.17)
        payload[main_bot.DISPATCH_ORIGIN_FIELD] = origin
        ok, reasons = _run(main_bot._passes_entry_filters(payload))
        assert ok is False
        assert any("full-day sit-out" in reason for reason in reasons)
        # The same day, after the old 30-minute release, still sits out.
        again = _run(main_bot.entry_gap_sit_out_reason(payload))
        assert again and "full-day sit-out" in again

    # An app payload with no gap of its own still sees the scanner's sit-out.
    app_only = {
        "ticker": "AVGO",
        "instrument": "option",
        "action": "BUY",
        main_bot.DISPATCH_ORIGIN_FIELD: main_bot.DISPATCH_ORIGIN_APP,
    }
    ok, reasons = _run(main_bot._passes_entry_filters(app_only))
    assert ok is False
    assert any("full-day sit-out" in reason for reason in reasons)
    assert sent == ["spy_gap_sit_out:2026-11-25"]

    view = main_bot._pro_trader_status_payload(0)["spy_gap_sit_out"]
    assert view["active"] is True
    assert view["gap_pct"] == pytest.approx(-2.17)
    main_bot.state = saved_state


def test_gap_under_two_percent_does_not_sit_out(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    reason = _run(main_bot.entry_gap_sit_out_reason(_ctx(-1.9)))
    assert reason is None
    sizing = main_bot.portfolio_risk.assess_regime_sizing(
        main_bot.portfolio_risk.sanitize_risk_context({
            "spy_gap_pct": 1.9, "spy_gap_verified": True,
        }),
        "calm",
        {"minutes_since_open": 90, "setup": "vwap rejection"},
    )
    assert sizing["pause"] is False
    view = main_bot.portfolio_risk.spy_gap_sit_out_view("2026-11-25")
    assert view["active"] is False


def test_sit_out_does_not_stop_flatten_or_closes(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    main_bot.portfolio_risk.remember_spy_gap_sit_out(-2.17, True, "2026-11-25")
    flatten = main_bot.plan_daily_loss_flatten(-16.08, 10, False, 1, True)
    assert flatten["fire"] is True
    flat_book = main_bot.plan_daily_loss_flatten(-16.08, 10, False, 0, True)
    assert flat_book["fire"] is False
    assert "nothing is open" in flat_book["reason"]
    close = {
        "ticker": "TSLA",
        "instrument": "option",
        "action": "BUY",
        "intent": "close",
        "risk_context": {"spy_gap_pct": -2.17, "spy_gap_verified": True},
    }
    assert _run(main_bot.entry_gap_sit_out_reason(close)) is None
    assert _run(main_bot.entry_gap_sit_out_reason(close, is_close=True)) is None


def test_risk_weight_under_one_skips_on_scanner_and_app_paths():
    for origin in ("scanner", "app"):
        decision = main_bot.contract_qty_for_risk_weight(1, 0.25)
        assert decision["skipped"] is True
        assert decision["qty"] == 0
        assert "not rounded up" in decision["reason"]
        ctx = {"weight": 0.25, "notes": [f"{origin} unproven"]}
        assert main_bot._apply_risk_weight("TSLA", 1, ctx) == 0
        assert "not rounded up" in ctx["skip_reason"]
        assert main_bot._risk_weight_skip(ctx) == ctx["skip_reason"]

    half_of_two = main_bot.contract_qty_for_risk_weight(2, 0.5)
    assert half_of_two["skipped"] is False
    assert half_of_two["qty"] == 1
    three = main_bot.contract_qty_for_risk_weight(3, 0.5)
    assert three["qty"] == 1
    assert three["skipped"] is False
    four = main_bot.contract_qty_for_risk_weight(4, 0.25)
    assert four["qty"] == 1
    assert four["skipped"] is False
    full = main_bot.contract_qty_for_risk_weight(1, 1.0)
    assert full["qty"] == 1 and full["skipped"] is False


def test_app_budget_and_priced_risk_refuse_and_fail_closed(monkeypatch):
    over_budget = main_bot.assess_remaining_daily_budget(
        {"remaining_usd": 100, "used_usd": 600, "budget_usd": 700},
        1000,
    )
    assert over_budget and "100" in over_budget and "1,000" in over_budget
    inside = main_bot.assess_remaining_daily_budget(
        {"remaining_usd": 200, "used_usd": 100, "budget_usd": 300},
        400,
    )
    assert inside is None
    assert main_bot.assess_remaining_daily_budget(None, 10).startswith("daily risk budget unreadable")
    assert main_bot.assess_remaining_daily_budget(
        {"remaining_usd": "nope", "used_usd": 1, "budget_usd": 1}, 1,
    ).startswith("daily risk budget unreadable")

    unread_pnl = main_bot.assess_app_risk_gates(0, None, 10, 0, 0, 1000, 3, 10)
    assert any("realized P&L unreadable" in reason for reason in unread_pnl)
    readable = main_bot.assess_app_risk_gates(0, -1.0, 10, 0, 10, 1000, 3, 10)
    assert readable == []

    # One contract risks $200 against $50 left. The priced-risk cap refuses.
    priced = main_bot.plan_priced_option_risk(
        5.0, 1, 3.0, remaining_budget_usd=50, base_budget_usd=400,
    )
    assert priced["action"] == "reject"
    assert priced["qty"] == 0
    allowed = main_bot.plan_priced_option_risk(
        2.0, 1, 1.5, remaining_budget_usd=200, base_budget_usd=400,
    )
    assert allowed["action"] == "allow"
    assert allowed["qty"] == 1

    async def unread_budget():
        raise RuntimeError("budget store down")

    monkeypatch.setattr(main_bot, "_day_risk_budget", unread_budget)
    refused = _run(main_bot._priced_option_risk_gate("TSLA", 2.0, 1, 1.5))
    assert refused["action"] == "reject"
    assert "unreadable" in refused["reason"]


def test_policy_defaults_are_five_and_ten_and_persisted_values_clamp():
    assert main_bot.RISK_PER_TRADE_PCT == 5.0
    assert main_bot.DAILY_LOSS_LIMIT_PCT == 10.0
    assert main_bot.POLICY_HARD_MAX_RISK_PCT == 5.0
    assert main_bot.POLICY_HARD_MAX_DAILY_LOSS_PCT == 10.0
    assert pro_trader.risk.PER_TRADE_RISK_CEILING_PCT == 5.0
    assert main_bot.MAX_TRADE_RISK_FRACTION_OF_DAILY == 0.5
    # 0.5 × 10% day = 5% of the account, the same per-trade ceiling.
    assert main_bot.POLICY_HARD_MAX_DAILY_LOSS_PCT * main_bot.MAX_TRADE_RISK_FRACTION_OF_DAILY == 5.0

    current = dict(main_bot._LIVE_POLICY)
    kept = main_bot.plan_policy_sync(current, {
        "daily_loss_limit_pct": 8,
        "risk_per_trade_pct": 4,
    })
    assert kept["policy"]["daily_loss_limit_pct"] == 8
    assert kept["policy"]["risk_per_trade_pct"] == 4

    pushed = main_bot.plan_policy_sync(current, {
        "daily_loss_limit_pct": 20,
        "risk_per_trade_pct": 8,
    })
    assert pushed["policy"]["daily_loss_limit_pct"] == 10
    assert pushed["policy"]["risk_per_trade_pct"] == 5
    assert pushed["accepted"] is True
    assert any("daily_loss_limit_pct" in item and "10" in item for item in pushed["clamped"])
    assert any("risk_per_trade_pct" in item and "5" in item for item in pushed["clamped"])

    low = main_bot.plan_policy_sync(current, {
        "daily_loss_limit_pct": 0.01,
        "risk_per_trade_pct": 0.01,
    })
    assert low["policy"]["daily_loss_limit_pct"] == pytest.approx(0.1)
    assert low["policy"]["risk_per_trade_pct"] == pytest.approx(0.05)


def test_persisted_six_point_one_and_twenty_restore_as_five_and_ten(caplog, monkeypatch):
    saved_policy = dict(main_bot._LIVE_POLICY)
    saved_meta = dict(main_bot._POLICY_META)
    saved_state = main_bot.state
    store = main_bot.StateStore(None)
    stored = dict(saved_policy)
    stored["daily_loss_limit_pct"] = 20
    stored["risk_per_trade_pct"] = 6.1

    async def scenario():
        main_bot.state = store
        main_bot._LIVE_POLICY.clear()
        main_bot._LIVE_POLICY.update(saved_policy)
        await store.set(main_bot.POLICY_STATE_KEY, json.dumps({"policy": stored}))
        with caplog.at_level("WARNING", logger="etrade-bot"):
            await main_bot.restore_policy()
        return (
            main_bot.live_policy("daily_loss_limit_pct"),
            main_bot.live_policy("risk_per_trade_pct"),
        )

    try:
        daily, risk = _run(scenario())
    finally:
        main_bot._LIVE_POLICY.clear()
        main_bot._LIVE_POLICY.update(saved_policy)
        main_bot._POLICY_META.clear()
        main_bot._POLICY_META.update(saved_meta)
        main_bot.state = saved_state

    assert daily == 10
    assert risk == 5
    clamps = [rec.getMessage() for rec in caplog.records if "policy clamped" in rec.getMessage()]
    assert len(clamps) == 1
    assert "20" in clamps[0] and "6.1" in clamps[0]


def test_stale_write_keeps_heat_reservation_id():
    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("TSLA", {
            "qty": 1,
            "entry_premium": 4.2,
            "_heat_reservation_id": "heat-live",
        })
        stale = await store.get_position("TSLA")
        fresh = await store.get_position("TSLA")
        fresh["trail"] = 3.5
        await store.set_position("TSLA", fresh)
        stale["_heat_reservation_id"] = ""
        stale["qty"] = 4
        await store.set_position("TSLA", stale)
        return await store.get_position("TSLA")

    stored = _run(scenario())
    assert stored["_heat_reservation_id"] == "heat-live"
    assert stored["qty"] == 1


def _bare(origin):
    return {
        "ticker": "PLTR",
        "instrument": "option",
        "action": "BUY",
        "option_right": "PUT",
        main_bot.DISPATCH_ORIGIN_FIELD: origin,
    }


def test_sit_out_survives_a_restart_and_ignores_another_date(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    store = main_bot.StateStore(None)
    main_bot.state = store

    async def scenario():
        await main_bot.entry_gap_sit_out_reason(_ctx(2.5))
        stored = await store.get("spy_gap_sit_out:2026-11-25")
        assert stored and json.loads(stored)["active"] is True
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        refused = []
        for origin in (main_bot.DISPATCH_ORIGIN_SCANNER, main_bot.DISPATCH_ORIGIN_APP):
            reason = await main_bot.entry_gap_sit_out_reason(_bare(origin))
            refused.append(reason)
        other = main_bot.StateStore(None)
        main_bot.state = other
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        await other.set(
            "spy_gap_sit_out:2026-11-24",
            json.dumps({"active": True, "gap_pct": 2.5, "reason": "yesterday"}),
            ex=3600,
        )
        allowed = await main_bot.entry_gap_sit_out_reason(_ctx(1.0))
        return refused, allowed

    try:
        refused, allowed = _run(scenario())
    finally:
        main_bot.state = saved
    assert all(reason and "full-day sit-out" in reason for reason in refused)
    assert allowed is None
    assert main_bot.portfolio_risk.spy_gap_sit_out_view("2026-11-25")["active"] is False


def test_unmeasured_gap_blocks_until_a_small_gap_is_measured(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    sent = []

    async def send(*_args, **kwargs):
        sent.append(kwargs.get("dedupe_key"))

    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def scenario():
        reasons = []
        for origin in (main_bot.DISPATCH_ORIGIN_SCANNER, main_bot.DISPATCH_ORIGIN_APP):
            reasons.append(await main_bot.entry_gap_sit_out_reason(_bare(origin)))
        await main_bot._alert_spy_gap_sit_out_once(reasons[0] or "")
        await main_bot._alert_spy_gap_sit_out_once(reasons[0] or "")
        measured = await main_bot.entry_gap_sit_out_reason(_ctx(1.0))
        later = await main_bot.entry_gap_sit_out_reason(_bare(main_bot.DISPATCH_ORIGIN_APP))
        return reasons, measured, later

    try:
        reasons, measured, later = _run(scenario())
    finally:
        main_bot.state = saved
    assert all(reason and "unmeasured" in reason for reason in reasons)
    assert sent == ["spy_gap_unmeasured:2026-11-25"]
    assert measured is None
    assert later is None


def test_rejected_reference_alerts_and_does_not_block(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    sent = []

    async def send(*_args, **kwargs):
        sent.append(kwargs.get("dedupe_key"))

    monkeypatch.setattr(main_bot.alerts, "send", send)
    stale = {
        "symbol": "SPY", "last": 723.76, "open": 713.56, "high": 718.29, "low": 686.75,
        "previous_close": 680.0, "bid": 723.7, "ask": 723.8, "realtime": True,
        "change_close": 5.0,
    }
    quote = main_bot.portfolio_risk.index_gap_context(stale)
    assert quote["spy_gap_verified"] is False
    assert quote["spy_gap_reference_rejected"] is True
    missing = main_bot.portfolio_risk.index_gap_context({"symbol": "SPY", "last": 100.0})
    assert missing["spy_gap_reference_rejected"] is False
    assert missing["spy_gap_pct"] is None

    payload = _bare(main_bot.DISPATCH_ORIGIN_APP)
    payload["risk_context"] = {
        "spy_gap_reference_rejected": True,
        "spy_gap_verified": False,
    }

    async def scenario():
        first = await main_bot.entry_gap_sit_out_reason(payload)
        await main_bot._alert_spy_gap_sit_out_once(first or "")
        scanner = dict(payload)
        scanner[main_bot.DISPATCH_ORIGIN_FIELD] = main_bot.DISPATCH_ORIGIN_SCANNER
        second = await main_bot.entry_gap_sit_out_reason(scanner)
        await main_bot._alert_spy_gap_sit_out_once(second or "")
        return first, second

    try:
        first, second = _run(scenario())
    finally:
        main_bot.state = saved
    assert first is None
    assert second is None
    assert sent == ["spy_gap_reference_rejected:2026-11-25"]


def test_explain_dry_run_does_not_alert_the_sit_out(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    sent = []

    async def send(*_args, **kwargs):
        sent.append(kwargs.get("dedupe_key"))

    async def not_killed():
        return False

    monkeypatch.setattr(main_bot.alerts, "send", send)
    monkeypatch.setattr(main_bot.state, "is_killed", not_killed)

    async def scenario():
        ok, reasons = await main_bot._passes_entry_filters(_bare("app"), dry_run=True)
        return ok, reasons

    try:
        ok, reasons = _run(scenario())
    finally:
        main_bot.state = saved
    assert ok is False
    assert any("unmeasured" in reason for reason in reasons)
    assert sent == []


def test_unreadable_realized_dollars_fail_closed():
    for bad in (None, float("nan"), "nope"):
        budget = main_bot.plan_risk_budget(10_000, bad, 0, loss_limit_pct=10)
        assert budget["unreadable"] is True
        assert "unreadable" in budget["reason"]
        assert main_bot.assess_remaining_daily_budget(budget, 100) == budget["reason"]
    readable = main_bot.plan_risk_budget(10_000, 0, 0, loss_limit_pct=10)
    assert not readable.get("unreadable")
    assert readable["remaining_usd"] == 1000
    lost = main_bot.plan_risk_budget(10_000, -100, 0, loss_limit_pct=10)
    assert lost["used_usd"] == 100


def test_six_percent_hard_cap_allows_one_full_five_percent_position():
    assert main_bot.HARD_RISK_CAP_PCT == 6.0
    assert main_bot.APP_MAX_OPEN_RISK_PCT == 6.0
    assert main_bot._hard_risk_cap_from_env("9") == 6.0
    assert main_bot._hard_risk_cap_from_env("4") == 4.0
    assert main_bot._hard_risk_cap_from_env("nope") == 6.0
    # 5% already open plus another 5% is 10% and does not fit under 6%.
    crowded = main_bot.assess_open_risk_ceiling(500, 500, 10_000, main_bot.HARD_RISK_CAP_PCT)
    assert crowded and "10.00%" in crowded and "6%" in crowded
    first = main_bot.assess_open_risk_ceiling(0, 500, 10_000, main_bot.HARD_RISK_CAP_PCT)
    assert first is None
    smaller = main_bot.assess_open_risk_ceiling(500, 50, 10_000, main_bot.HARD_RISK_CAP_PCT)
    assert smaller is None
