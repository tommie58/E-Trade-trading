"""5.99.7: full-day SPY gap sit-out, no round-up, app budget parity, 5/10 policy."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

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
        ctx = {"weight": 0.25, "notes": [f"{origin} regime cut"]}
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


def _calm_tape():
    return {
        "minutes_since_open": 90,
        "structure_valid": True,
        "feeds_healthy": True,
        "spreads_healthy": True,
        "setup": "vwap rejection",
    }


def test_one_lot_unproven_weight_stays_one_contract():
    decision = main_bot.contract_qty_for_risk_weight(
        1, 0.5, market_weight=1.0, ev_weight=0.5,
    )
    assert decision["skipped"] is False
    assert decision["qty"] == 1
    ctx = {
        "weight": 0.5,
        "ev_weight": 0.5,
        "regime": {"weight": 1.0, "notes": []},
        "notes": ["permitted, not proven → ×0.5 (unproven)"],
    }
    assert main_bot._apply_risk_weight("TSLA", 1, ctx) == 1
    assert "skip_reason" not in ctx


def test_one_lot_regime_cut_skips():
    sizing = main_bot.portfolio_risk.assess_regime_sizing(
        {"spy_gap_pct": 0.4, "spy_gap_verified": True},
        "stressed",
        _calm_tape(),
    )
    assert sizing["pause"] is False
    assert sizing["weight"] == 0.5
    ctx = {
        "weight": round(sizing["weight"] * 0.5, 3),
        "ev_weight": 0.5,
        "regime": sizing,
        "notes": sizing["notes"],
    }
    assert main_bot._apply_risk_weight("TSLA", 1, ctx) == 0
    assert ctx["skip_reason"].startswith("regime cut ")
    assert "not rounded up" in ctx["skip_reason"]
    assert main_bot._risk_weight_skip(ctx) == ctx["skip_reason"]


def test_one_lot_gap_cut_skips():
    sizing = main_bot.portfolio_risk.assess_regime_sizing(
        {"gap_pct": 4.0, "spy_gap_pct": 0.2, "spy_gap_verified": True},
        "calm",
        _calm_tape(),
    )
    assert sizing["pause"] is False
    assert sizing["weight"] == 0.5
    assert any("opening gap" in note for note in sizing["notes"])
    ctx = {
        "weight": sizing["weight"],
        "ev_weight": 1.0,
        "regime": sizing,
        "notes": sizing["notes"],
    }
    assert main_bot._apply_risk_weight("NVDA", 1, ctx) == 0
    assert ctx["skip_reason"].startswith("gap cut ")
    assert "not rounded up" in ctx["skip_reason"]


def test_one_lot_kept_for_unproven_still_refuses_a_cap_breach():
    kept = main_bot.contract_qty_for_risk_weight(1, 0.5, market_weight=1.0, ev_weight=0.5)
    assert kept["qty"] == 1 and kept["skipped"] is False
    over_budget = main_bot.assess_remaining_daily_budget(
        {"remaining_usd": 20, "used_usd": 480, "budget_usd": 500},
        100,
    )
    assert over_budget and "20" in over_budget
    # 0.5 of a $500 day budget is the $250 per-trade ceiling. One contract at
    # $400 of risk does not fit, and neither does $400 against $50 left.
    priced = main_bot.plan_priced_option_risk(
        5.0, kept["qty"], 1.0, remaining_budget_usd=500, base_budget_usd=500,
    )
    assert priced["action"] == "reject"
    left = main_bot.plan_priced_option_risk(
        5.0, kept["qty"], 1.0, remaining_budget_usd=50, base_budget_usd=1000,
    )
    assert left["action"] == "reject"
    assert left["ceiling_source"] == "remaining"
    hard = main_bot.assess_open_risk_ceiling(0, 700, 10_000, main_bot.HARD_RISK_CAP_PCT)
    assert hard and "6%" in hard
    unread = main_bot.plan_risk_budget(10_000, "nope", 0, loss_limit_pct=10)
    assert unread["unreadable"] is True
    assert main_bot.contract_qty_for_risk_weight(
        1, 0.5, market_weight="nope", ev_weight=0.5,
    )["skipped"] is True


def test_three_lot_unproven_weight_is_unchanged():
    decision = main_bot.contract_qty_for_risk_weight(
        3, 0.5, market_weight=1.0, ev_weight=0.5,
    )
    assert decision["skipped"] is False
    assert decision["qty"] == 1
    ctx = {
        "weight": 0.5,
        "ev_weight": 0.5,
        "regime": {"weight": 1.0},
        "notes": ["permitted, not proven → ×0.5 (unproven)"],
    }
    assert main_bot._apply_risk_weight("TSLA", 3, ctx) == 1
    assert "skip_reason" not in ctx


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
    # The unmeasured alert is withheld before 9:30 ET. Pin the clock so the
    # assertion does not depend on when the runner starts.
    monkeypatch.setattr(
        main_bot, "_utcnow",
        lambda: datetime(2026, 11, 25, 14, 31, tzinfo=timezone.utc),
    )
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


def _gap_ctx_from_quote(quote):
    index = main_bot.portfolio_risk.index_gap_context(quote)
    index.pop("bar_consistent", None)
    return main_bot.portfolio_risk.sanitize_risk_context(index)


def _entry_with(origin, ctx):
    payload = _bare(origin)
    payload["risk_context"] = ctx
    return payload


def test_each_rejected_reference_blocks_entries(quiet_gap, monkeypatch):
    """Bar, both vendor mismatches, and the trust cap each block as gap_unmeasured."""
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    good = {
        "open": 660.35, "previous_close": 675.0, "last": 662.0, "high": 664.0, "low": 658.0,
        "change_close": -13.0, "change_close_pct": -1.926, "realtime": True,
    }
    vendor_change = dict(good, change_close=-2.0)
    vendor_percent = dict(good)
    vendor_percent.pop("change_close")
    vendor_percent["change_close_pct"] = -0.3
    trust = {
        "open": 66.035, "previous_close": 675.0, "last": 66.2, "high": 66.4, "low": 65.8,
        "realtime": True,
    }
    session_bar = {
        "open": 724.33, "previous_close": 689.81, "last": 723.76,
        "high": 718.29, "low": 686.75, "realtime": True,
    }
    cases = (
        (session_bar, "session_bar", "session bar"),
        (vendor_change, "vendor_change", "vendor mismatch"),
        (vendor_percent, "vendor_percent", "vendor percent"),
        (trust, "trust_cap", "trust cap"),
    )
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    try:
        for quote, kind, label in cases:
            main_bot.portfolio_risk.clear_spy_gap_sit_out()
            ctx = _gap_ctx_from_quote(quote)
            assert ctx["spy_gap_verified"] is False
            assert ctx["spy_gap_reference_rejected"] is True
            assert ctx["spy_gap_reject_kind"] == kind
            reason = _run(main_bot.entry_gap_sit_out_reason(
                _entry_with(main_bot.DISPATCH_ORIGIN_SCANNER, ctx),
            ))
            assert reason and "entries blocked until verified" in reason
            assert label in reason
            assert main_bot.portfolio_risk.gap_block_code() == "gap_unmeasured"
            app = _run(main_bot.entry_gap_sit_out_reason(
                _entry_with(main_bot.DISPATCH_ORIGIN_APP, ctx),
            ))
            assert app == reason
    finally:
        main_bot.state = saved


def test_nov23_stale_bar_blocks_the_five_percent_open(quiet_gap, monkeypatch):
    """724.33 vs 689.81 with the prior session high still on the bar does not trade."""
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-23")
    # The unmeasured alert is withheld before 9:30 ET. Pin the clock so the
    # assertion does not depend on when the runner starts.
    monkeypatch.setattr(
        main_bot, "_utcnow",
        lambda: datetime(2026, 11, 23, 14, 31, tzinfo=timezone.utc),
    )
    quote = {
        "open": 724.33, "previous_close": 689.81, "last": 723.76,
        "high": 718.29, "low": 686.75, "realtime": True,
    }
    ctx = _gap_ctx_from_quote(quote)
    assert ctx["spy_gap_pct"] is None
    assert ctx["spy_gap_reject_kind"] == "session_bar"
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    sent = []

    async def send(*args, **kwargs):
        sent.append((args[2] if len(args) > 2 else "", kwargs.get("dedupe_key")))

    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def scenario():
        reason = await main_bot.entry_gap_sit_out_reason(
            _entry_with(main_bot.DISPATCH_ORIGIN_SCANNER, ctx),
        )
        await main_bot._alert_spy_gap_sit_out_once(reason or "")
        return reason

    try:
        reason = _run(scenario())
    finally:
        main_bot.state = saved
    assert reason and "session bar" in reason
    assert "entries blocked until verified" in reason
    assert sent == [(
        "SPY gap reference rejected (session bar) — entries blocked until verified",
        "spy_gap_unmeasured:2026-11-23",
    )]


def test_unverified_value_without_a_flag_blocks_and_does_not_measure(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)

    async def scenario():
        flagged = await main_bot.entry_gap_sit_out_reason(
            _entry_with("app", {"spy_gap_pct": -2.6, "spy_gap_verified": False}),
        )
        still = await main_bot.entry_gap_sit_out_reason(_bare("app"))
        unflagged = await main_bot.entry_gap_sit_out_reason(
            _entry_with("app", {"spy_gap_pct": 0.2}),
        )
        after = await main_bot.entry_gap_sit_out_reason(_bare("scanner"))
        return flagged, still, unflagged, after

    try:
        flagged, still, unflagged, after = _run(scenario())
    finally:
        main_bot.state = saved
    assert flagged and "unmeasured" in flagged
    assert still and "unmeasured" in still
    assert unflagged and "unmeasured" in unflagged
    assert after and "unmeasured" in after
    assert main_bot.portfolio_risk.spy_gap_sit_out_view("2026-11-25")["measured"] is False


def test_preopen_request_does_not_consume_the_unmeasured_alert(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    sent = []
    clock = {"at": datetime(2026, 11, 25, 14, 0, tzinfo=timezone.utc)}

    async def send(*args, **kwargs):
        sent.append(kwargs.get("dedupe_key"))

    monkeypatch.setattr(main_bot.alerts, "send", send)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: clock["at"])

    async def scenario():
        early = await main_bot.entry_gap_sit_out_reason(_bare("app"))
        await main_bot._alert_spy_gap_sit_out_once(early or "")
        clock["at"] = datetime(2026, 11, 25, 14, 31, tzinfo=timezone.utc)
        later = await main_bot.entry_gap_sit_out_reason(_bare("scanner"))
        await main_bot._alert_spy_gap_sit_out_once(later or "")
        await main_bot._alert_spy_gap_sit_out_once(later or "")
        return early, later

    try:
        early, later = _run(scenario())
    finally:
        main_bot.state = saved
    assert early and "unmeasured" in early
    assert later and "unmeasured" in later
    assert sent == ["spy_gap_unmeasured:2026-11-25"]


def test_gap_refusal_writes_one_entry_skipped_and_stops(quiet_gap, monkeypatch):
    monkeypatch.setattr(main_bot, "_session_day_et", lambda: "2026-11-25")
    saved = main_bot.state
    main_bot.state = main_bot.StateStore(None)
    rows = []

    async def record(event, data):
        rows.append((event, data))

    async def not_killed():
        return False

    async def send(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    monkeypatch.setattr(main_bot.alerts, "send", send)
    monkeypatch.setattr(main_bot.state, "is_killed", not_killed)

    async def scenario():
        return await main_bot._passes_entry_filters(_bare("app"))

    try:
        ok, reasons = _run(scenario())
    finally:
        main_bot.state = saved
    skipped = [data for event, data in rows if event == "entry_skipped"]
    assert ok is False
    assert len(reasons) == 1 and "unmeasured" in reasons[0]
    assert len(skipped) == 1
    assert skipped[0]["reason"] == "gap_unmeasured"


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
