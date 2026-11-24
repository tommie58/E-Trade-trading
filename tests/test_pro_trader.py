"""Pro Day Trader desk: regimes, rotation, pace, rails, shadow, endpoints."""
import pytest
from fastapi.testclient import TestClient

import main_bot
import pro_trader
import pro_trader.playbook
from pro_trader.risk import HARD_RAILS

TRADEABLE = ("TREND_UP", "TREND_DOWN", "CHOP_RANGE", "HIGH_VOL_EXPANSION", "LOW_VOL_PIN")

SETUPS = {
    "TREND_UP": ["breakout", "vwap pullback", "orb", "momentum", "flag", "vwap reclaim"],
    "TREND_DOWN": ["breakout", "vwap pullback", "orb", "momentum", "flag", "rejection"],
    "CHOP_RANGE": ["mean reversion", "vwap fade", "relative strength", "breakout", "momentum", "vwap reclaim"],
    "HIGH_VOL_EXPANSION": ["impulse", "breakout", "momentum", "expansion", "volatility", "impulse"],
    "LOW_VOL_PIN": ["scalp", "quick lock", "rvol scalp", "momentum", "flag", "vwap reclaim"],
}

MINUTES = [10 * 60, 10 * 60 + 50, 12 * 60 + 15, 13 * 60 + 50, 14 * 60 + 20, 15 * 60 + 5]
SCORES = [90, 84, 82, 76, 77, 86]


def _feed(ok=True, http=None, error=None):
    return {
        "provider": "massive",
        "ok": ok,
        "http_status": http,
        "stale": False,
        "last_error": error,
        "endpoint": "/v2/snapshot/locale/us/markets/stocks/tickers",
    }


def _tape(regime, **overrides):
    base = {
        "TREND_UP": {
            "spy_vs_vwap_pct": 0.45, "qqq_vs_vwap_pct": 0.55, "efficiency": 0.72,
            "vol_ratio": 1.1, "prices": [100, 101, 102, 103, 104, 105],
            "opening_range_high": 101, "opening_range_low": 99, "last": 105,
        },
        "TREND_DOWN": {
            "spy_vs_vwap_pct": -0.50, "qqq_vs_vwap_pct": -0.42, "efficiency": 0.70,
            "vol_ratio": 1.1, "prices": [105, 104, 103, 102, 101, 100],
            "opening_range_high": 106, "opening_range_low": 103, "last": 100,
        },
        "CHOP_RANGE": {
            "spy_vs_vwap_pct": 0.04, "qqq_vs_vwap_pct": -0.03, "efficiency": 0.22,
            "vol_ratio": 1.0, "prices": [100, 101, 99.5, 100.4, 99.8, 100.1],
            "opening_range_high": 101, "opening_range_low": 99, "last": 100,
        },
        "HIGH_VOL_EXPANSION": {
            "spy_vs_vwap_pct": 0.30, "qqq_vs_vwap_pct": 0.10, "efficiency": 0.48,
            "vol_ratio": 2.1, "realized_range_pct": 2.1, "expected_range_pct": 1.0,
            "prices": [100, 103, 99, 104, 98, 103],
        },
        "LOW_VOL_PIN": {
            "spy_vs_vwap_pct": 0.05, "qqq_vs_vwap_pct": 0.04, "efficiency": 0.18,
            "vol_ratio": 0.30, "prices": [100, 100.1, 99.95, 100.05, 100.0, 100.08],
            "opening_range_high": 100.4, "opening_range_low": 99.7, "last": 100.05,
        },
    }[regime]
    obs = {"event_active": False, "feed": _feed(), "minute_et": 10 * 60 + 15, **base}
    obs.update(overrides)
    return obs


def _candidate(regime, setup, score, minute, **overrides):
    right = "PUT" if regime == "TREND_DOWN" else "CALL"
    row = {
        "ticker": "NVDA",
        "setup": setup,
        "score": score,
        "rvol": 1.4,
        "option_right": right,
        "action": "BUY",
        "instrument": "option",
        "spread_pct": 4.0,
        "underlying_vs_vwap_pct": -0.2 if regime == "TREND_DOWN" else 0.2,
        "dte": 2,
    }
    row.update(overrides)
    return row


def _run_session(regime, mode="active"):
    desk = pro_trader.Desk()
    decisions = []
    taken = 0
    for minute, setup, score in zip(MINUTES, SETUPS[regime], SCORES):
        decision = desk.evaluate(
            _tape(regime, minute_et=minute),
            _candidate(regime, setup, score, minute),
            mode=mode,
            configured_risk_pct=8.0,
            daily_loss_limit_pct=10.0,
            realized_pnl_today_pct=0.0,
        )
        effect = pro_trader.order_effect(decision, 2)
        decisions.append((decision, effect))
        if mode == "active" and effect["allow"] and decision["verdict"] != "BLOCK":
            desk.note_fill()
            taken += 1
    return desk, decisions, taken


def test_default_mode_is_shadow():
    assert pro_trader.normalize_mode(None) == "shadow"
    assert pro_trader.normalize_mode("nope") == "shadow"
    assert main_bot.PRO_TRADER_MODE == "shadow"
    assert main_bot.live_policy("pro_trader_mode") == "shadow"
    assert main_bot.MIN_TRADES_PER_DAY == 4


def test_each_trading_regime_classifies_and_debounces():
    for regime in TRADEABLE:
        desk_memory = None
        seen, _ = pro_trader.classify(_tape(regime), desk_memory)
        assert seen["regime"] == regime, seen
    # A 403 enters immediately, even off a clean trend.
    memory = {"regime": "TREND_UP", "pending": None, "pending_count": 0}
    degraded, memory = pro_trader.classify(
        _tape("TREND_UP", feed=_feed(ok=False, http=403, error="Massive rejected the API key (HTTP 403)")),
        memory,
    )
    assert degraded["regime"] == "DEGRADED_DATA"
    assert "massive_http_403" in degraded["reason_codes"]
    # One healthy bar does not leave the fail-safe.
    held, memory = pro_trader.classify(_tape("TREND_UP"), memory)
    assert held["regime"] == "DEGRADED_DATA"
    _, memory = pro_trader.classify(_tape("TREND_UP"), memory)
    assert memory["regime"] == "TREND_UP"
    # Trend/chop does not flip on a single bar.
    memory = {"regime": "TREND_UP", "pending": None, "pending_count": 0}
    held, memory = pro_trader.classify(_tape("CHOP_RANGE"), memory)
    assert held["regime"] == "TREND_UP"
    assert memory["pending"] == "CHOP_RANGE"


def test_event_risk_is_immediate():
    classified, _ = pro_trader.classify(_tape("TREND_UP", event_active=True), {"regime": "TREND_UP", "pending": None, "pending_count": 0})
    assert classified["regime"] == "EVENT_RISK"


def test_rotation_names_match_the_desk():
    assert pro_trader.playbook.rotation_ids("TREND_UP") == [
        "trend_continuation", "vwap_pullback", "opening_range_drive",
    ]
    assert "mean_reversion" in pro_trader.playbook.rotation_ids("CHOP_RANGE")
    assert "range_extreme_vwap_fade" in pro_trader.playbook.rotation_ids("CHOP_RANGE")
    assert "rs_leader_breakout" in pro_trader.playbook.rotation_ids("CHOP_RANGE")
    assert pro_trader.playbook.rotation_ids("HIGH_VOL_EXPANSION") == ["volatility_impulse"]
    assert "rvol_scalp" in pro_trader.playbook.rotation_ids("LOW_VOL_PIN")
    assert "quick_lock" in pro_trader.playbook.rotation_ids("LOW_VOL_PIN")


@pytest.mark.parametrize("regime", TRADEABLE)
def test_session_finds_at_least_four_without_breaking_rails(regime):
    before = (
        main_bot.DAILY_LOSS_LIMIT_PCT,
        main_bot.RISK_PER_TRADE_PCT,
        main_bot.trailing_engine.BREAKEVEN_TRIGGER_R,
        main_bot.trailing_engine.EOD_FLATTEN_MINUTE,
        main_bot.APP_MAX_OPEN_RISK_PCT,
    )
    desk, decisions, taken = _run_session(regime, mode="active")
    assert taken >= 4, [(d["playbook"], d["verdict"], d["summary"], d["size_multiplier"]) for d, _ in decisions]
    mix = {d["playbook"] for d, effect in decisions if effect["allow"] and d["verdict"] != "BLOCK"}
    assert len(mix) >= 1
    for decision, effect in decisions:
        assert decision["size_multiplier"] <= 1.0
        assert effect["qty"] <= 2
        if decision["verdict"] != "BLOCK":
            assert 0.35 <= decision["size_multiplier"] <= 1.0
            assert decision["risk"]["effective_risk_pct"] <= 8.0 + 1e-6
            assert decision["risk"]["daily_loss_limit_mutated"] is False
            assert decision["risk"]["daily_loss_limit_pct"] == 10.0
            assert decision["exit_alignment"]["sequence"] == ["STOP", "breakeven_locked", "TRAILING_STOP_CNST"]
            assert decision["exit_alignment"]["flatten_et"] == "15:40"
            assert decision["exit_alignment"]["had_native_trail_sticky"] is True
            assert decision["pacing"]["overrides_hard_rails"] is False
        assert decision["morning_loss_shutoff"] is False
    assert desk.status()["min_trades_per_day"] == 4
    assert before == (
        main_bot.DAILY_LOSS_LIMIT_PCT,
        main_bot.RISK_PER_TRADE_PCT,
        main_bot.trailing_engine.BREAKEVEN_TRIGGER_R,
        main_bot.trailing_engine.EOD_FLATTEN_MINUTE,
        main_bot.APP_MAX_OPEN_RISK_PCT,
    )
    assert main_bot.trailing_engine.EOD_FLATTEN_MINUTE == 15 * 60 + 40
    assert main_bot.position_had_native_trail({"had_native_trail": True}) is True


def test_pacing_admits_bplus_only_when_behind_and_never_lifts_rails():
    behind = pro_trader.pace_state(14 * 60, 0, 4)
    assert behind["behind"] is True
    assert behind["state"] == "behind"
    assert 0.40 <= behind["pace_size_multiplier"] <= 0.75
    on_pace = pro_trader.pace_state(10 * 60, 0, 4)
    assert on_pace["behind"] is False

    early = pro_trader.evaluate(
        _tape("CHOP_RANGE", minute_et=10 * 60),
        _candidate("CHOP_RANGE", "mean reversion", 76, 10 * 60),
        mode="active", trades_completed=0, min_trades=4,
        configured_risk_pct=8, daily_loss_limit_pct=10,
    )
    assert early["verdict"] == "BLOCK"
    assert "below_playbook_score" in early["reason_codes"]

    late = pro_trader.evaluate(
        _tape("CHOP_RANGE", minute_et=14 * 60),
        _candidate("CHOP_RANGE", "mean reversion", 76, 14 * 60),
        mode="active", trades_completed=0, min_trades=4,
        configured_risk_pct=8, daily_loss_limit_pct=10,
    )
    assert late["verdict"] == "REDUCE"
    assert late["pacing"]["applied"] is True
    assert 0.40 <= late["size_multiplier"] <= 0.75
    assert late["playbook"] == "mean_reversion"

    wide = pro_trader.evaluate(
        _tape("CHOP_RANGE", minute_et=14 * 60),
        _candidate("CHOP_RANGE", "mean reversion", 92, 14 * 60, spread_pct=28),
        mode="active", trades_completed=0, min_trades=4, daily_loss_limit_pct=10,
    )
    assert wide["verdict"] == "BLOCK"
    assert "spread_too_wide" in wide["reason_codes"]

    dead = pro_trader.evaluate(
        _tape("TREND_UP", minute_et=14 * 60, feed=_feed(False, 403, "HTTP 403 from api.massive.com/v2/snapshot")),
        _candidate("TREND_UP", "breakout", 95, 14 * 60),
        mode="active", trades_completed=0, min_trades=4, daily_loss_limit_pct=10,
    )
    assert dead["regime"] == "DEGRADED_DATA"
    assert dead["verdict"] == "BLOCK"

    halted = pro_trader.evaluate(
        _tape("LOW_VOL_PIN", minute_et=14 * 60),
        _candidate("LOW_VOL_PIN", "scalp", 95, 14 * 60),
        mode="active", trades_completed=1, min_trades=4,
        daily_loss_limit_pct=10, realized_pnl_today_pct=-10.0,
    )
    assert halted["verdict"] == "BLOCK"
    assert "daily_loss_limit" in halted["reason_codes"]

    event = pro_trader.evaluate(
        _tape("TREND_UP", minute_et=14 * 60, event_active=True),
        _candidate("TREND_UP", "breakout", 95, 14 * 60),
        mode="active", trades_completed=0, min_trades=4,
    )
    assert event["verdict"] == "BLOCK"
    assert event["regime"] == "EVENT_RISK"


def test_shadow_order_effect_matches_baseline_including_blocks():
    baseline_qty = 3
    for regime in TRADEABLE:
        _, decisions, _ = _run_session(regime, mode="shadow")
        for decision, _effect in decisions:
            assert decision["mode"] == "shadow"
            again = pro_trader.order_effect(decision, baseline_qty)
            assert again["allow"] is True
            assert again["qty"] == baseline_qty
            assert again["applied"] is False


def test_size_never_exceeds_configured_or_eight_percent():
    scaled = pro_trader.risk_scale(8.0, 1.5, daily_loss_limit_pct=10)
    assert scaled["size_multiplier"] <= 1.0
    assert scaled["effective_risk_pct"] <= 8.0
    assert scaled["daily_loss_limit_pct"] == 10
    assert scaled["daily_loss_limit_mutated"] is False
    over = pro_trader.risk_scale(10.0, 1.0, daily_loss_limit_pct=10)
    assert over["effective_risk_pct"] <= 8.0
    assert over["size_multiplier"] <= 1.0
    assert HARD_RAILS["engine_may_raise_daily_loss_limit"] is False
    assert HARD_RAILS["engine_may_raise_risk"] is False
    one_lot = pro_trader.order_effect(
        {"mode": "active", "verdict": "REDUCE", "size_multiplier": 0.45, "summary": "cut"},
        1,
    )
    assert one_lot["allow"] is True
    assert one_lot["qty"] == 1
    assert one_lot["floor_bound"] is True


def test_trail_default_is_unchanged_and_override_only_tightens():
    base = main_bot.trailing_engine.compute_trail_level("BUY", 10, 9.2, 10.5, 9.2)
    tight = main_bot.trailing_engine.compute_trail_level(
        "BUY", 10, 9.2, 10.5, 9.2, breakeven_trigger_pct=0.04,
    )
    assert base["breakeven_locked"] is False
    assert base["tier"] == "initial"
    assert tight["breakeven_locked"] is True
    # A looser override is ignored.
    loosened = main_bot.trailing_engine.compute_trail_level(
        "BUY", 10, 9.2, 10.5, 9.2, breakeven_trigger_pct=0.20, breakeven_trigger_r=2.0,
    )
    assert loosened["tier"] == base["tier"]
    assert loosened["breakeven_locked"] == base["breakeven_locked"]
    flat = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600, et_minutes=15 * 60 + 45, weekday="Wed",
        expiry="2026-11-20", today="2026-11-18", flatten_all=True,
    )
    assert flat["fire"] is True
    assert flat["reason"] == "eod_flatten"


def test_corrective_actions_rank_a_short_day_and_a_403():
    pacing = pro_trader.pace_state(15 * 60, 1, 4)
    actions = pro_trader.build_corrective_actions(
        [{"verdict": "BLOCK", "regime": "DEGRADED_DATA", "reason_codes": ["massive_http_403"], "playbook": "feed_failsafe"}],
        [],
        pacing=pacing,
        feed={"ok": False, "http_status": 403, "last_error": "HTTP 403"},
        min_trades=4,
    )
    ids = [item["id"] for item in actions]
    assert ids[0] == "feed-health-failure"
    assert "trades-below-min-per-day" in ids
    assert actions[0]["dispatch"]["repo"] == "tommie58/E-Trade-trading"
    assert actions[0]["dispatch"]["action"] == "cursor_corrective_pr"
    assert "TRAILING_STOP_CNST promotion" in actions[0]["proposal"]["do_not_touch"]
    report = pro_trader.build_report(
        [{"verdict": "BLOCK", "regime": "DEGRADED_DATA", "playbook": "feed_failsafe",
          "reason_codes": ["massive_http_403"], "mode": "shadow", "accepted_by_filters": True}],
        pacing=pacing, feed={"ok": False, "http_status": 403, "last_error": "403"}, min_trades=4,
    )
    assert report["min_trades_per_day"] == 4
    assert report["trades_completed"] == 1
    assert report["shadow_counterfactual_trades"] == 1
    assert report["corrective_actions"]


def test_policy_accepts_mode_and_trade_target():
    current = dict(main_bot._LIVE_POLICY)
    kept = main_bot.plan_policy_sync(current, {"pro_trader_mode": "yolo", "min_trades_per_day": 99})
    assert kept["policy"]["pro_trader_mode"] == current["pro_trader_mode"]
    assert any("pro_trader_mode" in item for item in kept["ignored"])
    assert kept["policy"]["min_trades_per_day"] == 12
    assert kept["clamped"]
    flipped = main_bot.plan_policy_sync(current, {"pro_trader_mode": "active", "min_trades_per_day": 4})
    assert flipped["policy"]["pro_trader_mode"] == "active"
    assert flipped["policy"]["min_trades_per_day"] == 4
    assert flipped["policy"]["risk_per_trade_pct"] == current["risk_per_trade_pct"]
    assert flipped["policy"]["daily_loss_limit_pct"] == current["daily_loss_limit_pct"]


def test_pro_trader_endpoints_use_the_webhook_secret(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    with TestClient(main_bot.app) as client:
        denied = client.get("/pro-trader/status")
        assert denied.status_code == 401
        headers = {"X-Rork-Secret": "s3cret"}
        status = client.get("/pro-trader/status", headers=headers)
        report = client.get("/pro-trader/report", headers=headers)
        actions = client.get("/pro-trader/corrective-actions", headers=headers)
    assert status.status_code == 200
    body = status.json()
    assert body["mode"] == "shadow"
    assert body["min_trades_per_day"] == 4
    assert body["morning_loss_shutoff"] is False
    assert "pacing" in body
    assert body["pacing"]["min_trades_per_day"] == 4
    assert body["hard_rails"]["max_size_multiplier"] == 1.0
    assert body["hard_rails"]["flatten_et"] == "15:40"
    assert report.status_code == 200
    payload = report.json()
    assert "corrective_actions" in payload
    assert "playbook_mix" in payload
    assert "counterfactual" in payload
    assert actions.status_code == 200
    assert "corrective_actions" in actions.json()


def test_shadow_filter_does_not_add_a_block(monkeypatch):
    async def observation(_p):
        return _tape("CHOP_RANGE", minute_et=12 * 60)

    monkeypatch.setattr(main_bot, "_pro_trader_observation", observation)
    monkeypatch.setattr(main_bot, "live_policy", lambda key: {
        "pro_trader_mode": "shadow",
        "risk_per_trade_pct": 8.0,
        "daily_loss_limit_pct": 10.0,
        "min_trades_per_day": 4,
    }.get(key))

    async def run():
        payload = {
            "ticker": "NVDA", "instrument": "option", "setup": "mean reversion",
            "score": 70, "rvol": 0.4, "option_right": "CALL", "action": "BUY",
        }
        blocked = await main_bot._fold_pro_trader(payload, [])
        assert blocked == []
        assert payload["_pro_trader"]["mode"] == "shadow"
        qty, reason = main_bot._apply_pro_trader_size("NVDA", 2, payload)
        assert qty == 2 and reason is None

    import asyncio
    asyncio.run(run())
