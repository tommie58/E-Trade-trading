"""PR 2: verified SPY gap pause, VWAP fail-closed, probation cadence,
loss-streak visibility, and the morning burst guard.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import main_bot

ET = ZoneInfo("America/New_York")
# Monday 2026-11-23 cash session. SPY opened 724.33 vs Friday's 689.81 close.
NOV23 = {
    "symbol": "SPY",
    "last": 723.76,
    "open": 724.33,
    "high": 727.98,
    "low": 713.53,
    "previous_close": 689.81,
    "bid": 723.75,
    "ask": 723.77,
    "realtime": True,
    "change_close": 723.76 - 689.81,
    "change_close_pct": (723.76 - 689.81) / 689.81 * 100.0,
}


def _gap_ctx(spy_gap, *, verified=True, structure=True, feed=True, spread=True):
    return main_bot.portfolio_risk.sanitize_risk_context({
        "spy_gap_pct": spy_gap,
        "spy_gap_verified": verified,
        "spy_structure_valid": structure,
        "spy_feed_healthy": feed,
        "spy_spread_healthy": spread,
    })


def _tape(minutes, setup, *, structure=True, feed=True, spread=True):
    return {
        "minutes_since_open": minutes,
        "structure_valid": structure,
        "feeds_healthy": feed,
        "spreads_healthy": spread,
        "setup": setup,
        "stabilize_minutes": 30,
        "settled_weight": 0.5,
    }


def _opened(when, ticker, setup="breakout", right="CALL"):
    return {
        "event": "position_opened",
        "ts": when.isoformat(),
        "data": {
            "ticker": ticker,
            "setup": setup,
            "action": "BUY",
            "contract": {"right": right},
        },
    }


def _closed(when, ticker, pct):
    return {
        "event": "position_closed",
        "ts": when.isoformat(),
        "data": {"ticker": ticker, "realized_pnl_pct": pct},
    }


def _burst(records, when, *, direction=1, setup="breakout"):
    return main_bot.assess_morning_burst(
        records,
        now_epoch=when.timestamp(),
        minute_of_day=when.hour * 60 + when.minute,
        direction=direction,
        setup=setup,
        zone=ET,
    )


# ---------------------------------------------------------------------------
# 1. SPY gap
# ---------------------------------------------------------------------------

def test_nov23_spy_gap_is_a_real_five_percent_open():
    verdict = main_bot.portfolio_risk.verify_opening_gap(
        NOV23["open"], NOV23["previous_close"],
        last=NOV23["last"], high=NOV23["high"], low=NOV23["low"],
        vendor_change=NOV23["change_close"],
        vendor_change_pct=NOV23["change_close_pct"],
        max_trust_pct=main_bot.portfolio_risk.SPY_GAP_MAX_TRUST_PCT,
    )
    assert verdict["verified"] is True
    assert verdict["gap_pct"] == pytest.approx(5.004, abs=1e-3)
    assert f"{verdict['gap_pct']:+.2f}" == "+5.00"
    ctx = main_bot.scanner_mod.build_risk_context(
        {"symbol": "NVDA", "last": 140.0, "open": 141.0, "high": 142.0, "low": 139.0,
         "previous_close": 130.0, "bid": 139.9, "ask": 140.1, "realtime": True},
        {"SPY": NOV23},
    )
    assert ctx["spy_gap_verified"] is True
    assert ctx["spy_gap_pct"] == pytest.approx(5.004, abs=1e-3)
    assert ctx["spy_structure_valid"] is True
    assert ctx["spy_feed_healthy"] is True
    assert ctx["spy_spread_healthy"] is True


def test_bogus_spy_gap_is_not_a_pause():
    # Live print sits outside a prior session's high/low: the open on that
    # bar is not today's cash open.
    stale = {
        "symbol": "SPY", "last": 723.76, "open": 713.56, "high": 718.29, "low": 686.75,
        "previous_close": 680.0, "bid": 723.7, "ask": 723.8, "realtime": True,
        "change_close": 5.0,
    }
    ctx = main_bot.scanner_mod.build_risk_context(
        {"symbol": "NVDA", "last": 100.0, "open": 100.0, "previous_close": 100.0},
        {"SPY": stale},
    )
    assert ctx["spy_gap_pct"] is None
    assert ctx["spy_gap_verified"] is False
    assert ctx["spy_range_pct"] is None
    sizing = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(5.0, verified=False), "calm", _tape(90, "vwap reclaim"),
    )
    assert sizing["pause"] is False
    assert any("unverified" in note for note in sizing["notes"])

    missing = main_bot.portfolio_risk.verify_opening_gap(None, 689.81, max_trust_pct=25)
    assert missing["verified"] is False and missing["gap_pct"] is None
    outside = main_bot.portfolio_risk.verify_opening_gap(
        724.33, 689.81, last=723.76, high=727.98, low=713.53,
        vendor_change=1.0, max_trust_pct=25,
    )
    assert outside["verified"] is False
    absurd = main_bot.portfolio_risk.verify_opening_gap(
        200.0, 100.0, last=200.0, high=201.0, low=199.0, max_trust_pct=25,
    )
    assert absurd["verified"] is False


def test_genuine_gap_sits_out_the_whole_session():
    raw = main_bot.portfolio_risk.gap_pct(724.33, 689.81)
    early = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(raw), "calm", _tape(10, "vwap reclaim"),
    )
    assert early["pause"] is True
    assert early["reasons"][0].startswith("regime pause: SPY opened +5.00%")
    assert "index gap ≥ 2%" in early["reasons"][0]
    assert "full-day sit-out" in early["reasons"][0]

    momentum = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(raw), "calm", _tape(45, "momentum"),
    )
    assert momentum["pause"] is True

    unsettled = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(raw), "calm", _tape(45, "vwap reclaim", structure=False),
    )
    assert unsettled["pause"] is True
    wide = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(raw), "calm", _tape(45, "mean reversion", spread=False),
    )
    assert wide["pause"] is True

    # A settled structure setup used to trade at half size. It sits out.
    settled = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(raw), "calm", _tape(45, "vwap reclaim"),
    )
    assert settled["pause"] is True
    assert settled["weight"] == 0
    assert any("full-day sit-out" in reason for reason in settled["reasons"])
    small = main_bot.portfolio_risk.assess_regime_sizing(
        _gap_ctx(1.9), "calm", _tape(90, "vwap reclaim"),
    )
    assert small["pause"] is False


def test_degraded_feed_keeps_the_gap_pause(monkeypatch):
    monkeypatch.setattr(
        main_bot, "_pro_trader_feed_health",
        lambda: {"ok": False, "http_status": 403, "last_error": "HTTP 403"},
    )
    ctx = _gap_ctx(5.004)
    tape = main_bot._spy_gap_tape({"setup": "orb"}, ctx)
    assert tape["feeds_healthy"] is False
    sizing = main_bot.portfolio_risk.assess_regime_sizing(ctx, "calm", tape)
    assert sizing["pause"] is True
    assert "DEGRADED" not in "".join(sizing["reasons"])


# ---------------------------------------------------------------------------
# 2. CALL index VWAP fails closed
# ---------------------------------------------------------------------------

def test_unmeasured_call_index_vwap_fails_closed():
    reason = main_bot.assess_call_index_vwap_gate("CALL", [
        {"symbol": "SPY", "last": 500.0, "vwap": None},
        {"symbol": "QQQ"},
    ])
    assert reason is not None and "unmeasured" in reason
    assert main_bot.assess_call_index_vwap_gate("CALL", []) is not None
    # One measured index at/above VWAP still allows. Measured-below still blocks.
    assert main_bot.assess_call_index_vwap_gate("CALL", [
        {"symbol": "SPY", "last": 510.0, "vwap": 505.0},
        {"symbol": "QQQ", "last": 400.0},
    ]) is None
    below = main_bot.assess_call_index_vwap_gate("CALL", [
        {"symbol": "SPY", "last": 500.0, "vwap": 505.0},
    ])
    assert below is not None and "below day VWAP" in below
    assert main_bot.assess_call_index_vwap_gate("PUT", []) is None


def test_call_index_vwap_feed_error_fails_closed(monkeypatch):
    async def boom(symbols=None):
        raise RuntimeError("feed down")

    monkeypatch.setattr(main_bot, "_index_vwap_quotes", boom)
    blocked = asyncio.run(main_bot._call_index_vwap_block({
        "instrument": "option", "option_right": "CALL", "action": "BUY",
    }))
    assert blocked is not None and "fail closed" in blocked
    put = asyncio.run(main_bot._call_index_vwap_block({
        "instrument": "option", "option_right": "PUT", "action": "BUY",
    }))
    assert put is None


# ---------------------------------------------------------------------------
# 3. Probation keeps the size cut and the 60s scan
# ---------------------------------------------------------------------------

def test_probation_cuts_size_without_stretching_the_scan():
    now = 1_700_000_000.0
    state = main_bot.learning.learning_state([], {main_bot.learning.BOOK: [now - 60]}, now)
    book = state["cells"][main_bot.learning.BOOK]
    assert book["state"] == "probation"
    assert state["book_weight"] == pytest.approx(0.25)
    assert state["scanner_throttle"] == pytest.approx(1.0)
    verdict = main_bot.learning.entry_verdict(state, "vwap reclaim", "normal")
    assert verdict["allowed"] is True
    assert verdict["weight"] == pytest.approx(0.25)
    qty, _floor = main_bot.learning.apply_weight(4, verdict["weight"])
    assert qty == 1
    delay = int(round(60 * max(1.0, min(main_bot.learning.MAX_THROTTLE, state["scanner_throttle"]))))
    assert delay == 60

    # An ACTIVE book that is down-weighted still looks less often. Probation
    # is the path that must not inherit that stretch.
    rets = [-0.35, -0.2, 0.15, -0.25, 0.05, -0.1, 0.0, -0.3, 0.2, -0.05, 0.1, -0.15, 0.08, -0.12, 0.02]
    trades = [
        {"opened": float(i), "closed": float(i) + 0.5, "ret": ret,
         "setup": "vwap reclaim", "regime": "normal", "basis": "underlying"}
        for i, ret in enumerate(rets, start=1)
    ]
    active = main_bot.learning.learning_state(trades, {}, now)
    assert active["cells"][main_bot.learning.BOOK]["state"] == "active"
    assert active["book_weight"] < 1.0
    assert active["scanner_throttle"] > 1.0


# ---------------------------------------------------------------------------
# 4. Loss streak is visible and the cooldown is unchanged
# ---------------------------------------------------------------------------

def test_loss_streak_status_matches_the_cooldown():
    day = datetime(2026, 11, 23, 10, 0, tzinfo=ET)
    records = [
        _closed(day, "AAA", -2.0),
        _closed(day + timedelta(minutes=10), "BBB", -3.0),
        _closed(day + timedelta(minutes=20), "CCC", -4.0),
    ]
    now = (day + timedelta(minutes=30)).timestamp()
    live = main_bot.assess_loss_streak(records, now_epoch=now, zone=ET)
    status = main_bot.loss_streak_status(records, now_epoch=now, zone=ET)
    assert live["paused"] is True
    assert live["streak"] == 3
    assert "3 losing trades" in live["reason"]
    assert live["minutes_remaining"] == 35
    assert status["enabled"] is True
    assert status["consecutive_losses"] == live["streak"]
    assert status["cooldown_active"] is live["paused"]
    assert status["minutes_remaining"] == live["minutes_remaining"]
    assert status["threshold"] == 3
    assert status["cooldown_minutes"] == 45

    later = (day + timedelta(minutes=20 + 50)).timestamp()
    lapsed = main_bot.assess_loss_streak(records, now_epoch=later, zone=ET)
    shown = main_bot.loss_streak_status(records, now_epoch=later, zone=ET)
    assert lapsed["paused"] is False and lapsed["lapsed"] is True
    assert shown["cooldown_active"] is False
    assert shown["lapsed"] is True

    short = main_bot.assess_loss_streak(records[:2], now_epoch=now, zone=ET)
    assert short["paused"] is False and short["streak"] == 2
    assert main_bot.loss_streak_status(records[:2], now_epoch=now, zone=ET)["consecutive_losses"] == 2


def test_pro_trader_status_surfaces_loss_streak(monkeypatch):
    from datetime import timezone

    day = datetime(2026, 11, 23, 10, 0, tzinfo=ET)
    records = [
        _closed(day, "AAA", -2.0),
        _closed(day + timedelta(minutes=10), "BBB", -1.5),
        _closed(day + timedelta(minutes=20), "CCC", -2.5),
    ]
    now = day + timedelta(minutes=30)
    frozen_utc = datetime(2026, 11, 23, 15, 30, tzinfo=timezone.utc)

    class _FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen_utc.astimezone(tz) if tz is not None else frozen_utc

    monkeypatch.setattr(main_bot, "_corrected_ledger", lambda _n: records)
    monkeypatch.setattr(main_bot, "datetime", _FrozenClock)
    payload = main_bot._pro_trader_status_payload(1)
    streak = payload["loss_streak"]
    assert payload["pacing"]["min_trades_per_day"] == 4
    assert streak["enabled"] is True
    assert streak["consecutive_losses"] == 3
    assert streak["cooldown_active"] is True
    assert streak["minutes_remaining"] == 35
    assert streak["threshold"] == main_bot.MAX_CONSECUTIVE_LOSSES
    assert streak["cooldown_minutes"] == main_bot.STREAK_COOLDOWN_MINUTES
    # The gate the status reports is the same function the entry path calls.
    assert main_bot.assess_loss_streak(records, now_epoch=now.timestamp(), zone=ET)["paused"] is True


# ---------------------------------------------------------------------------
# 5. Morning correlated burst
# ---------------------------------------------------------------------------

def test_morning_burst_stops_a_five_minute_cluster_and_still_allows_four():
    start = datetime(2026, 11, 19, 9, 50, tzinfo=ET)
    records = []
    allowed = 0
    for i in range(7):
        when = start + timedelta(seconds=40 * i)
        reason = _burst(records, when, direction=1, setup="breakout")
        if reason is None:
            allowed += 1
            records.append(_opened(when, f"N{i}", "breakout", "CALL"))
        else:
            assert "morning burst" in reason
    assert allowed == 2

    # A different direction and setup is not this cluster.
    fade = _burst(
        records, start + timedelta(minutes=3), direction=-1, setup="vwap rejection",
    )
    assert fade is None

    # Two of those, then a third long in the same five minutes, still blocked.
    # Afternoon entries are outside the window: the day can still reach 4.
    afternoon = datetime(2026, 11, 19, 11, 5, tzinfo=ET)
    assert _burst(records, afternoon, direction=1, setup="orb") is None
    assert _burst(records, afternoon + timedelta(minutes=20), direction=-1, setup="mean reversion") is None


def test_morning_burst_spaces_rapid_stopouts():
    opened = datetime(2026, 11, 19, 9, 50, tzinfo=ET)
    stopped = datetime(2026, 11, 19, 9, 56, tzinfo=ET)  # 6 minute hold, inside the 8 minute rapid band
    records = [
        _opened(opened, "NVDA", "momentum", "CALL"),
        _closed(stopped, "NVDA", -18.0),
    ]
    soon = stopped + timedelta(minutes=4)
    reason = _burst(records, soon, direction=1, setup="breakout")
    assert reason is not None and "rapid stop-out" in reason
    other = _burst(records, soon, direction=-1, setup="mean reversion")
    assert other is None
    later = stopped + timedelta(minutes=16)
    assert _burst(records, later, direction=1, setup="breakout") is None

    slow_open = datetime(2026, 11, 19, 9, 35, tzinfo=ET)
    slow_close = datetime(2026, 11, 19, 9, 58, tzinfo=ET)  # 23 minutes, not a rapid stop
    slow = [
        _opened(slow_open, "AMD", "breakout", "CALL"),
        _closed(slow_close, "AMD", -6.0),
    ]
    assert _burst(slow, slow_close + timedelta(minutes=2), direction=1, setup="momentum") is None


def test_morning_burst_ignores_a_cancelled_entry_and_can_be_disabled():
    when = datetime(2026, 11, 19, 9, 52, tzinfo=ET)
    records = [
        _opened(when, "TSLA", "breakout", "CALL"),
        {"event": "entry_abandoned", "ts": (when + timedelta(seconds=20)).isoformat(),
         "data": {"ticker": "TSLA"}},
    ]
    assert _burst(records, when + timedelta(minutes=1), direction=1, setup="breakout") is None
    crowd = [_opened(when + timedelta(seconds=i), f"S{i}") for i in range(4)]
    blocked = main_bot.assess_morning_burst(
        crowd, now_epoch=(when + timedelta(minutes=2)).timestamp(),
        minute_of_day=9 * 60 + 54, direction=1, setup="breakout", zone=ET, enabled=False,
    )
    assert blocked is None
