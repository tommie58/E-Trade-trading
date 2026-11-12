"""Day-trade book: EOD flatten-all, short DTE window, learning default."""
from datetime import date, timedelta

import main_bot


def test_day_trade_mode_defaults_on():
    assert main_bot.DAY_TRADE_MODE is True
    assert main_bot.LEARNING_ENABLED is True


def test_expression_window_is_same_week():
    assert main_bot.core_book.MIN_DTE == 0
    assert main_bot.core_book.MAX_DTE == 7
    assert main_bot.core_book.TARGET_DTE == 0
    assert main_bot.core_book.HOLD_DAYS == 0.25


def test_eod_flatten_all_closes_a_weekly(monkeypatch):
    # 15:45 ET, a Friday weekly still open — day-trade mode must flatten it.
    today = "2026-11-11"
    weekly = "2026-11-13"
    flat = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=1800,
        et_minutes=15 * 60 + 45,
        weekday="Wed",
        expiry=weekly,
        today=today,
        flatten_all=True,
    )
    assert flat["fire"] is True
    assert flat["reason"] == "eod_flatten"
    assert "day-trade" in flat["detail"]

    overnight = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=1800,
        et_minutes=15 * 60 + 45,
        weekday="Wed",
        expiry=weekly,
        today=today,
        flatten_all=False,
    )
    assert overnight["fire"] is False


def test_eod_flatten_still_closes_0dte_in_overnight_mode():
    today = "2026-11-11"
    out = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600,
        et_minutes=15 * 60 + 45,
        weekday="Wed",
        expiry=today,
        today=today,
        flatten_all=False,
    )
    assert out["fire"] is True
    assert out["reason"] == "eod_flatten"


def test_weekly_candidates_include_0dte_before_cutoff():
    today = date(2026, 11, 11)  # Wednesday
    listed = [today, today + timedelta(days=2), today + timedelta(days=9)]
    before = main_bot.core_book.weekly_candidates(listed, today, now_minutes_et=11 * 60)
    assert today in before
    after = main_bot.core_book.weekly_candidates(listed, today, now_minutes_et=15 * 60)
    assert today not in after


def test_pick_best_expiry_can_choose_0dte_when_cheapest():
    today = date(2026, 11, 11)
    # Same spread/Δ band; lower |θ| on 0DTE would be unrealistic, so give 0DTE
    # a tighter spread so same-session HOLD_DAYS makes it win.
    rows = [
        {
            "expiry": today.isoformat(),
            "strike": 100.0,
            "bid": 2.00,
            "ask": 2.05,
            "theta": -0.40,
            "delta": 0.60,
            "open_interest": 500,
        },
        {
            "expiry": (today + timedelta(days=2)).isoformat(),
            "strike": 100.0,
            "bid": 3.00,
            "ask": 3.20,
            "theta": -0.15,
            "delta": 0.60,
            "open_interest": 500,
        },
    ]
    best = main_bot.core_book.pick_best_expiry(rows, today, now_minutes_et=11 * 60)
    assert best is not None
    assert best["dte"] == 0
    assert best["expiry"] == today.isoformat()


def test_unmeasured_target_expiry_avoids_0dte():
    today = date(2026, 11, 11)
    assert main_bot.core_book.target_expiry(today) == today + timedelta(days=1)
    listed = [today, today + timedelta(days=1), today + timedelta(days=4)]
    picked = main_bot.core_book.pick_expiry(listed, today)
    assert picked == today + timedelta(days=1)
