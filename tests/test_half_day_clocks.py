"""Half-day session clocks follow today's real NYSE close.

Frozen America/New_York datetimes. Full-day numbers stay the historical
constants. The one full-day live change is the 15:40 ET entry block.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot

ET = ZoneInfo("America/New_York")
HALF_DAYS = (datetime(2026, 11, 27, 12, 0, tzinfo=ET), datetime(2026, 12, 24, 12, 0, tzinfo=ET))
FULL_DAYS = (datetime(2026, 11, 24, 12, 0, tzinfo=ET), datetime(2026, 11, 25, 12, 0, tzinfo=ET))


def _at(day: datetime, hour: int, minute: int, second: int = 0) -> datetime:
    return day.replace(hour=hour, minute=minute, second=second, microsecond=0)


def _freeze(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when.astimezone(timezone.utc))


def _option(ticker: str = "SPY") -> dict:
    return {
        ticker: {
            "qty": 1,
            "filled_qty": 1,
            "contract": {"right": "CALL", "expiration": "2026-11-27", "strike": 500},
        }
    }


def _tape_samples(day: datetime, minutes):
    out = []
    for i, minute in enumerate(minutes):
        ts = _at(day, minute // 60, minute % 60)
        out.append({"ts": ts.isoformat(), "last": 100.0 + i, "total_volume": 1000 * (i + 1)})
    return out


def _feats(minutes: float) -> dict:
    return {"rvol": 2.5, "rs_spy_pct": 1.0, "session_minutes": minutes}


def test_nov_25_is_a_full_session_and_not_on_either_calendar():
    assert "2026-11-25" not in main_bot._MARKET_HALF_DAYS
    assert "2026-11-25" not in main_bot._MARKET_HOLIDAYS
    assert main_bot._session_close_minute(FULL_DAYS[1]) == 16 * 60


@pytest.mark.parametrize("day", HALF_DAYS, ids=("2026-11-27", "2026-12-24"))
def test_half_day_close_flatten_and_bell(day, monkeypatch):
    assert main_bot._session_close_minute(day) == 13 * 60
    close = main_bot._session_close_minute(day)
    weekday = day.strftime("%a")
    today = day.date().isoformat()

    def planned(hour, minute):
        return main_bot.trailing_engine.plan_scheduled_exit(
            held_seconds=600,
            et_minutes=hour * 60 + minute,
            weekday=weekday,
            expiry=today,
            today=today,
            flatten_all=True,
            close_minute=close,
        )

    assert planned(12, 40)["fire"] is True
    assert planned(12, 40)["reason"] == "eod_flatten"
    assert planned(12, 39)["fire"] is False
    assert planned(13, 0)["fire"] is False

    _freeze(monkeypatch, _at(day, 13, 0, 1))
    assert main_bot._is_market_open() is False
    _freeze(monkeypatch, _at(day, 12, 59, 0))
    assert main_bot._is_market_open() is True


@pytest.mark.parametrize("day", HALF_DAYS, ids=("2026-11-27", "2026-12-24"))
def test_half_day_0dte_tape_and_entry_gate(day, monkeypatch):
    cutoff = main_bot.core_book.zero_dte_cutoff_for(day=day.date())
    assert cutoff == 11 * 60 + 50
    assert main_bot.core_book.same_day_entry_open(11 * 60 + 49, day=day.date()) is True
    assert main_bot.core_book.same_day_entry_open(11 * 60 + 50, day=day.date()) is False

    samples = _tape_samples(day, [10 * 60, 10 * 60 + 15, 10 * 60 + 30, 11 * 60])
    quote = {"last": 101.0, "open": 100.0, "previous_close": 100.0}
    late = main_bot.scanner_mod.read_tape(samples, quote, _at(day, 12, 30))
    assert late["measured"] is True
    assert late["plan"] is None
    assert any("no new entries" in note for note in late["watching"])
    earlier = main_bot.scanner_mod.read_tape(samples, quote, _at(day, 12, 29))
    assert not any("no new entries" in note for note in earlier["watching"])

    monkeypatch.setattr(main_bot, "FORCE_EXECUTE_ALLOWED", True)
    forced = {"force_execute": True}
    assert main_bot._entry_session_block(_at(day, 12, 39), "option", False, forced) is None
    for minute in (40, 59):
        reason = main_bot._entry_session_block(_at(day, 12, minute), "option", False, forced)
        assert reason == main_bot.FLATTEN_WINDOW_ENTRY_REASON
        assert main_bot._entry_session_block(_at(day, 12, minute), "equity", False, forced) == reason
    # The flatten window does not stop an exit.
    assert main_bot._entry_session_block(_at(day, 12, 40), "option", True, forced) is None


def test_force_execute_does_not_bypass_the_half_day_flatten_window(monkeypatch):
    monkeypatch.setattr(main_bot, "FORCE_EXECUTE_ALLOWED", True)
    when = datetime(2026, 11, 27, 12, 40, tzinfo=ET)
    payload = {"force_execute": True}
    assert main_bot._force_execute(payload) is True
    assert main_bot._entry_session_block(when, "option", False, payload) == (
        "flatten window open — entry refused"
    )
    assert main_bot._entry_session_block(when, "equity", False, payload) == (
        "flatten window open — entry refused"
    )


def test_crypto_is_exempt_only_from_the_new_flatten_entry_block(monkeypatch):
    monkeypatch.setattr(main_bot, "FORCE_EXECUTE_ALLOWED", False)
    half = datetime(2026, 11, 27, 12, 40, tzinfo=ET)
    assert main_bot._entry_session_block(half, "crypto", False, {}) is None
    assert main_bot._entry_session_block(half, "option", False, {}) == (
        "flatten window open — entry refused"
    )
    # After the bell, crypto's existing 24/7 exemption is unchanged.
    after = datetime(2026, 11, 27, 13, 5, tzinfo=ET)
    assert main_bot._entry_session_block(after, "crypto", False, {}) is None
    assert main_bot._entry_session_block(after, "option", False, {}) == (
        "market closed — entry refused (exchange-aware calendar)"
    )


def test_thanksgiving_refuses_entries_all_day():
    holiday = datetime(2026, 11, 26, 12, 0, tzinfo=ET)
    assert holiday.date().isoformat() in main_bot._MARKET_HOLIDAYS
    for hour, minute in ((9, 30), (12, 0), (15, 45)):
        when = _at(holiday, hour, minute)
        assert main_bot._is_market_open_at(when) is False
        reason = main_bot._entry_session_block(when, "option", False, {})
        assert reason == "market closed — entry refused (exchange-aware calendar)"


@pytest.mark.parametrize("day", FULL_DAYS, ids=("2026-11-24", "2026-11-25"))
def test_full_day_clocks_stay_put_except_the_1540_entry_block(day):
    assert main_bot._session_close_minute(day) == 16 * 60
    close = 16 * 60
    assert main_bot.core_book.zero_dte_cutoff_for(day=day.date()) == 14 * 60 + 30
    assert main_bot.core_book.ZERO_DTE_ENTRY_CUTOFF_ET_MIN == 14 * 60 + 30
    assert main_bot.core_book.same_day_entry_open(14 * 60 + 29, day=day.date()) is True
    assert main_bot.core_book.same_day_entry_open(14 * 60 + 30, day=day.date()) is False
    # Minutes alone, no date: still the full-day constant.
    assert main_bot.core_book.same_day_entry_open(14 * 60 + 29) is True
    assert main_bot.core_book.same_day_entry_open(14 * 60 + 30) is False

    assert main_bot.scanner_mod.tape_no_new_entry_minute(day) == 15 * 60 + 30
    assert main_bot.scanner_mod.session_close_minute(day) == close
    assert main_bot.edge.information_runway_limit(day) == 330.0

    flat = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600,
        et_minutes=15 * 60 + 40,
        weekday=day.strftime("%a"),
        expiry=day.date().isoformat(),
        today=day.date().isoformat(),
        flatten_all=True,
        close_minute=close,
    )
    assert flat["fire"] is True
    before = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600,
        et_minutes=15 * 60 + 39,
        weekday=day.strftime("%a"),
        today=day.date().isoformat(),
        flatten_all=True,
        close_minute=close,
    )
    assert before["fire"] is False

    assert main_bot._entry_session_block(_at(day, 15, 39), "option", False, {}) is None
    assert main_bot._entry_session_block(_at(day, 15, 40), "option", False, {}) == (
        "flatten window open — entry refused"
    )
    assert main_bot._entry_session_block(_at(day, 15, 59), "equity", False, {}) == (
        "flatten window open — entry refused"
    )


def test_full_day_tape_still_refuses_at_1530():
    day = FULL_DAYS[0]
    samples = _tape_samples(day, [10 * 60, 10 * 60 + 15, 11 * 60, 14 * 60])
    quote = {"last": 101.0, "open": 100.0, "previous_close": 100.0}
    late = main_bot.scanner_mod.read_tape(samples, quote, _at(day, 15, 30))
    assert late["plan"] is None
    assert any("no new entries" in note for note in late["watching"])
    earlier = main_bot.scanner_mod.read_tape(samples, quote, _at(day, 15, 29))
    assert not any("no new entries" in note for note in earlier["watching"])


def test_information_diffusion_runway_scales_and_full_day_stays_330():
    stored = next(
        c for c in main_bot.edge.THESES["information_diffusion"]["conditions"]
        if c["feature"] == "session_minutes"
    )
    assert stored["value"] == 330.0
    full = FULL_DAYS[0]
    half = HALF_DAYS[0]
    assert main_bot.edge.information_runway_limit(full) == 330.0
    assert main_bot.edge.information_runway_limit(half) == 150.0

    full_ok = main_bot.edge.evaluate_thesis("momentum", "long", _feats(330.0), now_et=full)
    full_late = main_bot.edge.evaluate_thesis("momentum", "long", _feats(330.1), now_et=full)
    assert full_ok["ok"] is True
    assert full_late["ok"] is False
    assert any("session_minutes" in item for item in full_late["unmet"])

    half_ok = main_bot.edge.evaluate_thesis("momentum", "long", _feats(150.0), now_et=half)
    half_late = main_bot.edge.evaluate_thesis("momentum", "long", _feats(150.1), now_et=half)
    assert half_ok["ok"] is True
    assert half_late["ok"] is False
    assert any("330" not in item and "150" in item for item in half_late["unmet"])


def test_flatten_retry_is_bounded_idempotent_and_exits_only():
    half = datetime(2026, 11, 27, 13, 5, tzinfo=ET)
    book = _option()
    sent = []
    for _ in range(2):
        due = main_bot.plan_flatten_retry_tick(book, half)
        for ticker in due:
            claimed = main_bot.flatten_retry_claim(book[ticker])
            assert claimed is not None
            assert "action" not in claimed or claimed.get("action") != "BUY"
            book[ticker] = claimed
            sent.append(ticker)
    assert sent == ["SPY"]
    assert main_bot.plan_flatten_retry_tick(book, half) == []

    working = _option()
    working["SPY"]["close_order_id"] = "working-close"
    assert main_bot.plan_flatten_retry_tick(working, half) == []
    pending = _option()
    pending["SPY"]["pending_close"] = True
    assert main_bot.plan_flatten_retry_tick(pending, half) == []

    too_late = datetime(2026, 11, 27, 13, 16, tzinfo=ET)
    assert main_bot.flatten_retry_window(too_late) is False
    assert main_bot.plan_flatten_retry_tick(_option(), too_late) == []
    assert main_bot.flatten_retry_window(datetime(2026, 11, 27, 13, 15, tzinfo=ET)) is True

    # A normal entry is refused after the bell. The retry list is positions
    # already tracked, not a new order.
    assert main_bot._entry_session_block(half, "option", False, {}) == (
        "market closed — entry refused (exchange-aware calendar)"
    )
    assert main_bot._entry_session_block(half, "option", True, {}) is None


def test_display_clocks_follow_the_close_and_full_day_labels_stay():
    from pro_trader.pacing import pace_state
    from pro_trader.regime import session_phase
    from pro_trader.risk import FLATTEN_ET, flatten_et_label

    assert flatten_et_label(None) == "15:40"
    assert flatten_et_label(16 * 60) == FLATTEN_ET == "15:40"
    assert flatten_et_label(13 * 60) == "12:40"
    assert session_phase(13 * 60, 13 * 60) == "closed"
    assert session_phase(12 * 60 + 59, 13 * 60) != "closed"
    assert session_phase(15 * 60) == "late_decay"
    assert session_phase(15 * 60, 16 * 60) == session_phase(15 * 60)
    assert session_phase(16 * 60) == "closed"
    assert pace_state(14 * 60, 0, 4) == pace_state(14 * 60, 0, 4, close_minute=16 * 60)
