"""Half-day session clocks follow today's real NYSE close.

Frozen America/New_York datetimes. Full-day numbers stay the historical
constants. The one full-day live change is the 15:40 ET entry block.
"""
import asyncio
import json
import time
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


def _open_option(when: datetime, stop_id: str = "resting-stop-1") -> dict:
    return {
        "qty": 1,
        "filled_qty": 1,
        "stop_order_id": stop_id,
        "ts": when.astimezone(timezone.utc).isoformat(),
        "action": "BUY",
        "contract": {
            "right": "CALL",
            "expiration": when.date().isoformat(),
            "strike": 500,
        },
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


class _Book:
    """In-memory positions. A trail lock or a position write after the bell fails the test."""

    def __init__(self, ticker: str, pos: dict, calls: dict):
        self.pos = dict(pos)
        self.ticker = ticker
        self.calls = calls

    async def set(self, *_args, **_kwargs):
        return None

    async def all_positions(self):
        return {self.ticker: dict(self.pos)}

    async def get_position(self, ticker):
        return dict(self.pos) if ticker == self.ticker else None

    async def set_position(self, ticker, pos):
        self.calls["writes"].append((ticker, dict(pos)))
        self.pos = dict(pos)

    def lock(self, *_args, **_kwargs):
        self.calls["lock"].append(True)
        raise AssertionError("trail lock acquired after the bell")


def _record(calls, name):
    async def _fn(*args, **kwargs):
        calls[name].append((args, kwargs))
        raise AssertionError(f"{name} called after the bell")

    return _fn


@pytest.mark.parametrize(
    "when",
    (
        datetime(2026, 11, 27, 13, 0, 1, tzinfo=ET),
        datetime(2026, 11, 24, 16, 0, 1, tzinfo=ET),
    ),
    ids=("2026-11-27 13:00:01", "2026-11-24 16:00:01"),
)
def test_after_the_bell_an_open_position_keeps_its_stop(when, monkeypatch):
    """No flatten, no stop cancel, and no order once the session is over."""
    stop_id = "resting-stop-1"
    _freeze(monkeypatch, when)
    assert main_bot._is_market_open() is False
    close = main_bot._session_close_minute(when)
    minute = when.hour * 60 + when.minute
    verdict = main_bot.trailing_engine.plan_scheduled_exit(
        held_seconds=600,
        et_minutes=minute,
        weekday=when.strftime("%a"),
        expiry=when.date().isoformat(),
        today=when.date().isoformat(),
        flatten_all=True,
        close_minute=close,
    )
    assert verdict["fire"] is False

    calls = {"cancel": [], "order": [], "flatten": [], "trail": [], "lock": [], "writes": []}
    pos = _open_option(when, stop_id)
    book = _Book("SPY", pos, calls)
    monkeypatch.setattr(main_bot, "state", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", _record(calls, "cancel"))
    monkeypatch.setattr(main_bot, "_place_option_market_close", _record(calls, "order"))

    closed = asyncio.run(main_bot._maybe_scheduled_exit("SPY", dict(pos), 1.25))
    assert closed is False
    assert calls["cancel"] == []
    assert calls["order"] == []
    assert calls["writes"] == []
    assert book.pos["stop_order_id"] == stop_id

    monkeypatch.setattr(main_bot, "TRAIL_ENGINE_ENABLED", True)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"access_token": "present"})
    monkeypatch.setattr(main_bot, "_worker_stop", False)
    monkeypatch.setattr(main_bot, "_maybe_scheduled_exit", _record(calls, "flatten"))
    monkeypatch.setattr(main_bot, "_trail_pass", _record(calls, "trail"))

    async def _one_sleep(_seconds):
        main_bot._worker_stop = True

    monkeypatch.setattr(main_bot.asyncio, "sleep", _one_sleep)
    asyncio.run(main_bot.trailing_worker())

    assert calls["flatten"] == []
    assert calls["trail"] == []
    assert calls["lock"] == []
    assert calls["cancel"] == []
    assert calls["order"] == []
    assert calls["writes"] == []
    assert book.pos["stop_order_id"] == stop_id
    assert book.pos["contract"] == pos["contract"]


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


def test_tape_names_todays_flatten_on_nov_24_and_nov_27():
    """Full day is 3:40 PM ET. The 11/27 half day derives 12:40 PM ET."""
    full = datetime(2026, 11, 24, 12, 0, tzinfo=ET)
    half = datetime(2026, 11, 27, 12, 0, tzinfo=ET)
    quote = {"last": 101.0, "open": 100.0, "previous_close": 100.0}
    full_tape = main_bot.scanner_mod.read_tape(
        _tape_samples(full, [10 * 60, 10 * 60 + 15, 11 * 60, 14 * 60]),
        quote,
        _at(full, 15, 30),
    )
    half_tape = main_bot.scanner_mod.read_tape(
        _tape_samples(half, [10 * 60, 10 * 60 + 15, 10 * 60 + 30, 11 * 60]),
        quote,
        _at(half, 12, 30),
    )
    assert main_bot.scanner_mod.flatten_clock_label(_at(full, 15, 30)) == "3:40 PM ET"
    assert main_bot.scanner_mod.flatten_clock_label(_at(half, 12, 30)) == "12:40 PM ET"
    assert full_tape["watching"] == [
        "Last 30 minutes — no new entries; the bot flattens at 3:40 PM ET",
    ]
    assert half_tape["watching"] == [
        "Last 30 minutes — no new entries; the bot flattens at 12:40 PM ET",
    ]
    assert all("3:55" not in note for note in full_tape["watching"] + half_tape["watching"])


@pytest.mark.parametrize(
    "when",
    (
        datetime(2026, 11, 24, 16, 0, 1, tzinfo=ET),
        datetime(2026, 11, 27, 13, 0, 1, tzinfo=ET),
    ),
    ids=("2026-11-24 16:00:01", "2026-11-27 13:00:01"),
)
def test_trail_pass_returns_before_the_book_when_the_market_is_closed(when, monkeypatch):
    """Closed session: no position read, no flatten replace, no ratchet.

    The early return is the 5.95/5.96 behavior. The worker also skips the
    call; this locks the guard inside ``_trail_pass`` itself.
    """
    _freeze(monkeypatch, when)
    assert main_bot._is_market_open() is False
    calls = []

    async def positions():
        calls.append("positions")
        return {"SPY": {"stop_order_id": "resting-stop-1"}}

    async def replace():
        calls.append("replace")

    async def ratchet(_ticker):
        calls.append("ratchet")

    monkeypatch.setattr(main_bot.state, "all_positions", positions)
    monkeypatch.setattr(main_bot, "_replace_unfilled_flatten_closes", replace)
    monkeypatch.setattr(main_bot, "_trail_ratchet_guarded", ratchet)
    asyncio.run(main_bot._trail_pass())
    assert calls == []


_CLOSE_WINDOWS = (
    datetime(2026, 11, 24, 15, 40, tzinfo=ET),
    datetime(2026, 11, 24, 15, 59, tzinfo=ET),
    datetime(2026, 11, 27, 12, 40, tzinfo=ET),
    datetime(2026, 11, 27, 12, 59, tzinfo=ET),
)


def _close_payload(action, ticker, signal_id, **extra):
    payload = {
        "ticker": ticker,
        "action": action,
        "instrument": "option",
        "option_right": "CALL",
        "mode": "live",
        "signal_id": signal_id,
        "_origin": "app",
    }
    payload.update(extra)
    return payload


def _mentions_flatten_block(result) -> bool:
    parts = [str(result.get(key) or "") for key in ("status", "reason", "message")]
    inner = result.get("result")
    if isinstance(inner, dict):
        parts.append(str(inner.get("reason") or ""))
        parts.append(str(inner.get("message") or ""))
    text = " ".join(parts)
    return main_bot.FLATTEN_WINDOW_ENTRY_REASON in text or "flatten_window" in text


def test_close_orders_are_not_refused_by_the_close_20_entry_block(monkeypatch):
    """EXIT/close stays an exit on parked replay, execute_live_order, and process_signal.

    The window is 15:40–15:59 ET on 11/24 and 12:40–12:59 ET on 11/27.
    A BUY at those clocks is refused. A close is not.
    """
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(main_bot, "load_tokens", lambda: None)

    async def _record(*_args, **_kwargs):
        return None

    async def _send(*_args, **_kwargs):
        return None

    monkeypatch.setattr(main_bot.trade_ledger, "record", _record)
    monkeypatch.setattr(main_bot.alerts, "send", _send)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", _send)
    original_process = main_bot.process_signal

    closes = (
        ("EXIT", {}),
        ("CLOSE", {}),
        ("BUY", {"intent": "close"}),
    )

    async def _no_sleep(_seconds):
        return None

    for when in _CLOSE_WINDOWS:
        _freeze(monkeypatch, when)
        assert main_bot._flatten_window_open(when) is True
        store = main_bot.StateStore(None)
        monkeypatch.setattr(main_bot, "state", store)
        stamp = when.strftime("%H%M")

        entry = _close_payload("BUY", "SPY", f"entry-{stamp}")
        assert main_bot._is_close_payload(entry) is False
        refused = asyncio.run(main_bot.process_signal(entry))
        assert refused["status"] == "rejected"
        assert refused["reason"] == main_bot.FLATTEN_WINDOW_ENTRY_REASON

        async def _run_buy():
            await main_bot.execute_live_order(
                _close_payload("BUY", "SPY", f"exec-entry-{stamp}"),
            )

        with pytest.raises(main_bot.LocalRefusal) as caught:
            asyncio.run(_run_buy())
        assert str(caught.value) == main_bot.FLATTEN_WINDOW_ENTRY_REASON

        for index, (action, extra) in enumerate(closes):
            payload = _close_payload(
                action, "SPY", f"close-{stamp}-{index}", **extra,
            )
            assert main_bot._is_close_payload(payload) is True
            processed = asyncio.run(main_bot.process_signal(dict(payload)))
            assert processed["status"] != "rejected"
            assert _mentions_flatten_block(processed) is False

            async def _run_close(body=payload):
                await main_bot.execute_live_order(dict(body))

            with pytest.raises(main_bot.LocalRefusal) as close_caught:
                asyncio.run(_run_close())
            assert main_bot.FLATTEN_WINDOW_ENTRY_REASON not in str(close_caught.value)
            assert "tokens not set" in str(close_caught.value)

        parked_exit = _close_payload("EXIT", "SPY", f"park-exit-{stamp}")
        parked_close = _close_payload("CLOSE", "QQQ", f"park-close-{stamp}")
        parked_intent = _close_payload(
            "BUY", "IWM", f"park-intent-{stamp}", intent="close",
        )
        parked_entry = _close_payload("BUY", "DIA", f"park-entry-{stamp}")
        now = time.time()
        dropped = []

        async def _drop_record(event, data=None):
            dropped.append((event, dict(data or {})))
            return None

        monkeypatch.setattr(main_bot.trade_ledger, "record", _drop_record)

        async def _replay():
            for payload in (parked_exit, parked_close, parked_intent, parked_entry):
                item = json.dumps({
                    "payload": payload,
                    "ts": now,
                    "ticker": payload["ticker"],
                })
                await store.queue_push(main_bot.PARKED_KEY, item)
            seen = []
            real = main_bot.process_signal

            async def _spy(pd, replay=False):
                result = await real(pd, replay=replay)
                seen.append((pd, result))
                return result

            monkeypatch.setattr(main_bot, "process_signal", _spy)
            monkeypatch.setattr(main_bot.asyncio, "sleep", _no_sleep)
            await main_bot._replay_parked_signals()
            return seen

        seen = asyncio.run(_replay())
        monkeypatch.setattr(main_bot, "process_signal", original_process)
        seen_actions = {(pd.get("ticker"), pd.get("action"), pd.get("intent")) for pd, _result in seen}
        assert ("SPY", "EXIT", None) in seen_actions
        assert ("QQQ", "CLOSE", None) in seen_actions
        assert ("IWM", "BUY", "close") in seen_actions
        assert ("DIA", "BUY", None) not in seen_actions
        assert all(_mentions_flatten_block(result) is False for _pd, result in seen)
        assert any(
            event == "parked_signal_dropped"
            and data.get("ticker") == "DIA"
            and data.get("reason") == "flatten_window"
            for event, data in dropped
        )
        monkeypatch.setattr(main_bot.trade_ledger, "record", _record)
