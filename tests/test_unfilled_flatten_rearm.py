"""Unfilled flatten close: re-arm one fixed stop in the last two minutes.

Broker calls are mocked. The clock is a frozen America/New_York datetime.
Nothing in this file places an order, and nothing runs at or after the bell.
"""
import asyncio
import copy
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot

ET = ZoneInfo("America/New_York")
HALF = datetime(2026, 11, 27, 12, 0, tzinfo=ET)
FULL = datetime(2026, 11, 24, 12, 0, tzinfo=ET)
ACCOUNT = "CANARY-DO-NOT-LOG"


def _at(day: datetime, hour: int, minute: int, second: int = 0) -> datetime:
    return day.replace(hour=hour, minute=minute, second=second, microsecond=0)


def _freeze(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when.astimezone(timezone.utc))


def _position(qty: int = 4) -> dict:
    return {
        "qty": qty,
        "filled_qty": qty,
        "action": "BUY",
        "stop_order_id": None,
        "native_trail_order_id": None,
        "had_native_trail": True,
        "stop_premium": 1.40,
        # A later ratchet must not become the re-arm price.
        "trail": 9.99,
        "scheduled_exit_done": True,
        "scheduled_exit_reason": "eod_flatten",
        "accountId": ACCOUNT,
        "accountIdKey": ACCOUNT,
        "contract": {"right": "CALL", "expiration": "2026-11-27", "strike": 500},
        "pending_flatten_close": {
            "order_id": "CLOSE-1",
            "qty": qty,
            "placed_at": "2026-11-27T17:40:00+00:00",
            "stop_premium": 1.40,
            "stop": None,
            "trail": None,
            "reason": "eod_flatten",
        },
    }


class Book:
    def __init__(self, pos: dict):
        self.pos = copy.deepcopy(pos)

    async def all_positions(self):
        if not self.pos:
            return {}
        return {"SPY": copy.deepcopy(self.pos)}

    async def get_position(self, ticker):
        if ticker != "SPY" or not self.pos:
            return None
        return copy.deepcopy(self.pos)

    async def set_position(self, ticker, pos):
        self.pos = copy.deepcopy(pos)

    async def delete_position(self, ticker):
        old = copy.deepcopy(self.pos)
        self.pos = None
        return old

    async def update_guard(self, *args, **kwargs):
        return None

    async def all_guards(self):
        return {}


def _arm(monkeypatch, when: datetime, pos: dict, *, enabled=None):
    """Frozen clock, in-memory book, and broker spies. Returns the recorder."""
    book = Book(pos)
    calls = []
    alerts = []
    stops = []
    script = []
    state = {"cancel": main_bot.CANCEL_CONFIRMED, "fill": None, "status_error": False}

    _freeze(monkeypatch, when)
    monkeypatch.setattr(main_bot, "state", book)
    if enabled is None:
        monkeypatch.delenv("UNFILLED_FLATTEN_REARM_ENABLED", raising=False)
    else:
        monkeypatch.setenv("UNFILLED_FLATTEN_REARM_ENABLED", "true" if enabled else "false")

    async def order_state(order_id, client_id):
        calls.append("status")
        assert order_id == "CLOSE-1"
        assert client_id is None
        if state["status_error"]:
            raise RuntimeError("broker status unavailable")
        if not script:
            raise AssertionError("order status read was not scripted")
        status, filled = script.pop(0)
        state["last_status"] = status
        return status, filled

    async def cancel(order_id, expected_qty=None):
        calls.append("cancel")
        assert order_id == "CLOSE-1"
        return state["cancel"]

    async def place_stop(ticker, contract, qty, stop_premium):
        # The close must already be confirmed dead. A stop beside a working
        # sell-close can oversell into a short.
        assert str(state.get("last_status") or "").upper() in {
            "CANCELLED", "CANCELED", "EXPIRED", "REJECTED",
        }
        if "cancel" in calls:
            cancel_at = calls.index("cancel")
            assert any(i > cancel_at for i, name in enumerate(calls) if name == "status")
        calls.append("stop")
        stops.append({"ticker": ticker, "qty": int(qty), "stop": float(stop_premium)})
        return {
            "order_id": "STOP-NEW", "client_id": "cid",
            "qty": int(qty), "stop": float(stop_premium),
        }

    async def native(*args, **kwargs):
        calls.append("native")
        raise AssertionError("TRAILING_STOP_CNST must not be re-armed here")

    async def resolve(order_id, timeout_s=8.0):
        calls.append("fill")
        return state["fill"]

    async def send(severity, event, message, **kwargs):
        alerts.append({
            "severity": severity, "event": event, "message": message, **kwargs,
        })

    async def no_close(*args, **kwargs):
        calls.append("close")
        raise AssertionError("a second flatten close was sent")

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_stop)
    monkeypatch.setattr(main_bot, "_arm_native_trail", native)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", resolve)
    monkeypatch.setattr(main_bot, "_place_option_market_close", no_close)
    monkeypatch.setattr(main_bot.alerts, "send", send)
    return {
        "book": book, "calls": calls, "alerts": alerts, "stops": stops,
        "script": script, "state": state,
    }


def _critical(alerts):
    return [a for a in alerts if a["event"] == "unfilled_flatten_rearm"]


def _run(recorder) -> None:
    asyncio.run(main_bot._rearm_unfilled_flatten_closes())


def test_rearm_window_is_the_last_two_open_minutes():
    assert main_bot._unfilled_flatten_rearm_window(_at(HALF, 12, 57)) is False
    assert main_bot._unfilled_flatten_rearm_window(_at(HALF, 12, 58)) is True
    assert main_bot._unfilled_flatten_rearm_window(_at(HALF, 12, 59)) is True
    assert main_bot._unfilled_flatten_rearm_window(_at(HALF, 13, 0, 0)) is False
    assert main_bot._unfilled_flatten_rearm_window(_at(HALF, 13, 0, 1)) is False
    assert main_bot._unfilled_flatten_rearm_window(_at(FULL, 15, 57)) is False
    assert main_bot._unfilled_flatten_rearm_window(_at(FULL, 15, 58)) is True
    assert main_bot._unfilled_flatten_rearm_window(_at(FULL, 15, 59)) is True
    assert main_bot._unfilled_flatten_rearm_window(_at(FULL, 16, 0, 0)) is False
    assert main_bot._unfilled_flatten_rearm_window(_at(FULL, 16, 0, 1)) is False


@pytest.mark.parametrize(
    "when",
    (_at(HALF, 12, 50), _at(HALF, 12, 57), _at(FULL, 15, 57)),
    ids=("12:50-half", "12:57-half", "15:57-full"),
)
def test_close_that_fills_before_close_minus_2_does_nothing(when, monkeypatch):
    """The fill already happened. Before close-2 there is no cancel, re-arm, or alert."""
    rec = _arm(monkeypatch, when, _position())
    rec["script"].append(("EXECUTED", 4))

    async def boom(*args, **kwargs):
        raise AssertionError("broker was touched before the re-arm window")

    monkeypatch.setattr(main_bot, "_order_state", boom)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", boom)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", boom)
    monkeypatch.setattr(main_bot.alerts, "send", boom)
    _run(rec)
    assert rec["calls"] == []
    assert rec["stops"] == []
    assert rec["alerts"] == []


@pytest.mark.parametrize(
    "when",
    (_at(HALF, 12, 58), _at(FULL, 15, 58)),
    ids=("2026-11-27-12:58", "2026-11-24-15:58"),
)
def test_unfilled_close_rearms_one_fixed_stop_and_a_second_tick_is_idle(when, monkeypatch):
    rec = _arm(monkeypatch, when, _position(qty=4))
    rec["script"].extend([("OPEN", 0), ("CANCELLED", 0)])
    _run(rec)

    assert rec["calls"] == ["status", "cancel", "status", "stop"]
    assert rec["stops"] == [{"ticker": "SPY", "qty": 4, "stop": 1.40}]
    assert "native" not in rec["calls"]
    assert "close" not in rec["calls"]
    crit = _critical(rec["alerts"])
    assert len(crit) == 1
    assert crit[0]["severity"] == "critical"
    text = crit[0]["message"]
    assert "SPY" in text
    assert "CLOSE-1" in text
    assert "for 4" in text
    assert "CANCELLED" in text
    assert "re-armed: yes" in text
    assert ACCOUNT not in text
    stored = rec["book"].pos
    assert stored["stop_order_id"] == "STOP-NEW"
    assert stored["unfilled_flatten_rearmed"] is True
    assert stored["had_native_trail"] is True
    assert stored["native_trail_order_id"] is None
    assert stored["scheduled_exit_done"] is True

    _run(rec)
    assert rec["calls"] == ["status", "cancel", "status", "stop"]
    assert rec["stops"] == [{"ticker": "SPY", "qty": 4, "stop": 1.40}]
    assert len(_critical(rec["alerts"])) == 1


def test_partial_fill_stop_covers_only_the_remainder(monkeypatch):
    rec = _arm(monkeypatch, _at(HALF, 12, 58), _position(qty=4))
    rec["script"].extend([("OPEN", 1), ("CANCELLED", 1)])
    _run(rec)

    assert rec["calls"].index("stop") > rec["calls"].index("cancel")
    assert rec["calls"].index("cancel") > rec["calls"].index("status")
    assert rec["stops"] == [{"ticker": "SPY", "qty": 3, "stop": 1.40}]
    assert rec["book"].pos["filled_qty"] == 3
    assert rec["book"].pos["stop_order_id"] == "STOP-NEW"
    text = _critical(rec["alerts"])[0]["message"]
    assert "for 4" in text
    assert "re-armed: yes for 3" in text
    assert "CANCELLED" in text
    assert ACCOUNT not in text

    before = list(rec["calls"])
    _run(rec)
    assert rec["calls"] == before
    assert len(rec["stops"]) == 1


def test_cancel_not_confirmed_dead_places_no_stop_and_retries(monkeypatch):
    """CANCEL_CONFIRMED from the helper is not enough when the order is still requested."""
    rec = _arm(monkeypatch, _at(HALF, 12, 58), _position())
    rec["state"]["cancel"] = main_bot.CANCEL_CONFIRMED
    rec["script"].extend([
        ("OPEN", 0), ("CANCEL_REQUESTED", 0),
        ("OPEN", 0), ("CANCEL_REQUESTED", 0),
    ])
    _run(rec)
    assert rec["stops"] == []
    assert "stop" not in rec["calls"]
    assert rec["calls"] == ["status", "cancel", "status"]
    crit = _critical(rec["alerts"])
    assert len(crit) == 1
    assert crit[0]["severity"] == "critical"
    assert "CANCEL_REQUESTED" in crit[0]["message"]
    assert "re-armed: no" in crit[0]["message"]
    assert ACCOUNT not in crit[0]["message"]
    assert rec["book"].pos.get("stop_order_id") in (None, "")
    assert not rec["book"].pos.get("unfilled_flatten_rearmed")

    _run(rec)
    assert rec["calls"] == ["status", "cancel", "status", "status", "cancel", "status"]
    assert rec["stops"] == []
    assert len(_critical(rec["alerts"])) == 2


def test_cancel_live_places_no_stop_and_retries(monkeypatch):
    rec = _arm(monkeypatch, _at(FULL, 15, 58), _position())
    rec["state"]["cancel"] = main_bot.CANCEL_LIVE
    rec["script"].extend([("OPEN", 0), ("OPEN", 0), ("OPEN", 0), ("OPEN", 0)])
    _run(rec)
    _run(rec)
    assert rec["stops"] == []
    assert rec["calls"].count("cancel") == 2
    assert "stop" not in rec["calls"]
    assert len(_critical(rec["alerts"])) == 2
    assert all("re-armed: no" in a["message"] for a in _critical(rec["alerts"]))


def test_status_read_error_places_no_stop_and_retries(monkeypatch):
    rec = _arm(monkeypatch, _at(HALF, 12, 58), _position())
    rec["state"]["status_error"] = True
    _run(rec)
    assert rec["calls"] == ["status"]
    assert rec["stops"] == []
    crit = _critical(rec["alerts"])
    assert len(crit) == 1
    assert "status_unread" in crit[0]["message"]
    assert "re-armed: no" in crit[0]["message"]
    assert "SPY" in crit[0]["message"]
    assert "CLOSE-1" in crit[0]["message"]
    assert ACCOUNT not in crit[0]["message"]

    _run(rec)
    assert rec["calls"] == ["status", "status"]
    assert rec["stops"] == []
    assert len(_critical(rec["alerts"])) == 2


def test_kill_switch_off_alerts_only_and_the_next_tick_reads_the_env(monkeypatch):
    rec = _arm(monkeypatch, _at(HALF, 12, 58), _position(), enabled=False)
    rec["script"].extend([("OPEN", 0), ("OPEN", 0), ("CANCELLED", 0)])
    _run(rec)
    assert rec["calls"] == ["status"]
    assert rec["stops"] == []
    crit = _critical(rec["alerts"])
    assert len(crit) == 1
    assert crit[0]["severity"] == "critical"
    assert "OPEN" in crit[0]["message"]
    assert "re-armed: no" in crit[0]["message"]
    assert "SPY" in crit[0]["message"] and "CLOSE-1" in crit[0]["message"]
    assert ACCOUNT not in crit[0]["message"]

    monkeypatch.setenv("UNFILLED_FLATTEN_REARM_ENABLED", "true")
    _run(rec)
    assert rec["calls"] == ["status", "status", "cancel", "status", "stop"]
    assert rec["stops"] == [{"ticker": "SPY", "qty": 4, "stop": 1.40}]
    assert len(rec["stops"]) == 1


def test_stop_is_never_placed_while_the_close_is_still_working(monkeypatch):
    rec = _arm(monkeypatch, _at(HALF, 12, 59), _position())
    seen = {"last": None}

    async def order_state(order_id, client_id):
        rec["calls"].append("status")
        if seen["last"] is None:
            seen["last"] = "OPEN"
            return "OPEN", 0
        seen["last"] = "CANCELLED"
        return "CANCELLED", 0

    async def place_stop(ticker, contract, qty, stop_premium):
        assert seen["last"] == "CANCELLED"
        assert rec["calls"].index("cancel") < len(rec["calls"]) - 1
        assert "status" in rec["calls"][rec["calls"].index("cancel") + 1:]
        rec["calls"].append("stop")
        rec["stops"].append(int(qty))
        return {"order_id": "STOP-NEW", "client_id": "c", "qty": int(qty), "stop": 1.40}

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_stop)
    _run(rec)
    assert rec["calls"] == ["status", "cancel", "status", "stop"]
    assert rec["stops"] == [4]


def test_filled_close_inside_the_window_is_booked_without_cancel_or_rearm(monkeypatch):
    rec = _arm(monkeypatch, _at(HALF, 12, 58), _position())
    rec["script"].append(("EXECUTED", 4))
    rec["state"]["fill"] = 1.55
    booked = []

    async def record_close(ticker, exit_price, payload, exit_premium=None, close_reason=None, **kwargs):
        booked.append((ticker, exit_premium, close_reason))
        rec["book"].pos = None

    monkeypatch.setattr(main_bot, "_record_close", record_close)
    _run(rec)
    assert booked == [("SPY", 1.55, "eod_flatten")]
    assert "cancel" not in rec["calls"]
    assert "stop" not in rec["calls"]
    assert _critical(rec["alerts"]) == []
    _run(rec)
    assert booked == [("SPY", 1.55, "eod_flatten")]
    assert "stop" not in rec["calls"]


def test_executed_close_without_a_price_does_not_place_a_stop(monkeypatch):
    rec = _arm(monkeypatch, _at(FULL, 15, 58), _position())
    rec["script"].extend([("EXECUTED", 4), ("EXECUTED", 4)])
    rec["state"]["fill"] = None
    _run(rec)
    _run(rec)
    assert "cancel" not in rec["calls"]
    assert "stop" not in rec["calls"]
    assert _critical(rec["alerts"]) == []
    assert rec["book"].pos.get("pending_close") is True
    assert rec["book"].pos.get("stop_order_id") in (None, "")


@pytest.mark.parametrize(
    "when",
    (
        _at(HALF, 13, 0, 0),
        _at(HALF, 13, 0, 1),
        _at(FULL, 16, 0, 0),
        _at(FULL, 16, 0, 1),
    ),
    ids=("13:00:00", "13:00:01", "16:00:00", "16:00:01"),
)
def test_at_and_after_the_bell_nothing_runs(when, monkeypatch):
    rec = _arm(monkeypatch, when, _position())

    async def boom(*args, **kwargs):
        raise AssertionError(f"broker touched at {when.isoformat()}")

    monkeypatch.setattr(main_bot, "_order_state", boom)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", boom)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", boom)
    monkeypatch.setattr(main_bot.alerts, "send", boom)
    _run(rec)

    # The worker itself only enters the tick while the session is open.
    # 13:00:00 is still "open" on the exchange clock; 13:00:01 is not.
    if not main_bot._is_market_open():
        monkeypatch.setattr(main_bot, "TRAIL_ENGINE_ENABLED", True)
        monkeypatch.setattr(main_bot, "load_tokens", lambda: {"access_token": "present"})
        monkeypatch.setattr(main_bot, "_worker_stop", False)

        async def _one_sleep(_seconds):
            main_bot._worker_stop = True

        monkeypatch.setattr(main_bot.asyncio, "sleep", _one_sleep)
        asyncio.run(main_bot.trailing_worker())
    assert rec["calls"] == []
    assert rec["stops"] == []
    assert rec["book"].pos["stop_order_id"] is None
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_scheduled_exit_records_the_pending_close_and_will_not_send_another(monkeypatch):
    when = _at(HALF, 12, 40)
    _freeze(monkeypatch, when)
    monkeypatch.setattr(main_bot, "DAY_TRADE_MODE", True)
    monkeypatch.setattr(main_bot, "SCHEDULED_EXIT_ENABLED", True)
    pos = {
        "qty": 2,
        "filled_qty": 2,
        "action": "BUY",
        "stop_order_id": "STOP-OLD",
        "native_trail_order_id": "TRAIL-OLD",
        "stop_premium": 0.85,
        "trail": 0.90,
        "stop": 500.0,
        "ts": when.astimezone(timezone.utc).isoformat(),
        "contract": {"right": "CALL", "expiration": "2026-11-27", "strike": 500},
    }
    book = Book(pos)
    placed = []
    alerts = []

    async def cancel(order_id, expected_qty=None):
        assert order_id == "TRAIL-OLD" or order_id == "STOP-OLD"
        return main_bot.CANCEL_CONFIRMED

    async def place_close(ticker, contract, qty, market_ok=False):
        placed.append((ticker, qty, market_ok))
        return "CLOSE-77"

    async def resolve(order_id, timeout_s=8.0):
        return None

    async def send(*args, **kwargs):
        alerts.append(args)

    monkeypatch.setattr(main_bot, "state", book)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_option_market_close", place_close)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", resolve)
    monkeypatch.setattr(main_bot.alerts, "send", send)

    closed = asyncio.run(main_bot._maybe_scheduled_exit("SPY", dict(pos), 1.25))
    assert closed is True
    assert placed == [("SPY", 2, True)]
    pending = book.pos["pending_flatten_close"]
    assert pending["order_id"] == "CLOSE-77"
    assert pending["qty"] == 2
    assert isinstance(pending["placed_at"], str) and pending["placed_at"]
    assert pending["stop_premium"] == 0.85
    assert pending["trail"] == 0.90
    assert pending["stop"] == 500.0
    assert book.pos["scheduled_exit_done"] is True
    assert book.pos["stop_order_id"] is None
    assert book.pos["native_trail_order_id"] is None
    assert all(a[1] != "unfilled_flatten_rearm" for a in alerts if len(a) > 1)

    again = asyncio.run(main_bot._maybe_scheduled_exit("SPY", dict(book.pos), 1.25))
    assert again is False
    assert placed == [("SPY", 2, True)]


def test_trail_pass_invokes_the_rearm_check_inside_the_window(monkeypatch):
    when = _at(HALF, 12, 58)
    _freeze(monkeypatch, when)
    book = Book(_position())
    seen = []

    async def hook():
        seen.append("rearm")

    monkeypatch.setattr(main_bot, "state", book)
    monkeypatch.setattr(main_bot, "_rearm_unfilled_flatten_closes", hook)
    asyncio.run(main_bot._trail_pass())
    assert seen == ["rearm"]
