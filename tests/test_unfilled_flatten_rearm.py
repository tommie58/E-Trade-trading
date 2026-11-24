"""Unfilled flatten close: replace with one MARKET sell from close-10.

Broker calls are mocked. The clock is a frozen America/New_York datetime.
This path never places a stop. Nothing runs at or after the bell.
"""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot

ET = ZoneInfo("America/New_York")
HALF = datetime(2026, 11, 27, 12, 0, tzinfo=ET)  # half day, close 13:00
FULL = datetime(2026, 11, 24, 12, 0, tzinfo=ET)  # full day, close 16:00
ACCOUNT = "CANARY-DO-NOT-LOG"


def _at(day: datetime, hour: int, minute: int, second: int = 0) -> datetime:
    return day.replace(hour=hour, minute=minute, second=second, microsecond=0)


def _freeze(monkeypatch, when: datetime) -> None:
    monkeypatch.setattr(main_bot, "_utcnow", lambda: when.astimezone(timezone.utc))


def _row(status: str, filled: int = 0, ordered: int = 4) -> dict:
    return {"status": status, "filled": filled, "ordered": ordered}


def _position(when: datetime, qty: int = 4, *, expiration: str = "2026-11-27") -> dict:
    placed = (when - timedelta(minutes=10)).astimezone(timezone.utc).isoformat()
    return {
        "qty": qty,
        "filled_qty": qty,
        "action": "BUY",
        "entry_premium": 1.25,
        "stop_order_id": None,
        "native_trail_order_id": None,
        "had_native_trail": True,
        "stop_premium": 1.40,
        "trail": 9.99,
        "scheduled_exit_done": True,
        "scheduled_exit_reason": "eod_flatten",
        "accountId": ACCOUNT,
        "accountIdKey": ACCOUNT,
        "contract": {"right": "CALL", "expiration": expiration, "strike": 500},
        "pending_flatten_close": {
            # Local qty is a decoy. The replacement size comes from the broker.
            "order_id": "CLOSE-1",
            "qty": 99,
            "placed_at": placed,
            "reason": "eod_flatten",
            "replacements": 0,
        },
    }


class _Lock:
    async def acquire(self):
        return None

    async def release(self):
        return None

    async def try_acquire(self):
        return True


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

    async def set(self, *args, **kwargs):
        return None

    def position_lock(self, *args, **kwargs):
        return _Lock()

    def lock(self, *args, **kwargs):
        return _Lock()


def _arm(monkeypatch, when: datetime, pos: dict, *, enabled=None, held=None):
    """Frozen clock, in-memory book, and broker spies."""
    book = Book(pos)
    calls = []
    alerts = []
    orders = []
    ledger = []
    scripts = {"CLOSE-1": []}
    open_orders = []
    state = {"fill": None, "read_error": False, "open_error": False, "place_error": False}

    _freeze(monkeypatch, when)
    monkeypatch.setattr(main_bot, "state", book)
    if enabled is None:
        monkeypatch.delenv("UNFILLED_FLATTEN_REPLACE_ENABLED", raising=False)
    else:
        monkeypatch.setenv("UNFILLED_FLATTEN_REPLACE_ENABLED", "true" if enabled else "false")
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})

    async def account_key(_tokens):
        return "ACCT"

    async def read_order(order_id):
        calls.append(("status", str(order_id)))
        if state["read_error"]:
            raise RuntimeError("broker status unavailable")
        queue = scripts.get(str(order_id))
        if not queue:
            raise AssertionError(f"no status script for {order_id}")
        return queue.pop(0)

    async def cancel(order_id, expected_qty=None):
        calls.append(("cancel", str(order_id), expected_qty))
        return main_bot.CANCEL_CONFIRMED

    async def place_order(kind, common, tokens):
        if state["place_error"]:
            calls.append("place_failed")
            raise RuntimeError("broker rejected the close")
        orders.append(dict(common))
        calls.append("place")
        if any(str(order.get("priceType") or "").upper() in {"STOP", "STOP_LIMIT", "TRAILING_STOP_CNST"}
               for order in orders):
            raise AssertionError("a stop was placed by the flatten path")
        return {"PlaceOrderResponse": {"OrderIds": [{"orderId": "CLOSE-2"}]}}

    async def place_stop(*args, **kwargs):
        calls.append("stop")
        raise AssertionError("this path must not place a stop")

    async def native(*args, **kwargs):
        calls.append("native")
        raise AssertionError("this path must not promote a native trail")

    async def rearm(*args, **kwargs):
        calls.append("rearm")
        raise AssertionError("this path must not re-arm a guard")

    async def handoff(*args, **kwargs):
        calls.append("handoff")
        raise AssertionError("this path must not hand off a native trail")

    async def eyes(ticker, pos):
        calls.append("eyes")
        raise AssertionError("trail eyes ran under a pending flatten close")

    async def resolve(order_id, timeout_s=8.0):
        calls.append("fill")
        return state["fill"]

    async def open_orders_fn():
        calls.append("open_orders")
        if state["open_error"]:
            raise RuntimeError("open orders unavailable")
        return list(open_orders)

    async def held_qty(ticker, pos):
        calls.append("held")
        return held

    async def send(severity, event, message, **kwargs):
        alerts.append({
            "severity": severity, "event": event, "message": message, **kwargs,
        })
        assert ACCOUNT not in str(message)
        assert "oauth" not in str(message).lower()

    async def record(event, data=None):
        ledger.append({"event": event, "data": dict(data or {})})

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account_key)
    monkeypatch.setattr(main_bot, "_flatten_read_order", read_order)
    monkeypatch.setattr(main_bot, "_flatten_open_orders", open_orders_fn)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held_qty)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_place_order_smart", place_order)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_stop)
    monkeypatch.setattr(main_bot, "_arm_native_trail", native)
    monkeypatch.setattr(main_bot, "_reconcile_rearm_guard", rearm)
    monkeypatch.setattr(main_bot, "_maybe_hand_off_native_trail", handoff)
    monkeypatch.setattr(main_bot, "_refresh_trail_eyes", eyes)
    monkeypatch.setattr(main_bot, "_resolve_exit_fill", resolve)
    monkeypatch.setattr(main_bot.alerts, "send", send)
    monkeypatch.setattr(main_bot.trade_ledger, "record", record)
    return {
        "book": book, "calls": calls, "alerts": alerts, "orders": orders,
        "ledger": ledger, "scripts": scripts, "open_orders": open_orders,
        "state": state,
    }


def _run(recorder=None) -> None:
    asyncio.run(main_bot._replace_unfilled_flatten_closes())


def _replace_alerts(alerts):
    return [a for a in alerts if a["event"] == "unfilled_flatten_replace"]


def _manual_alerts(alerts):
    return [a for a in alerts if a["event"] == "unfilled_flatten_manual"]


def test_replace_window_is_close_minus_10_until_the_bell():
    assert main_bot._unfilled_flatten_replace_window(_at(HALF, 12, 49)) is False
    assert main_bot._unfilled_flatten_replace_window(_at(HALF, 12, 50)) is True
    assert main_bot._unfilled_flatten_replace_window(_at(HALF, 12, 59)) is True
    assert main_bot._unfilled_flatten_replace_window(_at(HALF, 13, 0, 0)) is False
    assert main_bot._unfilled_flatten_replace_window(_at(HALF, 13, 0, 1)) is False
    assert main_bot._unfilled_flatten_replace_window(_at(FULL, 15, 49)) is False
    assert main_bot._unfilled_flatten_replace_window(_at(FULL, 15, 50)) is True
    assert main_bot._unfilled_flatten_replace_window(_at(FULL, 15, 59)) is True
    assert main_bot._unfilled_flatten_replace_window(_at(FULL, 16, 0, 0)) is False
    assert main_bot._unfilled_flatten_replace_window(_at(FULL, 16, 0, 1)) is False


@pytest.mark.parametrize("when", (_at(HALF, 12, 49), _at(FULL, 15, 49)), ids=("12:49", "15:49"))
def test_close_filled_before_close_minus_10_does_nothing(when, monkeypatch):
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].append(_row("EXECUTED", 4, 4))

    async def boom(*args, **kwargs):
        raise AssertionError("broker was touched before close-10")

    monkeypatch.setattr(main_bot, "_flatten_read_order", boom)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", boom)
    monkeypatch.setattr(main_bot, "_place_order_smart", boom)
    _run()
    assert rec["calls"] == []
    assert rec["alerts"] == []
    assert rec["orders"] == []


def _assert_market_sell(order: dict, qty: int) -> None:
    assert order["orderAction"] == "SELL_CLOSE"
    assert order["priceType"] == "MARKET"
    assert order["orderTerm"] == "GOOD_FOR_DAY"
    assert order["quantity"] == qty
    assert "limitPrice" not in order
    assert order["priceType"] not in {"STOP", "STOP_LIMIT", "TRAILING_STOP_CNST"}


@pytest.mark.parametrize(
    "when",
    (_at(HALF, 12, 50), _at(FULL, 15, 50)),
    ids=("12:50-half", "15:50-full"),
)
def test_unfilled_close_is_replaced_by_one_market_sell(when, monkeypatch):
    pos = _position(when)
    rec = _arm(monkeypatch, when, pos)
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("CANCELLED", 0, 4)])
    rec["scripts"]["CLOSE-2"] = [_row("OPEN", 0, 4)]

    _run()
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 1
    assert rec["orders"] and rec["orders"][0]["quantity"] == 4
    _assert_market_sell(rec["orders"][0], 4)
    assert "stop" not in rec["calls"]
    assert "native" not in rec["calls"]
    assert any(a["event"] == "unfilled_flatten_replace" and "MARKET" in a["message"] for a in rec["alerts"])
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-2"
    assert rec["book"].pos["pending_flatten_close"]["replacements"] == 1

    _run()
    assert rec["calls"].count("place") == 1
    assert rec["orders"][0]["quantity"] == 4
    assert "stop" not in rec["calls"]


def test_market_order_at_the_bell_boundary_and_manual_0dte_alert(monkeypatch):
    when = _at(HALF, 12, 59)
    rec = _arm(monkeypatch, when, _position(when, expiration="2026-11-27"))
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("CANCELLED", 0, 4)])
    rec["scripts"]["CLOSE-2"] = [_row("OPEN", 0, 4), _row("OPEN", 0, 4)]

    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)
    manual = _manual_alerts(rec["alerts"])
    assert len(manual) == 1
    assert "close it manually" in manual[0]["message"]
    assert "0DTE" in manual[0]["message"]
    assert "expires today" in manual[0]["message"]
    assert "stop" not in rec["calls"]

    _run()
    assert rec["calls"].count("place") == 1
    assert len(_manual_alerts(rec["alerts"])) == 1


def test_manual_alert_says_when_the_contract_does_not_expire_today(monkeypatch):
    when = _at(HALF, 12, 59)
    pos = _position(when, expiration="2026-12-18")
    pos["pending_flatten_close"]["placed_at"] = when.astimezone(timezone.utc).isoformat()
    rec = _arm(monkeypatch, when, pos)
    rec["scripts"]["CLOSE-1"].append(_row("OPEN", 0, 4))
    _run()
    assert rec["calls"].count("place") == 0
    manual = _manual_alerts(rec["alerts"])
    assert len(manual) == 1
    assert "does not expire today" in manual[0]["message"]
    assert "0DTE" not in manual[0]["message"]
    assert "close it manually" in manual[0]["message"]


def test_partial_fill_replaces_the_remainder_and_books_unknown_price_as_none(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].extend([
        _row("OPEN", 1, 4),
        _row("CANCELLED", 1, 4),
    ])
    rec["state"]["fill"] = None
    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 3)
    assert rec["ledger"]
    data = rec["ledger"][0]["data"]
    assert rec["ledger"][0]["event"] == "flatten_partial_unpriced"
    assert data["exit"] is None
    assert data["price_unknown"] is True
    assert data["needs_reconcile_trueup"] is True
    assert data["realized_pnl_pct"] is None
    assert data["reason"] == "flatten_close_partial"
    assert 0.0 not in data.values()
    assert 0 not in data.values()
    assert rec["book"].pos["price_unknown"] is True
    assert rec["book"].pos["needs_reconcile_trueup"] is True
    assert rec["book"].pos["unpriced_close_order_id"] == "CLOSE-1"


def test_replacement_qty_is_capped_by_the_broker_position_not_local_state(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when, qty=9)
    rec = _arm(monkeypatch, when, pos, held=2)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 2)
    assert ("cancel", "CLOSE-1", 4) not in rec["calls"]


def test_fill_during_cancel_sends_no_replacement(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=99)
    rec["scripts"]["CLOSE-1"].extend([
        _row("OPEN", 0, 4),
        _row("FILLED", 4, 4),
    ])
    booked = []

    async def record_close(ticker, exit_price, payload, exit_premium=None, close_reason=None, **kwargs):
        booked.append((ticker, exit_premium, close_reason))
        rec["book"].pos = None

    rec["state"]["fill"] = 1.40
    monkeypatch.setattr(main_bot, "_record_close", record_close)
    _run()
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert booked == [("SPY", 1.40, "eod_flatten")]
    assert "stop" not in rec["calls"]


def test_cancel_not_confirmed_sends_nothing_and_alerts(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].extend([
        _row("OPEN", 0, 4),
        _row("CANCEL_REQUESTED", 0, 4),
    ])
    _run()
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 0
    assert rec["alerts"]
    assert any("no new order" in a["message"] for a in rec["alerts"])
    assert "stop" not in rec["calls"]


def test_status_read_error_alerts_and_sends_nothing(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["state"]["read_error"] = True
    _run()
    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    assert any("could not be read" in a["message"] for a in rec["alerts"])


def test_not_found_alerts_and_sends_nothing(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].append(_row("NOT_FOUND", 0, 0))
    _run()
    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    assert any("NOT_FOUND" in a["message"] for a in rec["alerts"])
    assert "stop" not in rec["calls"]


def test_resting_stop_is_cancelled_and_confirmed_before_the_replacement(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["scripts"]["STOP-9"] = [_row("CANCELLED", 0, 4)]
    rec["open_orders"].append({
        "order_id": "STOP-9",
        "symbol": "SPY",
        "status": "OPEN",
        "price_type": "STOP",
        "order_action": "SELL_CLOSE",
    })
    _run()
    cancel_at = rec["calls"].index(("cancel", "STOP-9", None))
    assert rec["calls"].index("place") > cancel_at
    assert ("status", "STOP-9") in rec["calls"]
    assert rec["calls"].index(("status", "STOP-9")) < rec["calls"].index("place")
    _assert_market_sell(rec["orders"][0], 4)
    assert "stop" not in rec["calls"]


def test_unknown_protective_order_skips_the_replacement(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["scripts"]["TRAIL-1"] = [_row("CANCEL_REQUESTED", 0, 4)]
    rec["open_orders"].append({
        "order_id": "TRAIL-1",
        "symbol": "SPY",
        "status": "OPEN",
        "price_type": "TRAILING_STOP_CNST",
        "order_action": "SELL_CLOSE",
    })
    _run()
    assert rec["calls"].count("place") == 0
    assert any("no replacement close" in a["message"] for a in rec["alerts"])
    assert "stop" not in rec["calls"]
    assert "native" not in rec["calls"]


def test_protective_read_error_skips_the_replacement(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when)
    pos["native_trail_order_id"] = "TRAIL-1"
    rec = _arm(monkeypatch, when, pos)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["state"]["read_error"] = False

    async def read_order(order_id):
        rec["calls"].append(("status", str(order_id)))
        if str(order_id) == "TRAIL-1":
            raise RuntimeError("trail status unavailable")
        return _row("EXPIRED", 0, 4)

    monkeypatch.setattr(main_bot, "_flatten_read_order", read_order)
    _run()
    assert rec["calls"].count("place") == 0
    assert any("could not be confirmed dead" in a["message"] for a in rec["alerts"])


def test_not_found_stop_is_not_working_and_the_close_still_replaces(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when)
    pos["stop_order_id"] = "STOP-OLD"
    rec = _arm(monkeypatch, when, pos)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["scripts"]["STOP-OLD"] = [_row("NOT_FOUND", 0, 0)]
    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)
    assert "stop" not in rec["calls"]


def test_reject_storm_stays_inside_the_cap_and_dedupes_alerts(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    rec["state"]["place_error"] = True
    per_tick = []

    async def read_order(order_id):
        rec["calls"].append(("status", str(order_id)))
        return _row("REJECTED", 0, 4)

    monkeypatch.setattr(main_bot, "_flatten_read_order", read_order)
    for _ in range(12):
        before = rec["calls"].count("place_failed")
        _run()
        per_tick.append(rec["calls"].count("place_failed") - before)

    assert per_tick.count(1) == 3
    assert all(n in {0, 1} for n in per_tick)
    assert rec["calls"].count("place_failed") == 3
    assert sum(1 for a in rec["alerts"] if "FAILED" in a["message"]) == 1
    assert sum(1 for a in rec["alerts"] if "waiting before" in a["message"]) == 1
    assert sum(1 for a in rec["alerts"] if "cap is reached" in a["message"]) == 1
    assert rec["orders"] == []
    assert "stop" not in rec["calls"]

    # A later minute may alert again, but it cannot place a fourth close.
    _freeze(monkeypatch, _at(HALF, 12, 53))
    _run()
    assert rec["calls"].count("place_failed") == 3


def test_kill_switch_alerts_only_and_is_reread_each_tick(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), enabled=False)
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4)] * 3)
    _run()
    _run()
    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    assert sum(1 for a in rec["alerts"] if "replacement is off" in a["message"]) == 1

    _freeze(monkeypatch, _at(HALF, 12, 52))
    _run()
    assert sum(1 for a in rec["alerts"] if "replacement is off" in a["message"]) == 2

    monkeypatch.setenv("UNFILLED_FLATTEN_REPLACE_ENABLED", "true")
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("CANCELLED", 0, 4)])
    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)


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
    rec = _arm(monkeypatch, when, _position(when))

    async def boom(*args, **kwargs):
        raise AssertionError(f"broker touched at {when.isoformat()}")

    monkeypatch.setattr(main_bot, "_flatten_read_order", boom)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", boom)
    monkeypatch.setattr(main_bot, "_place_order_smart", boom)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", boom)
    monkeypatch.setattr(main_bot.alerts, "send", boom)
    _run()

    if not main_bot._is_market_open():
        monkeypatch.setattr(main_bot, "TRAIL_ENGINE_ENABLED", True)
        monkeypatch.setattr(main_bot, "_worker_stop", False)

        async def _one_sleep(_seconds):
            main_bot._worker_stop = True

        monkeypatch.setattr(main_bot.asyncio, "sleep", _one_sleep)
        asyncio.run(main_bot.trailing_worker())
    assert rec["calls"] == []
    assert rec["orders"] == []
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_scheduled_exit_records_the_pending_close_without_a_stop_snapshot(monkeypatch):
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
        assert order_id in {"TRAIL-OLD", "STOP-OLD"}
        return main_bot.CANCEL_CONFIRMED

    async def place_close(ticker, contract, qty, market_ok=False, force_market=False):
        placed.append((ticker, qty, market_ok, force_market))
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
    assert placed == [("SPY", 2, True, False)]
    pending = book.pos["pending_flatten_close"]
    assert pending["order_id"] == "CLOSE-77"
    assert pending["qty"] == 2
    assert isinstance(pending["placed_at"], str) and pending["placed_at"]
    assert "stop_premium" not in pending
    assert "trail" not in pending
    assert "stop" not in pending
    assert book.pos["scheduled_exit_done"] is True
    assert book.pos["stop_order_id"] is None
    assert book.pos["native_trail_order_id"] is None
    assert all(a[1] != "unfilled_flatten_replace" for a in alerts if len(a) > 1)

    again = asyncio.run(main_bot._maybe_scheduled_exit("SPY", dict(book.pos), 1.25))
    assert again is False
    assert placed == [("SPY", 2, True, False)]


def test_trail_pass_does_not_arm_or_promote_a_stop_while_a_flatten_close_is_pending(monkeypatch):
    when = _at(HALF, 12, 58)
    pos = _position(when)
    pos["pending_flatten_close"]["placed_at"] = when.astimezone(timezone.utc).isoformat()
    rec = _arm(monkeypatch, when, pos)
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("OPEN", 0, 4)])

    asyncio.run(main_bot._trail_ratchet_one("SPY", dict(rec["book"].pos)))
    asyncio.run(main_bot._trail_pass())
    assert "eyes" not in rec["calls"]
    assert "stop" not in rec["calls"]
    assert "native" not in rec["calls"]
    assert "handoff" not in rec["calls"]
    assert "rearm" not in rec["calls"]
    assert rec["calls"].count("place") == 0


def test_learning_tolerates_the_flatten_close_reason_and_skips_unknown_prices():
    priced = {
        "ticker": "IWM",
        "setup": "orb",
        "realized_pnl_pct": 2.5,
        "exit": 1.5,
        "reason": "flatten_close_partial",
        "basis": "premium",
    }
    unknown = {
        "ticker": "SPY",
        "setup": "orb",
        "realized_pnl_pct": 0.0,
        "exit": None,
        "price_unknown": True,
        "needs_reconcile_trueup": True,
        "reason": "flatten_close_partial",
        "basis": "premium",
    }
    records = [
        {"event": "position_opened", "ts": "2026-11-27T15:00:00+00:00",
         "data": {"ticker": "SPY", "setup": "orb"}},
        {"event": "position_closed", "ts": "2026-11-27T17:50:00+00:00", "data": unknown},
        {"event": "flatten_partial_unpriced", "ts": "2026-11-27T17:51:00+00:00",
         "data": {**unknown, "ticker": "QQQ", "realized_pnl_pct": -100.0, "reason": "not_a_known_reason"}},
        {"event": "position_opened", "ts": "2026-11-27T15:00:00+00:00",
         "data": {"ticker": "IWM", "setup": "orb"}},
        {"event": "position_closed", "ts": "2026-11-27T17:55:00+00:00", "data": priced},
    ]
    trades = main_bot.edge.edge_trades(records)
    assert [t["ticker"] for t in trades] == ["IWM"]
    assert all(t["ret"] != 0.0 for t in trades)
    snapshot = main_bot.learning.learning_state(trades, {}, 1_800_000_000.0)
    report = main_bot.learning.learning_report(snapshot)
    assert report["live_closes"] == 1
    stats = main_bot.summarize_setup_performance(records)
    assert stats["orb"]["trades"] == 1
    assert stats["orb"]["wins"] == 1
    label = main_bot._CLOSE_REASON_LABELS.get("flatten_close_partial", "flatten_close_partial")
    assert "true-up" in label
    assert main_bot._CLOSE_REASON_LABELS.get("not_a_known_reason", "not_a_known_reason") == "not_a_known_reason"


_REAL_FLATTEN_READ = main_bot._flatten_read_order


class _Miss(Exception):
    status_code = 404


def _broker_order(order_id, status, *, symbol="SPY", price_type="MARKET",
                  action="SELL_CLOSE", filled=0, ordered=4) -> dict:
    return {
        "orderId": order_id,
        "OrderDetail": [{
            "status": status,
            "priceType": price_type,
            "Instrument": [{
                "orderAction": action,
                "filledQuantity": filled,
                "orderedQuantity": ordered,
                "Product": {"symbol": symbol, "securityType": "OPTN"},
            }],
        }],
    }


def _wire_client(monkeypatch, client) -> None:
    monkeypatch.setattr(main_bot, "_flatten_read_order", _REAL_FLATTEN_READ)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {"oauth_token": "t", "oauth_token_secret": "s"})

    async def account_key(_tokens):
        return "ACCT"

    async def direct(fn, *args, source="etrade", idempotent=True, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account_key)
    monkeypatch.setattr(main_bot, "_orders_client", lambda _tokens: client)
    monkeypatch.setattr(main_bot, "_etrade_call", direct)


def test_not_found_on_the_first_page_is_found_on_the_next_page(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    pages = {"p2": 0}

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            if marker is None:
                return {
                    "OrdersResponse": {
                        "marker": "p2",
                        "Order": [_broker_order("OTHER", "EXECUTED", symbol="QQQ")],
                    },
                }
            assert marker == "p2"
            pages["p2"] += 1
            status = "OPEN" if pages["p2"] == 1 else "CANCELLED"
            return {"OrdersResponse": {"Order": [_broker_order("CLOSE-1", status)]}}

        def list_order_details(self, *args, **kwargs):
            raise AssertionError("by-id lookup is not needed when a later page has the order")

    _wire_client(monkeypatch, Client())
    _run()
    assert pages["p2"] >= 2
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)
    assert "stop" not in rec["calls"]


def test_order_missing_from_every_page_is_found_by_id(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    detail_reads = {"n": 0}

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            return {"OrdersResponse": {"Order": [_broker_order("OTHER", "OPEN", symbol="QQQ")]}}

        def list_order_details(self, account_id_key, order_id, resp_format="json"):
            assert str(order_id) == "CLOSE-1"
            detail_reads["n"] += 1
            status = "OPEN" if detail_reads["n"] == 1 else "CANCELLED"
            return {"OrdersResponse": {"Order": [_broker_order("CLOSE-1", status)]}}

    _wire_client(monkeypatch, Client())
    _run()
    assert detail_reads["n"] >= 2
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)


def test_truly_not_found_after_paging_and_by_id_alerts_only(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when))
    seen = {"pages": 0, "details": 0}

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            seen["pages"] += 1
            assert marker is None
            return {"OrdersResponse": {"Order": [_broker_order("OTHER", "OPEN", symbol="QQQ")]}}

        def list_order_details(self, account_id_key, order_id, resp_format="json"):
            seen["details"] += 1
            raise _Miss("404 Not Found")

    _wire_client(monkeypatch, Client())
    _run()
    assert seen["pages"] >= 1
    assert seen["details"] == 1
    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    assert any("NOT_FOUND" in a["message"] for a in rec["alerts"])
