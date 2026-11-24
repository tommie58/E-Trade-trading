"""Unfilled flatten close: replace with one MARKET sell from close-10.

Broker calls are mocked. The clock is a frozen America/New_York datetime.
This path never places a stop. Nothing runs at or after the bell.
"""
import asyncio
import copy
import logging
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
        if isinstance(held, list):
            if not held:
                return None
            return held.pop(0)
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
    rec = _arm(monkeypatch, when, pos, held=4)
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
    rec = _arm(monkeypatch, when, _position(when, expiration="2026-11-27"), held=4)
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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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


def _no_cancel(calls) -> bool:
    return not any(isinstance(c, tuple) and c[0] == "cancel" for c in calls)


def test_session_cap_leaves_the_working_close_in_place(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when)
    pos["pending_flatten_close"]["replacements"] = 3
    rec = _arm(monkeypatch, when, pos, held=4)
    rec["scripts"]["CLOSE-1"].append(_row("OPEN", 0, 4))
    _run()
    assert _no_cancel(rec["calls"])
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert any("cap is reached" in a["message"] and a["severity"] == "critical" for a in rec["alerts"])
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"
    assert rec["book"].pos["pending_flatten_close"].get("resolved") != "flat"


def test_reject_backoff_leaves_the_working_close_in_place(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when)
    pos["pending_flatten_close"]["backoff_ticks"] = 2
    pos["pending_flatten_close"]["backoff_for"] = "CLOSE-1"
    rec = _arm(monkeypatch, when, pos, held=4)
    rec["scripts"]["CLOSE-1"].append(_row("OPEN", 0, 4))
    _run()
    assert _no_cancel(rec["calls"])
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert any("waiting before" in a["message"] for a in rec["alerts"])
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_unknown_stop_state_does_not_cancel_the_working_close(monkeypatch):
    when = _at(HALF, 12, 50)
    pos = _position(when)
    pos["native_trail_order_id"] = "TRAIL-1"
    rec = _arm(monkeypatch, when, pos, held=4)
    rec["scripts"]["CLOSE-1"].append(_row("OPEN", 0, 4))
    rec["state"]["open_error"] = True
    _run()
    assert _no_cancel(rec["calls"])
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert any(
        a["severity"] == "critical" and "no cancel and no new order" in a["message"]
        for a in rec["alerts"]
    )
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_held_none_before_cancel_sends_nothing(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=None)
    rec["scripts"]["CLOSE-1"].append(_row("OPEN", 0, 4))
    _run()
    assert _no_cancel(rec["calls"])
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert any(
        a["severity"] == "critical" and "could not be read" in a["message"]
        for a in rec["alerts"]
    )
    assert rec["book"].pos["pending_flatten_close"].get("resolved") != "flat"


def test_held_none_after_cancel_does_not_sell_or_mark_flat(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=[4, None])
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("CANCELLED", 0, 4)])
    _run()
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    pending = rec["book"].pos["pending_flatten_close"]
    assert pending.get("resolved") != "flat"
    assert any(
        a["severity"] == "critical" and "not marked flat" in a["message"]
        for a in rec["alerts"]
    )


def test_confirmed_zero_holding_after_cancel_is_flat_with_no_sell(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=[4, 0])
    rec["scripts"]["CLOSE-1"].extend([_row("OPEN", 0, 4), _row("CANCELLED", 0, 4)])
    _run()
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 0
    assert rec["orders"] == []
    assert rec["book"].pos["pending_flatten_close"]["resolved"] == "flat"


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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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
    rec = _arm(monkeypatch, when, pos, held=4)
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
    rec = _arm(monkeypatch, when, pos, held=4)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["scripts"]["STOP-OLD"] = [_row("NOT_FOUND", 0, 0)]
    _run()
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)
    assert "stop" not in rec["calls"]


def test_reject_storm_stays_inside_the_cap_and_dedupes_alerts(monkeypatch):
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)
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
    rec = _arm(monkeypatch, when, _position(when), enabled=False, held=4)
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
_REAL_FLATTEN_OPEN = main_bot._flatten_open_orders


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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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
    rec = _arm(monkeypatch, when, _position(when), held=4)
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


_MISSING = object()


def _order_row(order_id, status=_MISSING, *, filled=0, ordered=4, extra_details=None):
    """One broker order. ``status`` omitted means the detail has no status key."""
    detail = {
        "Instrument": [{
            "orderAction": "SELL_CLOSE",
            "filledQuantity": filled,
            "orderedQuantity": ordered,
            "Product": {"symbol": "SPY", "securityType": "OPTN"},
        }],
    }
    if status is not _MISSING:
        detail["status"] = status
    details = [detail]
    if extra_details:
        details.extend(extra_details)
    return {"orderId": order_id, "OrderDetail": details}


def test_blank_status_is_unknown_and_a_named_fill_still_matches():
    """Missing status is not OPEN. An EXECUTED detail still reports its fill."""
    blank = _order_row("BLANK", filled=2, ordered=4)
    empty = _order_row("EMPTY", "", filled=1, ordered=4)
    spaces = _order_row("SPACE", "   ", filled=3, ordered=4)
    opened = _broker_order("OPEN-1", "OPEN", filled=0, ordered=4)
    filled = _order_row(
        "FILL-1", "", filled=1, ordered=2,
        extra_details=[{
            "status": "EXECUTED",
            "Instrument": [{"filledQuantity": 1, "orderedQuantity": 2}],
        }],
    )
    sibling = _order_row(
        "SIB", "EXECUTED", filled=2, ordered=2,
        extra_details=[{
            "status": "  ",
            "Instrument": [{"filledQuantity": 0, "orderedQuantity": 0}],
        }],
    )
    no_detail = {"orderId": "NONE"}

    blank_hit = main_bot._match_order_qty([blank], "BLANK")
    assert blank_hit == {"status": "UNKNOWN", "filled": 2, "ordered": 4}
    assert main_bot._match_order_qty([empty], "EMPTY")["status"] == "UNKNOWN"
    assert main_bot._match_order_qty([spaces], "SPACE")["status"] == "UNKNOWN"
    assert main_bot._match_order_qty([no_detail], "NONE")["status"] == "UNKNOWN"
    assert main_bot._match_order_qty([opened], "OPEN-1")["status"] == "OPEN"
    assert main_bot._flatten_close_view(blank_hit) == "unknown"
    assert main_bot._flatten_close_view(main_bot._match_order_qty([opened], "OPEN-1")) == "working"

    fill_hit = main_bot._match_order_qty([filled], "FILL-1")
    assert fill_hit["status"] == "EXECUTED"
    assert fill_hit["filled"] == 2
    sib_hit = main_bot._match_order_qty([sibling], "SIB")
    assert sib_hit["status"] == "EXECUTED"
    assert sib_hit["filled"] == 2
    assert main_bot.classify_cancel_status("EXECUTED", 2, 2) == "filled"
    assert main_bot.classify_cancel_status("OPEN", 0, 4) == "working"


def test_order_state_blank_status_is_unknown_and_executed_fill_counts(monkeypatch):
    rows = [
        _order_row("BLANK", filled=2, ordered=4),
        _order_row("FILL-1", "EXECUTED", filled=2, ordered=2),
        _broker_order("OPEN-1", "OPEN", filled=0, ordered=4),
        _order_row("EMPTY", ""),
    ]

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            return {"OrdersResponse": {"Order": rows}}

    _wire_client(monkeypatch, Client())

    async def read():
        return {
            "blank": await main_bot._order_state("BLANK", None),
            "fill": await main_bot._order_state("FILL-1", None),
            "open": await main_bot._order_state("OPEN-1", None),
            "empty": await main_bot._order_state("EMPTY", None),
            "missing": await main_bot._order_state("GONE", None),
        }

    got = asyncio.run(read())
    assert got["blank"] == ("UNKNOWN", 2)
    assert got["fill"] == ("EXECUTED", 2)
    assert got["open"] == ("OPEN", 0)
    assert got["empty"] == ("UNKNOWN", 0)
    assert got["missing"] == ("NOT_FOUND", 0)


@pytest.mark.parametrize("status", (_MISSING, "", "   "), ids=("missing", "empty", "blank"))
def test_statusless_flatten_close_is_not_cancelled_or_replaced(status, monkeypatch):
    when = _at(FULL, 15, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)
    order = _order_row("CLOSE-1", status, ordered=4)

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            return {"OrdersResponse": {"Order": [order]}}

        def list_order_details(self, *args, **kwargs):
            raise AssertionError("the page already returned the order")

    _wire_client(monkeypatch, Client())
    _run()
    _run()

    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    critical = [
        a for a in rec["alerts"]
        if a["severity"] == "critical" and a["event"] == "unfilled_flatten_replace"
    ]
    assert len(critical) == 1
    assert "UNKNOWN" in critical[0]["message"]
    assert "no cancel and no new order" in critical[0]["message"]
    assert critical[0]["dedupe_key"] == "unfilled_flatten:SPY:status_unknown"
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_explicit_open_status_is_still_cancelled_and_replaced(monkeypatch):
    when = _at(FULL, 15, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)
    reads = {"n": 0}

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            reads["n"] += 1
            status = "OPEN" if reads["n"] == 1 else "CANCELLED"
            return {"OrdersResponse": {"Order": [_broker_order("CLOSE-1", status)]}}

        def list_order_details(self, *args, **kwargs):
            raise AssertionError("the page already returned the order")

    _wire_client(monkeypatch, Client())
    _run()
    assert reads["n"] >= 2
    assert ("cancel", "CLOSE-1", 4) in rec["calls"]
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-2"


def test_concurrent_fill_survives_the_flatten_replace_save(monkeypatch):
    """A fill written after the snapshot and before the save stays on the book."""
    when = _at(FULL, 15, 55)
    rec = _arm(monkeypatch, when, _position(when), held=4)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))

    async def held_qty(ticker, pos):
        rec["calls"].append("held")
        current = await main_bot.state.get_position("SPY")
        current["entry_premium"] = 2.75
        current["filled_qty"] = 2
        current["qty"] = 2
        current["reconcile_stamp"] = "broker-fill"
        await main_bot.state.set_position("SPY", current)
        return 4

    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", held_qty)
    _run()
    saved = rec["book"].pos
    assert saved["entry_premium"] == 2.75
    assert saved["filled_qty"] == 2
    assert saved["qty"] == 2
    assert saved["reconcile_stamp"] == "broker-fill"
    assert saved["had_native_trail"] is True
    assert saved["pending_flatten_close"]["order_id"] == "CLOSE-2"
    assert saved["close_order_id"] == "CLOSE-2"
    assert rec["calls"].count("place") == 1
    _assert_market_sell(rec["orders"][0], 4)


def _protective_row(order_id, price_type, status):
    row = _order_row(order_id, status)
    row["OrderDetail"][0]["priceType"] = price_type
    row["OrderDetail"][0]["stopPrice"] = 1.0
    return row


@pytest.mark.parametrize(
    "price_type,status",
    [
        ("STOP", _MISSING),
        ("STOP", ""),
        ("STOP", "   "),
        ("TRAILING_STOP_CNST", _MISSING),
        ("TRAILING_STOP_CNST", ""),
    ],
    ids=("stop-missing", "stop-empty", "stop-blank", "trail-missing", "trail-empty"),
)
def test_blank_status_protective_order_is_not_cancelled(price_type, status, monkeypatch):
    """A blank stop on the flatten list is UNKNOWN, so nothing is cancelled."""
    when = _at(FULL, 15, 50)
    pos = _position(when)
    if price_type == "STOP":
        pos["stop_order_id"] = "STOP-9"
    else:
        pos["native_trail_order_id"] = "STOP-9"
    rec = _arm(monkeypatch, when, pos, held=4)

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            return {"OrdersResponse": {"Order": [
                _broker_order("CLOSE-1", "OPEN"),
                _protective_row("STOP-9", price_type, status),
            ]}}

        def list_order_details(self, *args, **kwargs):
            raise AssertionError("the page already returned both orders")

    _wire_client(monkeypatch, Client())
    monkeypatch.setattr(main_bot, "_flatten_open_orders", _REAL_FLATTEN_OPEN)
    _run()
    _run()

    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    critical = [
        a for a in rec["alerts"]
        if a["severity"] == "critical" and a["event"] == "unfilled_flatten_replace"
    ]
    assert len(critical) == 1
    assert "STOP-9" in critical[0]["message"]
    assert "UNKNOWN" in critical[0]["message"]
    assert "no cancel and no new order" in critical[0]["message"]
    assert critical[0]["dedupe_key"] == "unfilled_flatten:SPY:protection_unknown"
    saved = rec["book"].pos
    assert saved["pending_flatten_close"]["order_id"] == "CLOSE-1"
    if price_type == "STOP":
        assert saved["stop_order_id"] == "STOP-9"
    else:
        assert saved["native_trail_order_id"] == "STOP-9"


def test_reconciler_still_treats_a_blank_order_status_as_open():
    """The live-order check still counts a blank status as OPEN."""
    def resp(status):
        detail = {
            "priceType": "STOP",
            "stopPrice": 1.0,
            "Instrument": {
                "orderAction": "SELL_CLOSE",
                "orderedQuantity": 4,
                "filledQuantity": 0,
                "Product": {"symbol": "SPY", "securityType": "OPTN"},
            },
        }
        if status is not _MISSING:
            detail["status"] = status
        return {"OrdersResponse": {"Order": {"orderId": "STOP-9", "OrderDetail": detail}}}

    for status in (_MISSING, ""):
        orders = main_bot.reconciliation.parse_open_orders(resp(status))
        assert orders[0]["status"] == "OPEN"
        live = [
            order for order in orders
            if order.get("status") in main_bot.reconciliation._OPEN_ORDER_STATUSES
        ]
        assert [order["order_id"] for order in live] == ["STOP-9"]
        stops = main_bot.reconciliation.protective_stops_by_leg(live)
        assert stops[("SPY", True)][0]["order_id"] == "STOP-9"
        flatten = main_bot.reconciliation.parse_open_orders(resp(status), blank_as_open=False)
        assert flatten[0]["status"] == "UNKNOWN"
        assert flatten[0]["status"] not in main_bot.reconciliation._OPEN_ORDER_STATUSES

    named = main_bot.reconciliation.parse_open_orders(resp("open"), blank_as_open=False)
    assert named[0]["status"] == "OPEN"
    spaces = main_bot.reconciliation.parse_open_orders(resp("   "))
    assert spaces[0]["status"] == "   "
    assert main_bot.reconciliation.parse_open_orders(
        resp("   "), blank_as_open=False,
    )[0]["status"] == "UNKNOWN"


def _sibling_close(status):
    """A MARKET SELL_CLOSE whose status key may be missing or blank."""
    row = _order_row("CLOSE-2", status)
    row["OrderDetail"][0]["priceType"] = "MARKET"
    return row


@pytest.mark.parametrize("status", (_MISSING, ""), ids=("missing", "blank"))
@pytest.mark.parametrize(
    "close_status", ("OPEN", "EXPIRED"), ids=("replace", "dead"),
)
def test_blank_sibling_close_blocks_the_replacement(status, close_status, monkeypatch):
    """An unconfirmed sibling close blocks both the replace and the dead path."""
    when = _at(FULL, 15, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)

    class Client:
        def list_orders(self, account_id_key, resp_format="json", count=25, marker=None, **kwargs):
            assert marker is None
            return {"OrdersResponse": {"Order": [
                _broker_order("CLOSE-1", close_status),
                _sibling_close(status),
            ]}}

        def list_order_details(self, *args, **kwargs):
            raise AssertionError("the page already returned both closes")

    _wire_client(monkeypatch, Client())
    monkeypatch.setattr(main_bot, "_flatten_open_orders", _REAL_FLATTEN_OPEN)
    _run()
    _run()

    assert rec["calls"].count("place") == 0
    assert not any(isinstance(c, tuple) and c[0] == "cancel" for c in rec["calls"])
    assert len(rec["alerts"]) == 1
    alert = rec["alerts"][0]
    assert alert["severity"] == "critical"
    assert alert["event"] == "unfilled_flatten_replace"
    assert "CLOSE-2" in alert["message"]
    assert "UNKNOWN" in alert["message"]
    assert "no cancel and no new order" in alert["message"]
    assert alert["dedupe_key"] == "unfilled_flatten:SPY:protection_unknown"
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-1"


def test_done_trade_executed_is_filled_and_does_not_send_another_close(monkeypatch):
    """E*TRADE reports a completed close as DONE_TRADE_EXECUTED. That is a fill."""
    when = _at(FULL, 15, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)
    rec["scripts"]["CLOSE-1"].append(_row("DONE_TRADE_EXECUTED", 0, 4))
    rec["state"]["fill"] = 1.40
    booked = []

    async def record_close(ticker, exit_price, payload, exit_premium=None, close_reason=None, **kwargs):
        booked.append((ticker, exit_premium, close_reason))
        rec["book"].pos = None

    monkeypatch.setattr(main_bot, "_record_close", record_close)
    assert main_bot._flatten_close_view(_row("DONE_TRADE_EXECUTED", 0, 4)) == "filled"
    assert main_bot._flatten_close_view(_row("done_trade_executed", 4, 4)) == "filled"
    _run()
    assert rec["calls"].count("place") == 0
    assert _no_cancel(rec["calls"])
    assert rec["orders"] == []
    assert booked == [("SPY", 1.40, "eod_flatten")]
    assert "stop" not in rec["calls"]


def test_lock_busy_after_replacement_sent_keeps_the_new_close(monkeypatch):
    """A busy position lock after the market sell must not drop the new order id."""
    when = _at(HALF, 12, 50)
    rec = _arm(monkeypatch, when, _position(when), held=4)
    rec["scripts"]["CLOSE-1"].append(_row("EXPIRED", 0, 4))
    rec["scripts"]["CLOSE-2"] = [_row("OPEN", 0, 4)]

    class _Busy:
        async def acquire(self):
            raise main_bot.LockNotAcquired("busy")

        async def release(self):
            return None

    rec["book"].position_lock = lambda *args, **kwargs: _Busy()
    _run()
    pending = rec["book"].pos["pending_flatten_close"]
    assert rec["calls"].count("place") == 1
    assert pending["order_id"] == "CLOSE-2"
    assert pending["replacements"] == 1
    assert rec["book"].pos["close_order_id"] == "CLOSE-2"

    _run()
    assert rec["calls"].count("place") == 1
    assert rec["book"].pos["pending_flatten_close"]["order_id"] == "CLOSE-2"


def test_concurrent_flatten_saves_keep_each_others_fields():
    """Two passes write different keys. Neither snapshot replaces the other."""
    store = main_bot.StateStore(None)

    async def scenario():
        await store.set_position("SPY", {
            "qty": 4,
            "note": "keep",
            "alpha": 0,
            "beta": 0,
            "_rev": 1,
        })
        overlap = {"n": 0, "max": 0}
        original = store.get_position

        async def slow_get(ticker):
            overlap["n"] += 1
            overlap["max"] = max(overlap["max"], overlap["n"])
            try:
                await asyncio.sleep(0.05)
                return await original(ticker)
            finally:
                overlap["n"] -= 1

        store.get_position = slow_get
        saved = main_bot.state
        main_bot.state = store
        try:
            async def edit(key, value):
                pos = await original("SPY")
                token = main_bot._flatten_bind_baseline(copy.deepcopy(pos))
                try:
                    working = copy.deepcopy(pos)
                    working[key] = value
                    await main_bot._flatten_save("SPY", working)
                finally:
                    main_bot._FLATTEN_BASELINE.reset(token)

            await asyncio.gather(edit("alpha", 1), edit("beta", 2))
            final = await original("SPY")
        finally:
            main_bot.state = saved
        return final, overlap

    final, overlap = asyncio.run(scenario())
    assert overlap["max"] == 1
    assert final["alpha"] == 1
    assert final["beta"] == 2
    assert final["note"] == "keep"
    assert final["qty"] == 4


def test_stale_position_write_cannot_clear_a_newer_flatten_close():
    """An older whole-position copy must not erase a close id written later."""

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", {
            "qty": 4,
            "entry": 1.25,
            "trail": 1.0,
            "pending_flatten_close": {"order_id": "CLOSE-NEW", "replacements": 1},
            "close_order_id": "CLOSE-NEW",
        })
        current = await store.get_position("SPY")
        stale = {
            "qty": 4,
            "entry": 1.25,
            "trail": 2.5,
            "_rev": int(current["_rev"]) - 1,
        }
        await store.set_position("SPY", stale)
        kept = await store.get_position("SPY")
        older = {
            "qty": 4,
            "entry": 1.25,
            "trail": 2.5,
            "close_order_id": "CLOSE-OLD",
            "pending_flatten_close": {"order_id": "CLOSE-OLD"},
            "_rev": int(kept["_rev"]) - 1,
        }
        await store.set_position("SPY", older)
        still = await store.get_position("SPY")
        fresh = dict(still)
        fresh["close_order_id"] = "CLOSE-2"
        fresh["pending_flatten_close"] = {"order_id": "CLOSE-2"}
        await store.set_position("SPY", fresh)
        replaced = await store.get_position("SPY")
        cleared = dict(replaced)
        cleared.pop("close_order_id")
        cleared.pop("pending_flatten_close")
        await store.set_position("SPY", cleared)
        current = await store.get_position("SPY")
        resurrect = dict(current)
        resurrect["close_order_id"] = "CLOSE-OLD"
        resurrect["pending_flatten_close"] = {"order_id": "CLOSE-OLD"}
        resurrect["_rev"] = int(current["_rev"]) - 1
        await store.set_position("SPY", resurrect)
        gone = await store.get_position("SPY")
        return kept, still, replaced, gone

    kept, still, replaced, gone = asyncio.run(scenario())
    assert kept["close_order_id"] == "CLOSE-NEW"
    assert kept["pending_flatten_close"]["order_id"] == "CLOSE-NEW"
    assert kept["trail"] == 2.5
    assert still["close_order_id"] == "CLOSE-NEW"
    assert still["pending_flatten_close"]["order_id"] == "CLOSE-NEW"
    assert replaced["close_order_id"] == "CLOSE-2"
    assert "close_order_id" not in gone
    assert "pending_flatten_close" not in gone


def test_flatten_save_waits_two_seconds_unless_a_sell_was_sent():
    waits = []

    class _Busy:
        async def acquire(self):
            raise main_bot.LockNotAcquired("busy")

        async def release(self):
            return None

    def lock(ticker, ttl_ms=120_000, wait_timeout=30.0):
        waits.append(wait_timeout)
        return _Busy()

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", {"qty": 4, "note": "keep"})
        store.position_lock = lock
        saved_state = main_bot.state
        main_bot.state = store
        try:
            pos = await store.get_position("SPY")
            token = main_bot._flatten_bind_baseline(copy.deepcopy(pos))
            try:
                working = copy.deepcopy(pos)
                working["note"] = "bookkeeping"
                await main_bot._flatten_save("SPY", working)
            finally:
                main_bot._FLATTEN_BASELINE.reset(token)
            untouched = await store.get_position("SPY")
            placed = main_bot._FLATTEN_ORDER_PLACED.set(True)
            try:
                token = main_bot._flatten_bind_baseline(copy.deepcopy(untouched))
                try:
                    working = copy.deepcopy(untouched)
                    working["close_order_id"] = "CLOSE-9"
                    working["pending_flatten_close"] = {"order_id": "CLOSE-9"}
                    working["note"] = "sold"
                    await main_bot._flatten_save("SPY", working)
                finally:
                    main_bot._FLATTEN_BASELINE.reset(token)
            finally:
                main_bot._FLATTEN_ORDER_PLACED.reset(placed)
            saved = await store.get_position("SPY")
        finally:
            main_bot.state = saved_state
        return untouched, saved

    untouched, saved = asyncio.run(scenario())
    assert waits == [
        main_bot.FLATTEN_SAVE_NO_ORDER_WAIT_S,
        main_bot.FLATTEN_SAVE_AFTER_SELL_WAIT_S,
    ]
    assert main_bot.FLATTEN_SAVE_NO_ORDER_WAIT_S == 2.0
    assert main_bot.FLATTEN_SAVE_AFTER_SELL_WAIT_S == 30.0
    assert untouched["note"] == "keep"
    assert "close_order_id" not in untouched
    assert saved["close_order_id"] == "CLOSE-9"
    assert saved["pending_flatten_close"]["order_id"] == "CLOSE-9"
    assert saved["note"] == "sold"


def test_unsaved_close_id_retries_once_then_alerts(monkeypatch, caplog):
    calls = {"n": 0}
    sent = []

    class _Held:
        async def acquire(self):
            return None

        async def release(self):
            return None

    async def boom(ticker, delta):
        calls["n"] += 1
        raise RuntimeError("store down")

    async def fake_send(severity, event, message, **kwargs):
        sent.append({"severity": severity, "event": event, "message": message})

    monkeypatch.setattr(main_bot, "_flatten_merge_changed", boom)
    monkeypatch.setattr(main_bot.alerts, "send", fake_send)

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", {
            "qty": 4,
            "accountId": ACCOUNT,
            "accountIdKey": ACCOUNT,
        })
        store.position_lock = lambda *args, **kwargs: _Held()
        saved = main_bot.state
        main_bot.state = store
        try:
            pos = await store.get_position("SPY")
            token = main_bot._flatten_bind_baseline(copy.deepcopy(pos))
            placed = main_bot._FLATTEN_ORDER_PLACED.set(True)
            try:
                working = copy.deepcopy(pos)
                working["close_order_id"] = "CLOSE-9"
                working["pending_flatten_close"] = {"order_id": "CLOSE-9"}
                await main_bot._flatten_save("SPY", working)
            finally:
                main_bot._FLATTEN_ORDER_PLACED.reset(placed)
                main_bot._FLATTEN_BASELINE.reset(token)
        finally:
            main_bot.state = saved

    caplog.set_level(logging.ERROR, logger="etrade-bot")
    asyncio.run(scenario())
    assert calls["n"] == 2
    assert len(sent) == 1
    assert sent[0]["severity"] == "error"
    assert sent[0]["event"] == "flatten_close_unsaved"
    assert sent[0]["message"] == "SPY: close sent, order id not saved (CLOSE-9)"
    assert ACCOUNT not in sent[0]["message"]
    assert "close sent, order id not saved" in caplog.text
    assert "SPY" in caplog.text
    assert "CLOSE-9" in caplog.text
    assert ACCOUNT not in caplog.text


def test_close_id_retry_that_lands_does_not_alert(monkeypatch):
    calls = {"n": 0}
    sent = []
    original = main_bot._flatten_merge_changed

    class _Held:
        async def acquire(self):
            return None

        async def release(self):
            return None

    async def flaky(ticker, delta):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("once")
        await original(ticker, delta)

    async def fake_send(*args, **kwargs):
        sent.append(args)

    monkeypatch.setattr(main_bot, "_flatten_merge_changed", flaky)
    monkeypatch.setattr(main_bot.alerts, "send", fake_send)

    async def scenario():
        store = main_bot.StateStore(None)
        await store.set_position("SPY", {"qty": 4, "note": "keep"})
        store.position_lock = lambda *args, **kwargs: _Held()
        saved_state = main_bot.state
        main_bot.state = store
        try:
            pos = await store.get_position("SPY")
            token = main_bot._flatten_bind_baseline(copy.deepcopy(pos))
            placed = main_bot._FLATTEN_ORDER_PLACED.set(True)
            try:
                working = copy.deepcopy(pos)
                working["close_order_id"] = "CLOSE-9"
                working["pending_flatten_close"] = {"order_id": "CLOSE-9"}
                await main_bot._flatten_save("SPY", working)
            finally:
                main_bot._FLATTEN_ORDER_PLACED.reset(placed)
                main_bot._FLATTEN_BASELINE.reset(token)
            return await store.get_position("SPY")
        finally:
            main_bot.state = saved_state

    saved = asyncio.run(scenario())
    assert calls["n"] == 2
    assert sent == []
    assert saved["close_order_id"] == "CLOSE-9"
    assert saved["note"] == "keep"
