"""DONE_TRADE_EXECUTED is a filled terminal status for entries and stops.

E*TRADE's official Order API lists DONE_TRADE_EXECUTED only as an order
event name. Client enums also put it on OrderDetail.status. What
filledQuantity means beside that status is unverified. The guard still
treats the status as filled: it does not cancel or delete the position,
and it sizes a protective stop from the broker holding.
"""
from __future__ import annotations

import asyncio
import time

import main_bot


def _run(coro):
    return asyncio.run(coro)


def test_done_trade_executed_is_filled_and_terminal():
    assert "DONE_TRADE_EXECUTED" in main_bot._TERMINAL_ORDER_STATUSES
    assert "DONE_TRADE_EXECUTED" in main_bot._FILLED_ORDER_STATUSES
    assert main_bot._FILLED_ORDER_STATUSES is main_bot._FLATTEN_FILLED_STATUSES
    assert main_bot._filled_order_status("done_trade_executed") is True
    assert main_bot._emergency_flatten_reuse("DONE_TRADE_EXECUTED", 0) is True
    assert main_bot._emergency_flatten_reuse("EXECUTED", 0) is True
    assert main_bot._emergency_flatten_reuse("CANCELLED", 0) is False
    assert main_bot._emergency_flatten_reuse("OPEN", 0) is True
    assert main_bot._entry_terminal_unfilled("DONE_TRADE_EXECUTED", 0) is False
    assert main_bot._entry_terminal_unfilled("CANCELLED", 0) is True
    assert main_bot._entry_terminal_unfilled("EXECUTED", 0) is False
    assert main_bot._entry_terminal_unfilled("EXPIRED", 2) is False


def test_entry_chase_holds_a_done_trade_executed_order():
    held = main_bot.plan_entry_chase(
        1.0, 3.0, 0, 2, 120, 30, 0, "DONE_TRADE_EXECUTED",
    )
    filled = main_bot.plan_entry_chase(1.0, 3.0, 0, 2, 120, 30, 0, "EXECUTED")
    chasing = main_bot.plan_entry_chase(1.0, 3.0, 0, 2, 120, 30, 0, "OPEN")
    assert held["action"] == "hold"
    assert filled["action"] == "hold"
    assert chasing["action"] == "chase"


def test_cancel_classification_treats_done_trade_executed_as_filled():
    assert main_bot.classify_cancel_status("DONE_TRADE_EXECUTED", 0, 4) == "filled"
    assert main_bot.classify_cancel_status("DONE_TRADE_EXECUTED", 1, 4) == "partial"
    assert main_bot.classify_cancel_status("OPEN", 0, 4) == "working"


def test_resting_trail_offset_ignores_a_completed_order(monkeypatch):
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"OrdersResponse": {"Order": [{
            "orderId": "TRAIL-1",
            "OrderDetail": [{
                "status": "DONE_TRADE_EXECUTED",
                "priceType": "TRAILING_STOP_CNST",
                "offsetValue": 0.20,
                "Instrument": [{
                    "filledQuantity": 1,
                    "orderedQuantity": 1,
                    "Product": {"symbol": "SPY"},
                }],
            }],
        }]}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    offset = _run(main_bot._read_resting_trail_offset("TRAIL-1"))
    assert offset is None
    assert calls["n"] == 1


def test_exit_fill_reads_a_done_trade_executed_stop(monkeypatch):
    async def fetch():
        return {"OrdersResponse": {"Order": [{
            "orderId": "STOP-1",
            "OrderDetail": [{
                "status": "DONE_TRADE_EXECUTED",
                "priceType": "STOP",
                "Instrument": [{
                    "filledQuantity": 2,
                    "orderedQuantity": 2,
                    "averageExecutionPrice": 1.55,
                    "Product": {"symbol": "SPY"},
                }],
            }],
        }]}}

    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    price = _run(main_bot._resolve_exit_fill("STOP-1", timeout_s=0))
    assert price == 1.55


def test_ghost_resolution_books_a_done_trade_executed_stop():
    order = {
        "order_id": "STOP-1",
        "symbol": "SPY",
        "status": "DONE_TRADE_EXECUTED",
        "order_action": "SELL_CLOSE",
        "filled": 2,
        "exec_price": 1.55,
        "executed_ts": 1_700_000_000.0,
        "price_type": "STOP",
        "stop_price": 1.40,
    }
    pos = {"contract": {"right": "CALL"}, "stop_order_id": "STOP-1"}
    hit = main_bot.reconciliation.plan_ghost_resolution("SPY", pos, [order])
    assert hit["kind"] == "closed"
    assert hit["exit_price"] == 1.55
    assert hit["source"] == "tracked_stop"
    miss = main_bot.reconciliation.plan_ghost_resolution(
        "SPY", pos, [{**order, "status": "OPEN"}],
    )
    assert miss["kind"] != "closed"


def test_unreadable_fill_qty_prefers_the_broker_then_the_guard(monkeypatch):
    async def broker(_ticker, _pos):
        return 4

    async def unread(_ticker, _pos):
        return None

    async def flat(_ticker, _pos):
        return 0

    async def scenario(fetch):
        store = main_bot.StateStore(None)
        saved = main_bot.state
        main_bot.state = store
        monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", fetch)
        try:
            await store.set_position("QQQ", {"qty": 2, "filled_qty": 2})
            return await main_bot._protective_qty_for_unreadable_fill(
                "QQQ", {"qty": 9, "contract": {"right": "CALL"}},
            )
        finally:
            main_bot.state = saved

    assert _run(scenario(broker)) == 4
    assert _run(scenario(unread)) == 9
    assert _run(scenario(flat)) == 0


def test_done_trade_executed_with_zero_qty_places_a_stop_and_stays_tracked(monkeypatch):
    """Worker loop: filled status, filledQuantity 0, both deadlines already past."""
    placed = []
    alerts = []
    cancelled = []
    abandoned = []

    async def order_state(_order_id, _client_id):
        return "DONE_TRADE_EXECUTED", 0

    async def broker(_ticker, _pos):
        return 4

    async def place_stop(ticker, contract, qty, stop_premium):
        placed.append({"ticker": ticker, "qty": qty, "stop": stop_premium})
        return {"order_id": "STOP-NEW", "client_id": "cid", "qty": qty, "stop": stop_premium}

    async def send(severity, event, message, **_kwargs):
        alerts.append({"severity": severity, "event": event, "message": message})

    async def record(event, data=None, **_kwargs):
        if event == "entry_abandoned":
            abandoned.append(data)

    async def cancel(_order_id):
        cancelled.append(_order_id)

    async def no_true_up(_ticker, _guard):
        return False

    async def no_flatten(*_args, **_kwargs):
        raise AssertionError("emergency flatten ran on a filled entry")

    monkeypatch.setattr(main_bot, "_order_state", order_state)
    monkeypatch.setattr(main_bot, "_flatten_broker_held_qty", broker)
    monkeypatch.setattr(main_bot, "_place_option_protective_stop", place_stop)
    monkeypatch.setattr(main_bot, "_true_up_entry_fill", no_true_up)
    monkeypatch.setattr(main_bot, "_cancel_order_safe", cancel)
    monkeypatch.setattr(main_bot, "_emergency_flatten", no_flatten)
    monkeypatch.setattr(main_bot.alerts, "send", send)
    monkeypatch.setattr(main_bot.trade_ledger, "record", record)

    async def scenario():
        store = main_bot.StateStore(None)
        saved = main_bot.state
        main_bot.state = store
        try:
            past = time.time() - 30
            await store.set_position("QQQ", {
                "qty": 2,
                "filled_qty": 0,
                "action": "BUY",
                "contract": {"right": "CALL", "strike": 480, "expiration": "2026-11-24"},
            })
            await store.set_guard("QQQ", {
                "ticker": "QQQ",
                "kind": "option",
                "action": "BUY",
                "stop": 1.25,
                "contract": {"right": "CALL", "strike": 480, "expiration": "2026-11-24"},
                "entry_order_id": "ENTRY-1",
                "entry_client_id": "entry-cid",
                "guarded_qty": 0,
                "last_filled": 0,
                "last_status": "OPEN",
                "stop_order_id": None,
                "qty": 9,
                "done": False,
                "deadline_ts": past,
                "stop_deadline_ts": past,
                "fill_detected_ts": None,
                "placed_ts": past,
                "armed_ts": past,
            })
            await main_bot._stop_guard_worker("QQQ")
            return await store.get_position("QQQ"), await store.get_guard("QQQ")
        finally:
            main_bot.state = saved

    pos, guard = _run(scenario())
    assert placed == [{"ticker": "QQQ", "qty": 4, "stop": 1.25}]
    assert pos is not None
    assert pos["stop_order_id"] == "STOP-NEW"
    assert pos["filled_qty"] == 4
    assert guard["done"] is True
    assert guard["result"] == "filled_and_protected"
    critical = [a for a in alerts if a["event"] == "entry_fill_qty_unreadable"]
    assert len(critical) == 1
    assert critical[0]["severity"] == "critical"
    assert "QQQ" in critical[0]["message"]
    assert cancelled == []
    assert abandoned == []
