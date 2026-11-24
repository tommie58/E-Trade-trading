"""DONE_TRADE_EXECUTED is a filled terminal status for entries and stops.

E*TRADE lists it beside EXECUTED on OrderDetail.status (and as an order
event for a completed trade). Flatten closes already treat that status as
filled. Entry, stop, and emergency-flatten orders read the same field.
"""
from __future__ import annotations

import asyncio

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
