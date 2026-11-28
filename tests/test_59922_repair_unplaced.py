"""An unplaced re-arm after a repair cancel is a critical alert, not a re-arm."""
from __future__ import annotations

import asyncio
import logging

import main_bot


def _run(coro):
    return asyncio.run(coro)


def test_unplaced_repair_rearm_pages_and_is_not_logged_as_rearmed(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    sent = []

    async def offset(_order_id):
        return 7.6

    async def cancel(order_id, expected_qty=None):
        return main_bot.CANCEL_CONFIRMED

    async def arm(*args, **kwargs):
        raise main_bot.NativeTrailRefused("repair refused")

    async def rearm(ticker, pos, floor_price=None):
        return "unplaced"

    async def send_nowait(severity, event, message, **kwargs):
        sent.append((severity, event))

    async def send(*args, **kwargs):
        raise AssertionError("repair unplaced awaited alerts.send")

    monkeypatch.setattr(main_bot, "_read_resting_trail_offset", offset)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_arm_native_trail", arm)
    monkeypatch.setattr(main_bot, "_reconcile_rearm_guard", rearm)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    monkeypatch.setattr(main_bot.alerts, "send", send)

    async def fetch(right, stop):
        return {"OrdersResponse": {"Order": {
            "orderId": "1598",
            "OrderDetail": {
                "status": "OPEN",
                "priceType": "TRAILING_STOP_CNST",
                "stopPrice": stop,
                "offsetValue": 7.6,
                "Instrument": {
                    "orderAction": "SELL_CLOSE",
                    "orderedQuantity": 1,
                    "filledQuantity": 0,
                    "Product": {
                        "symbol": "QQQ",
                        "securityType": "OPTN",
                        "callPut": right,
                        "strikePrice": 450,
                        "expiryYear": 2026,
                        "expiryMonth": 12,
                        "expiryDay": 18,
                    },
                },
            },
        }}}

    async def scenario(right):
        sent.clear()
        pos = {
            "action": "BUY",
            "entry_premium": 3.0,
            "stop_premium": 2.4,
            "stop_order_id": None,
            "native_trail_order_id": "1598",
            "filled_qty": 1,
            "qty": 1,
            "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
        }
        await store.set_position("QQQ", pos)

        async def orders():
            return await fetch(right, 2.50)

        monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", orders)
        await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 4.00, 0.10, "1598",
        )
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        if pending:
            await asyncio.gather(*pending)
        assert ("critical", "position_unprotected") in sent

    for right in ("CALL", "PUT"):
        _run(scenario(right))

    notes = [rec.getMessage() for rec in caplog.records if "did not place" in rec.getMessage()]
    assert notes
    assert all("re-armed" not in note for note in notes)
