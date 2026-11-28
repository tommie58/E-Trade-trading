"""A failed pre-cancel stop read refuses a native-trail promotion or repair."""
from __future__ import annotations

import asyncio

import pytest

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _option(right: str) -> dict:
    return {
        "action": "BUY",
        "entry_premium": 3.0,
        "stop_premium": 2.4,
        "stop_order_id": "STOP-1",
        "filled_qty": 1,
        "qty": 1,
        "breakeven_locked": True,
        "contract": {"right": right, "strike": 450.0, "expiration": "2026-12-18"},
    }


def test_promotion_and_repair_keep_the_stop_when_the_read_fails(monkeypatch):
    store = main_bot.StateStore(None)
    cancelled = []

    async def read(order_id):
        raise RuntimeError("book down")

    async def cancel(order_id, expected_qty=None):
        cancelled.append(order_id)
        return main_bot.CANCEL_CONFIRMED

    async def quote(ticker, contract=None):
        return 4.00, 4.10

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    async def offset(_order_id):
        return 7.6

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_read_open_order_stop", read)
    monkeypatch.setattr(main_bot, "_cancel_order_verified", cancel)
    monkeypatch.setattr(main_bot, "_current_bid_ask", quote)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot, "_read_resting_trail_offset", offset)

    async def drain():
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        if pending:
            await asyncio.gather(*pending)

    async def promotion(right):
        cancelled.clear()
        pos = _option(right)
        await store.set_position("QQQ", pos)
        with pytest.raises(main_bot.NativeTrailRefused, match="could not be read"):
            await main_bot._arm_native_trail(
                "QQQ", min_trigger=2.4, source="auto",
            )
        await drain()
        assert cancelled == []

    async def repair(right):
        cancelled.clear()
        pos = _option(right)
        pos["stop_order_id"] = None
        pos["native_trail_order_id"] = "TRAIL-1"
        await store.set_position("QQQ", pos)
        owned = await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 4.00, 0.10, "TRAIL-1",
        )
        await drain()
        assert owned is True
        assert cancelled == []

    async def pages():
        recent = await main_bot.alerts.recent(10)
        return [row for row in recent if row.get("event") == "native_trail_stop_unreadable"]

    async def scenario():
        main_bot.alerts.init(store, None)
        for right in ("CALL", "PUT"):
            await promotion(right)
            await repair(right)
        first = await pages()
        assert len(first) == 1
        assert first[0]["severity"] == "error"
        pos = _option("CALL")
        pos["stop_order_id"] = None
        pos["native_trail_order_id"] = "TRAIL-1"
        await main_bot._maybe_repair_inflated_native_trail(
            "QQQ", pos, 4.00, 0.10, "TRAIL-1",
        )
        await drain()
        assert len(await pages()) == 1

    _run(scenario())
