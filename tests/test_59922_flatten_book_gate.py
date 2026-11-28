"""Panic and daily-loss sell when list_orders fails.

The stop-guard deadline flatten still refuses that sell.
"""
from __future__ import annotations

import asyncio

import pytest

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _install(monkeypatch, store):
    sold = []

    async def fetch():
        raise RuntimeError("orders down")

    async def smart(kind, common, tokens):
        sold.append(int(common.get("quantity") or 0))
        return {"orderId": "MKT-1"}

    def tokens():
        return {"oauth_token": "t", "oauth_token_secret": "s"}

    async def account(_tokens):
        return "acct"

    async def quiet(*args, **kwargs):
        return None

    async def send_nowait(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_reconcile_fetch_orders", fetch)
    monkeypatch.setattr(main_bot, "_place_order_smart", smart)
    monkeypatch.setattr(main_bot, "load_tokens", tokens)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account)
    monkeypatch.setattr(main_bot.trade_ledger, "record", quiet)
    monkeypatch.setattr(main_bot, "_defer_close_to_reconciler", quiet)
    monkeypatch.setattr(main_bot.alerts, "send_nowait", send_nowait)
    return sold


def test_panic_and_daily_loss_sell_when_the_order_book_fails(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def scenario(trigger):
        await store.set_position("QQQ", {
            "action": "BUY", "entry": 100.0, "stop": 90.0,
            "filled_qty": 100, "qty": 100,
        })
        await store.set_guard("QQQ", {
            "done": False, "kind": "equity", "action": "BUY", "stop": 90.0,
        })
        await main_bot._flatten_book(trigger)
        return list(sold)

    assert _run(scenario("panic")) == [100]
    sold.clear()
    assert _run(scenario("daily_loss")) == [100]


def test_deadline_flatten_does_not_sell_when_the_order_book_fails(monkeypatch):
    store = main_bot.StateStore(None)
    sold = _install(monkeypatch, store)

    async def scenario():
        with pytest.raises(Exception, match="RuntimeError"):
            await main_bot._emergency_flatten(
                "QQQ", {"kind": "equity", "action": "BUY"}, 100,
            )
        return list(sold)

    assert _run(scenario()) == []
