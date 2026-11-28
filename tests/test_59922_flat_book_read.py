"""A failed portfolio read is reported even when the tracked book is flat."""
from __future__ import annotations

import asyncio

import main_bot


def _run(coro):
    return asyncio.run(coro)


def test_flat_book_portfolio_failure_alerts_once(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)

    async def portfolio():
        raise RuntimeError("portfolio down")

    async def orders():
        return {"OrdersResponse": {}}

    async def cancel(order_id):
        return True

    async def scenario():
        main_bot.alerts.init(store, None)
        for _ in range(2):
            await main_bot.reconciliation.reconcile_once(
                store, portfolio, orders, cancel,
                alert=main_bot.alerts.send_nowait,
            )
        recent = await main_bot.alerts.recent(10)
        return [row for row in recent if row.get("event") == "untracked_check_unavailable"]

    pages = _run(scenario())
    assert len(pages) == 1
    assert pages[0]["severity"] == "error"
    assert "portfolio" in pages[0]["message"]


def test_expired_session_alerts_when_the_book_is_flat(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)

    async def portfolio():
        raise main_bot.reconciliation.BrokerSessionExpired("session dead")

    async def orders():
        return {"OrdersResponse": {}}

    async def cancel(order_id):
        return True

    async def scenario():
        main_bot.alerts.init(store, None)
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, orders, cancel,
            alert=main_bot.alerts.send_nowait,
        )
        recent = await main_bot.alerts.recent(10)
        return [row for row in recent if row.get("event") == "untracked_check_unavailable"]

    pages = _run(scenario())
    assert len(pages) == 1
    assert pages[0]["severity"] == "error"
    assert "BrokerSessionExpired" in pages[0]["message"]
    assert "session dead" not in pages[0]["message"]
