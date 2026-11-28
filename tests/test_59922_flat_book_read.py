"""A failed portfolio read pages once per outage when the tracked book is flat.

An expired session does not page here. broker_session_expired already
covers that. A later successful read re-arms the next outage.
"""
from __future__ import annotations

import asyncio

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _pages(recent):
    return [row for row in recent if row.get("event") == "untracked_check_unavailable"]


def test_generic_read_failure_pages_once_per_outage(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    calls = {"portfolio": 0}

    async def portfolio():
        calls["portfolio"] += 1
        if calls["portfolio"] == 7:
            return {"PortfolioResponse": {}}
        raise RuntimeError("portfolio down")

    async def orders():
        return {"OrdersResponse": {}}

    async def cancel(order_id):
        return True

    async def scenario():
        main_bot.alerts.init(store, None)
        for _ in range(6):
            await main_bot.reconciliation.reconcile_once(
                store, portfolio, orders, cancel,
                alert=main_bot.alerts.send_nowait,
            )
        during = _pages(await main_bot.alerts.recent(20))
        # Successful read clears the latch.
        await main_bot.reconciliation.reconcile_once(
            store, portfolio, orders, cancel,
            alert=main_bot.alerts.send_nowait,
        )
        after_recovery = _pages(await main_bot.alerts.recent(20))
        # The next outage pages once more, then stays quiet.
        for _ in range(4):
            await main_bot.reconciliation.reconcile_once(
                store, portfolio, orders, cancel,
                alert=main_bot.alerts.send_nowait,
            )
        after_new = _pages(await main_bot.alerts.recent(20))
        return during, after_recovery, after_new

    during, after_recovery, after_new = _run(scenario())
    assert len(during) == 1
    assert during[0]["severity"] == "error"
    assert "portfolio" in during[0]["message"]
    assert len(after_recovery) == 1
    assert len(after_new) == 2
    assert after_new[-1]["severity"] == "error"


def test_expired_session_does_not_page_untracked_check(monkeypatch):
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
        for _ in range(8):
            await main_bot.reconciliation.reconcile_once(
                store, portfolio, orders, cancel,
                alert=main_bot.alerts.send_nowait,
            )
        return _pages(await main_bot.alerts.recent(20))

    pages = _run(scenario())
    assert pages == []
