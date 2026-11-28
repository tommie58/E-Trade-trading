"""Mode-off 3:50 names the same contracts the 3:40 flatten would sell."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import main_bot


ET = ZoneInfo("America/New_York")
FULL_DAY = datetime(2026, 11, 18, 15, 50, tzinfo=ET)


def _run(coro):
    return asyncio.run(coro)


def _option(expiration):
    return {
        "qty": 1,
        "filled_qty": 1,
        "action": "BUY",
        "contract": {"right": "PUT", "strike": 450, "expiration": expiration},
    }


def test_mode_off_alert_matches_contract_expires_by(monkeypatch):
    monkeypatch.setattr(main_bot, "_utcnow", lambda: FULL_DAY.astimezone(timezone.utc))
    monkeypatch.setattr(main_bot, "DAY_TRADE_MODE", False)
    saved = main_bot.state
    store = main_bot.StateStore(None)
    main_bot.state = store
    today = "2026-11-18"

    async def scenario():
        main_bot.alerts.init(store, None)
        await store.set_position("TODAY", _option(today))
        await store.set_position("PAST", _option("2026-11-17"))
        await store.set_position("BAD", _option("not-a-date"))
        await store.set_position("NONE", _option(None))
        await store.set_position("SWING", _option("2026-12-18"))
        await store.set_position("EQ", {"qty": 1, "filled_qty": 1, "action": "BUY"})
        await main_bot._alert_positions_still_open()
        return await main_bot.alerts.recent(5)

    try:
        recent = _run(scenario())
    finally:
        main_bot.state = saved
    message = recent[0]["message"]
    for name in ("TODAY", "PAST", "BAD", "NONE"):
        assert name in message
    assert "SWING" not in message
    assert "EQ" not in message
    assert main_bot.trailing_engine.contract_expires_by("2026-11-17", today) is True
    assert main_bot.trailing_engine.contract_expires_by("nope", today) is True
    assert main_bot.trailing_engine.contract_expires_by("2026-12-18", today) is False
