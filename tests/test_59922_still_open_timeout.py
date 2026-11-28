"""The 3:50 check times out, then the next schedule still runs."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import main_bot


ET = ZoneInfo("America/New_York")
FULL_DAY = datetime(2026, 11, 18, 15, 50, tzinfo=ET)


def _run(coro):
    return asyncio.run(coro)


def test_a_hung_open_check_times_out_and_the_next_run_works(monkeypatch):
    monkeypatch.setattr(main_bot, "_utcnow", lambda: FULL_DAY.astimezone(timezone.utc))
    monkeypatch.setattr(main_bot, "STILL_OPEN_CHECK_TIMEOUT_S", 0.05)
    saved_state = main_bot.state
    saved_task = main_bot._still_open_alert_task
    store = main_bot.StateStore(None)
    main_bot.state = store
    main_bot._still_open_alert_task = None

    async def hang():
        await asyncio.sleep(2)
        return {}

    async def ready():
        return {"QQQ": {"qty": 1, "filled_qty": 1}}

    async def scenario():
        main_bot.alerts.init(store, None)
        monkeypatch.setattr(store, "all_positions", hang)
        await main_bot._alert_positions_still_open()
        timed = await main_bot.alerts.recent(5)
        monkeypatch.setattr(store, "all_positions", ready)
        await main_bot._alert_positions_still_open()
        return timed, await main_bot.alerts.recent(5)

    try:
        timed, later = _run(scenario())
    finally:
        task = main_bot._still_open_alert_task
        if task is not None and not task.done():
            task.cancel()
        main_bot._still_open_alert_task = saved_task
        main_bot.state = saved_state

    assert any(row["event"] == "position_open_after_flatten_timeout" for row in timed)
    assert any(row["severity"] == "error" and row["event"] == "position_open_after_flatten_timeout" for row in timed)
    assert any("QQQ" in row["message"] for row in later)


def test_a_finished_timeout_does_not_block_the_next_kick(monkeypatch):
    monkeypatch.setattr(main_bot, "_utcnow", lambda: FULL_DAY.astimezone(timezone.utc))
    monkeypatch.setattr(main_bot, "STILL_OPEN_CHECK_TIMEOUT_S", 0.05)
    saved_state = main_bot.state
    saved_task = main_bot._still_open_alert_task
    store = main_bot.StateStore(None)
    main_bot.state = store
    main_bot._still_open_alert_task = None

    async def hang():
        await asyncio.sleep(2)
        return {}

    async def ready():
        return {"QQQ": {"qty": 1, "filled_qty": 1}}

    async def scenario():
        main_bot.alerts.init(store, None)
        monkeypatch.setattr(store, "all_positions", hang)
        main_bot._kick_positions_still_open_alert()
        first = main_bot._still_open_alert_task
        await first
        assert first.done()
        monkeypatch.setattr(store, "all_positions", ready)
        main_bot._kick_positions_still_open_alert()
        second = main_bot._still_open_alert_task
        assert second is not first
        await second
        return await main_bot.alerts.recent(5)

    try:
        recent = _run(scenario())
    finally:
        task = main_bot._still_open_alert_task
        if task is not None and not task.done():
            task.cancel()
        main_bot._still_open_alert_task = saved_task
        main_bot.state = saved_state

    text = " ".join(row["message"] for row in recent)
    assert "timed out" in text
    assert "QQQ" in text
