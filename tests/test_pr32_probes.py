"""Adapted Monitor probes for PR #32.

The originals asserted the bugs. These assert the corrected behavior:
fill releases by quantity, a close inside the TTL does not block re-entry,
a held name is not counted twice, a restart cannot admit 12% against a 10%
cap, a busy lock is retried by the sweep, and a stale position snapshot
cannot sneak an entry through.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import main_bot


ACCT = 4346.0
NOMINAL = ACCT * 0.061
ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 25, 10, 26, 30, tzinfo=ET)


def _run(coro):
    return asyncio.run(coro)


def _put(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "action": "BUY",
        "instrument": "option",
        "option_right": "PUT",
        "signal_id": f"probe:{ticker}",
    }


def _pos(entry, stop, qty=1, filled=None):
    return {
        "qty": qty,
        "filled_qty": qty if filled is None else filled,
        "contract": {"right": "PUT"},
        "entry_premium": entry,
        "stop_premium": stop,
        "action": "BUY",
    }


@pytest.fixture
def store(monkeypatch):
    mem = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", mem)
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")
    main_bot._heat_release_retry.clear()
    main_bot._heat_settle_retry.clear()
    return mem


def test_p1_booked_below_nominal_releases_on_fill(store):
    async def scenario():
        admitted = await main_bot.admit_heat_reservation(_put("PLTR"), NOMINAL, ACCT)
        await store.set_position("PLTR", _pos(2.20, 1.10))
        await main_bot.settle_heat_reservation("PLTR")
        raw = await store.get(main_bot.HEAT_RESERVATIONS_KEY)
        nxt = await main_bot.admit_heat_reservation(_put("TSLA"), NOMINAL, ACCT)
        return admitted, raw, nxt

    admitted, raw, nxt = _run(scenario())
    assert admitted["reserved"]
    assert raw in (None, "[]")
    assert nxt["allowed"] and nxt["reserved"]
    assert nxt["heat_pct"] == pytest.approx((110.0 + NOMINAL) / ACCT * 100.0, abs=0.05)
    assert (110.0 + NOMINAL) / ACCT * 100.0 < 10.0


def test_p1b_close_within_ttl_recounts_full_plan(store):
    """Stronger close: delete the position and do not call release again.

    A full fill already dropped the row. Re-entry must not see phantom heat.
    """
    async def scenario():
        await main_bot.admit_heat_reservation(_put("PLTR"), NOMINAL, ACCT)
        await store.set_position("PLTR", _pos(2.20, 1.10))
        await main_bot.settle_heat_reservation("PLTR")
        await store.delete_position("PLTR")
        reserved, _book = await main_bot.peek_reserved_exposure(await store.all_positions())
        again = await main_bot.admit_heat_reservation(_put("PLTR"), NOMINAL, ACCT)
        return reserved, again

    reserved, again = _run(scenario())
    assert reserved == pytest.approx(0.0)
    assert again["allowed"] and again["reserved"]
    assert not any("already reserving heat for PLTR" in reason for reason in again["reasons"])


def test_p1_partial_fill_keeps_only_the_unfilled_fraction(store):
    async def scenario():
        payload = _put("PLTR")
        payload["quantity"] = 3
        await main_bot.admit_heat_reservation(payload, 300.0, 10_000.0)
        await store.set_position("PLTR", _pos(2.0, 1.0, qty=3, filled=1))
        await main_bot.settle_heat_reservation("PLTR")
        after_settle = json.loads(await store.get(main_bot.HEAT_RESERVATIONS_KEY) or "[]")
        await main_bot.sweep_heat_reservations()
        after_one = json.loads(await store.get(main_bot.HEAT_RESERVATIONS_KEY) or "[]")
        await main_bot.sweep_heat_reservations()
        after_two = json.loads(await store.get(main_bot.HEAT_RESERVATIONS_KEY) or "[]")
        return after_settle, after_one, after_two

    after_settle, after_one, after_two = _run(scenario())
    assert after_settle[0]["qty"] == 2
    assert after_settle[0]["risk_usd"] == pytest.approx(200.0)
    assert after_one[0]["qty"] == 2
    assert after_one[0]["risk_usd"] == pytest.approx(200.0)
    assert after_two[0]["qty"] == 2
    assert after_two[0]["risk_usd"] == pytest.approx(200.0)
    assert after_two[0]["applied_filled_qty"] == 1
    assert after_two[0]["planned_qty"] == 3


def test_p2_held_avgo_is_not_counted_twice_against_nvda(store):
    async def scenario():
        payload = _put("AVGO")
        payload["quantity"] = 2
        await main_bot.admit_heat_reservation(payload, 50.0, 100_000.0)
        await store.set_position("AVGO", _pos(1.0, 0.8))
        await main_bot.settle_heat_reservation("AVGO")
        return await main_bot.admit_heat_reservation(_put("NVDA"), 50.0, 100_000.0)

    result = _run(scenario())
    assert result["allowed"], result["reasons"]
    assert not any("AVGO/AVGO" in reason for reason in result["reasons"])


def test_p3_restart_keeps_live_heat_so_the_cap_cannot_be_overshot(store):
    async def scenario():
        first = await main_bot.admit_heat_reservation(_put("TSLA"), 400.0, 10_000.0)
        await main_bot.drop_expired_heat_reservations("restart")
        second = await main_bot.admit_heat_reservation(_put("PLTR"), 400.0, 10_000.0)
        third = await main_bot.admit_heat_reservation(_put("AVGO"), 400.0, 10_000.0)
        return first, second, third

    first, second, third = _run(scenario())
    assert first["reserved"]
    assert sum(1 for row in (second, third) if row["reserved"]) == 1


def test_p3_requeued_job_rechecks_heat_before_placing(store, monkeypatch):
    async def account(**_kwargs):
        return 10_000.0

    monkeypatch.setattr(main_bot, "_risk_account_reference", account)
    monkeypatch.setattr(main_bot, "estimate_entry_risk_usd", lambda _payload: 400.0)
    saved = main_bot._LIVE_POLICY.get("risk_per_trade_pct")
    main_bot._LIVE_POLICY["risk_per_trade_pct"] = 4.0

    async def scenario():
        await main_bot.admit_heat_reservation(_put("TSLA"), 400.0, 10_000.0)
        await main_bot.admit_heat_reservation(_put("PLTR"), 400.0, 10_000.0)
        replay = _put("TSLA")
        replay["_heat_reservation_id"] = "probe:TSLA"
        kept = await main_bot.ensure_queued_heat(replay)
        refused = await main_bot.ensure_queued_heat(_put("AVGO"))
        return kept, refused

    try:
        kept, refused = _run(scenario())
    finally:
        if saved is None:
            main_bot._LIVE_POLICY.pop("risk_per_trade_pct", None)
        else:
            main_bot._LIVE_POLICY["risk_per_trade_pct"] = saved
    assert kept is None
    assert refused and "portfolio heat" in refused


def test_p4_busy_release_is_retried_by_the_sweep(store, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    async def scenario():
        admitted = await main_bot.admit_heat_reservation(_put("TSLA"), 400.0, 10_000.0)
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(0.2)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        started = time.monotonic()
        await main_bot.release_heat_reservation(admitted["id"], reason="reject")
        waited = time.monotonic() - started
        await task
        await main_bot.sweep_heat_reservations()
        raw = await store.get(main_bot.HEAT_RESERVATIONS_KEY)
        return waited, raw

    waited, raw = _run(scenario())
    assert waited < 0.5
    assert "sweep will retry" in caplog.text
    assert raw in (None, "[]")


def test_p5_settle_does_not_block_on_a_busy_lock(store, caplog):
    caplog.set_level(logging.INFO, logger="etrade-bot")

    async def scenario():
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(0.3)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        started = time.monotonic()
        await main_bot.settle_heat_reservation("PLTR")
        elapsed = time.monotonic() - started
        await task
        return elapsed

    assert _run(scenario()) < 0.5
    assert "does not wait" in caplog.text


def test_p6_lock_ttl_expiry_lost_update(store, monkeypatch):
    """Slow book read longer than the lock TTL. Only one entry may land."""
    real_lock = store.lock

    def short_heat_lock(name, ttl_ms=30_000, wait_timeout=10.0):
        if name == "heat-reservation":
            ttl_ms = 200
        return real_lock(name, ttl_ms=ttl_ms, wait_timeout=wait_timeout)

    monkeypatch.setattr(store, "lock", short_heat_lock)
    real_all = store.all_positions
    calls = {"n": 0}

    async def slow_all():
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(0.4)
        return await real_all()

    monkeypatch.setattr(store, "all_positions", slow_all)

    async def scenario():
        async def second():
            await asyncio.sleep(0.25)
            return await main_bot.admit_heat_reservation(_put("PLTR"), 600.0, 10_000.0)

        results = await asyncio.gather(
            main_bot.admit_heat_reservation(_put("TSLA"), 600.0, 10_000.0),
            second(),
        )
        rows = json.loads(await store.get(main_bot.HEAT_RESERVATIONS_KEY) or "[]")
        return results, rows

    results, rows = _run(scenario())
    assert sum(1 for row in results if row["reserved"]) == 1
    assert len(rows) == 1
    assert not (results[0]["reserved"] and results[1]["reserved"])


def test_p8_stale_snapshot_cannot_miss_a_fill(store):
    async def scenario():
        first = await main_bot.admit_heat_reservation(_put("TSLA"), 600.0, 10_000.0)
        stale = await store.all_positions()
        await store.set_position("TSLA", _pos(7.0, 1.0))
        await main_bot.settle_heat_reservation("TSLA")
        second = await main_bot.admit_heat_reservation(
            _put("PLTR"), 600.0, 10_000.0, positions=stale,
        )
        return first, second

    first, second = _run(scenario())
    assert first["reserved"]
    assert not second["reserved"]
    assert second["heat_pct"] == pytest.approx(12.0, abs=0.05)


def test_p7b_wrong_fresh_massive_cannot_enter_hash_and_fails_closed():
    true_vwap, wrong = 703.44, 703.44 * 0.99

    def bar(minute, price):
        when = datetime(2026, 11, 25, minute // 60, minute % 60, tzinfo=ET)
        return {
            "t": int(when.timestamp() * 1000),
            "vw": price, "v": 1000.0, "c": price, "o": price, "h": price, "l": price,
        }

    bars = [bar(minute, wrong) for minute in range(570, 626)]
    first = datetime(2026, 11, 25, 9, 30, tzinfo=ET)
    fields = {
        "sum_pv": repr(true_vwap * 1e5),
        "sum_v": repr(1e5),
        "count": "27",
        "first_ts": first.isoformat(),
        "last_ts": datetime(2026, 11, 25, 10, 25, tzinfo=ET).isoformat(),
        "last_volume": repr(8e6),
        "last_price": repr(true_vwap),
    }
    assert main_bot.scanner_mod.session_vwap_needs_seed(fields, NOW) is False
    plan = main_bot.scanner_mod.plan_session_vwap_seed(fields, bars, NOW, max_age_s=300)
    assert plan["applied"] is False
    view = {
        "vwap": main_bot.minute_aggs_rth_vwap({"results": bars}, NOW)["vwap"],
        "reason": None,
    }
    out = main_bot._crosscheck_today_vwap({"measured": True, "vwap": true_vwap}, view)
    assert out["measured"] is False and out["reason"] == "crosscheck_gap_bps"


def test_p7c_late_start_seed_still_writes_massive_prefix():
    """Pre-existing restart seed. Documented, not removed."""

    def bar(minute, price):
        when = datetime(2026, 11, 25, minute // 60, minute % 60, tzinfo=ET)
        return {
            "t": int(when.timestamp() * 1000),
            "vw": price, "v": 1000.0, "c": price, "o": price, "h": price, "l": price,
        }

    bars = [bar(minute, 700.0) for minute in range(570, 626)]
    plan = main_bot.scanner_mod.plan_session_vwap_seed(None, bars, NOW, max_age_s=300)
    assert plan["applied"] is True and abs(plan["vwap"] - 700.0) < 1e-6
