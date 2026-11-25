"""Heat is reserved when an entry is approved, not when it fills.

2026-11-25: TSLA, PLTR and AVGO puts were queued together. Each heat check
saw only the book already filled, so gross heat reached 10.844% against a
10% cap. The reservation is the planned risk, counted under one lock.
"""
from __future__ import annotations

import asyncio
import json
import time

import pytest

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _put(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "action": "BUY",
        "instrument": "option",
        "option_right": "PUT",
        "signal_id": f"test:{ticker}",
    }


def _option(entry: float, stop: float, qty: int = 1) -> dict:
    return {
        "qty": qty,
        "filled_qty": qty,
        "contract": {"right": "PUT"},
        "entry_premium": entry,
        "stop_premium": stop,
    }


@pytest.fixture
def heat_store(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")
    return store


def test_three_concurrent_entries_block_the_one_that_breaks_the_cap(heat_store):
    """400 + 400 + 400 on a $10,000 book is 12%. The third does not reserve."""

    async def scenario():
        results = await asyncio.gather(*[
            main_bot.admit_heat_reservation(_put(ticker), 400.0, 10_000.0, regime="stressed")
            for ticker in ("TSLA", "PLTR", "AVGO")
        ])
        raw = await heat_store.get(main_bot.HEAT_RESERVATIONS_KEY)
        return results, json.loads(raw)

    results, rows = _run(scenario())
    admitted = [r for r in results if r["reserved"]]
    refused = [r for r in results if not r["allowed"]]
    assert len(admitted) == 2
    assert len(refused) == 1
    assert any("portfolio heat" in reason for reason in refused[0]["reasons"])
    assert len(rows) == 2
    assert sum(row["risk_usd"] for row in rows) == pytest.approx(800.0)


def test_same_cluster_third_short_is_blocked_even_when_dollars_fit(heat_store):
    async def scenario():
        out = []
        for ticker in ("AVGO", "NVDA", "AMD"):
            out.append(await main_bot.admit_heat_reservation(
                _put(ticker), 10.0, 10_000.0, regime="stressed",
            ))
        return out

    results = _run(scenario())
    assert results[0]["allowed"] is True
    assert results[1]["allowed"] is True
    assert results[2]["allowed"] is False
    assert any("cluster" in reason for reason in results[2]["reasons"])


def test_release_on_cancel_reject_and_timeout(heat_store):
    now = time.time()
    rows = [
        {"id": "cancel-me", "ticker": "TSLA", "direction": -1, "risk_usd": 200,
         "qty": 1, "expires_at": now + 600},
        {"id": "reject-me", "ticker": "PLTR", "direction": -1, "risk_usd": 110,
         "qty": 1, "expires_at": now + 600},
        {"id": "timeout-me", "ticker": "AVGO", "direction": -1, "risk_usd": 188,
         "qty": 1, "expires_at": now + 600},
    ]
    after_cancel = main_bot.release_heat_rows(rows, now, reservation_id="cancel-me")
    assert [row["id"] for row in after_cancel] == ["reject-me", "timeout-me"]
    after_reject = main_bot.release_heat_rows(after_cancel, now, ticker="PLTR")
    assert [row["id"] for row in after_reject] == ["timeout-me"]
    after_timeout = main_bot.release_heat_rows(after_reject, now, ticker="AVGO")
    assert after_timeout == []


def test_expired_reservation_does_not_block_the_next_entry(heat_store):
    async def scenario():
        stale = [{
            "id": "stale", "ticker": "TSLA", "direction": -1, "risk_usd": 9_000,
            "qty": 1, "expires_at": time.time() - 5,
        }]
        await heat_store.set(main_bot.HEAT_RESERVATIONS_KEY, json.dumps(stale), ex=60)
        return await main_bot.admit_heat_reservation(
            _put("PLTR"), 400.0, 10_000.0, regime="stressed",
        )

    result = _run(scenario())
    assert result["allowed"] is True
    assert result["reserved"] is True


def test_fill_conversion_does_not_double_count_and_partial_keeps_the_remainder():
    now = time.time()
    rows = [{
        "id": "tsla", "ticker": "TSLA", "direction": -1, "risk_usd": 200.0,
        "qty": 2, "expires_at": now + 600,
    }]
    full = {"TSLA": _option(2.0, 0.0, qty=2)}
    assert main_bot.position_risk_usd(full["TSLA"]) == pytest.approx(400.0)
    # Planned 200 against a position that already books 200: reservation adds 0.
    covered = [{**rows[0], "risk_usd": 200.0, "qty": 1}]
    booked = {"TSLA": _option(3.0, 1.0, qty=1)}
    assert main_bot.position_risk_usd(booked["TSLA"]) == pytest.approx(200.0)
    extra, _book = main_bot.reservation_exposure(covered, booked, now)
    assert extra == pytest.approx(0.0)
    heat = (200.0 + extra) / 10_000.0 * 100.0
    assert heat == pytest.approx(2.0)

    # One contract filled ($100). The reservation is still the $200 plan,
    # so $100 of unfilled risk stays reserved and is not added twice.
    partial_pos = {"TSLA": _option(2.0, 1.0, qty=1)}
    partial_rows = [{**rows[0], "risk_usd": 200.0}]
    remainder, _rows = main_bot.reservation_exposure(partial_rows, partial_pos, now)
    assert remainder == pytest.approx(100.0)
    released, _gone = main_bot.reservation_exposure(
        main_bot.release_heat_rows(partial_rows, now, ticker="TSLA"),
        partial_pos, now,
    )
    assert released == pytest.approx(0.0)
    assert main_bot.position_risk_usd(partial_pos["TSLA"]) == pytest.approx(100.0)


def test_restart_drops_reservations_and_keeps_open_risk(heat_store):
    async def scenario():
        await heat_store.set_position("TSLA", _option(3.0, 1.0, qty=1))
        await heat_store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps([{
                "id": "working", "ticker": "PLTR", "direction": -1,
                "risk_usd": 900.0, "qty": 1, "expires_at": time.time() + 600,
            }]),
            ex=600,
        )
        await main_bot.clear_heat_reservations("restart")
        still = await heat_store.get_position("TSLA")
        gone = await heat_store.get(main_bot.HEAT_RESERVATIONS_KEY)
        admitted = await main_bot.admit_heat_reservation(
            _put("AVGO"), 400.0, 10_000.0, regime="stressed",
        )
        return still, gone, admitted

    still, gone, admitted = _run(scenario())
    assert still["entry_premium"] == 3.0
    assert gone is None
    assert admitted["allowed"] is True
    # Open risk 200 + new 400 = 6%, under the 10% cap. The cleared 900 is gone.
    assert admitted["heat_pct"] == pytest.approx(6.0)


def test_app_and_scanner_entries_cannot_both_pass_the_cap(heat_store):
    """A scanner reservation counts in the app open-risk gate, and the reverse."""

    def _call(ticker: str) -> dict:
        payload = _put(ticker)
        payload["option_right"] = "CALL"
        payload["signal_id"] = f"app:{ticker}"
        return payload

    async def scenario():
        scanner, app = await asyncio.gather(
            main_bot.admit_heat_reservation(
                _put("TSLA"), 600.0, 10_000.0, regime="stressed",
            ),
            main_bot.admit_heat_reservation(
                _call("AAPL"), 600.0, 10_000.0,
                enforce_correlation=False, heat_cap_pct=10.0,
            ),
        )
        return scanner, app

    scanner, app = _run(scenario())
    admitted = [row for row in (scanner, app) if row["reserved"]]
    refused = [row for row in (scanner, app) if not row["allowed"]]
    assert len(admitted) == 1
    assert len(refused) == 1
    assert any("portfolio heat" in reason or "open risk" in reason for reason in refused[0]["reasons"])
    reasons = main_bot.assess_app_risk_gates(
        0, 0.0, 20.0, 0.0, 600.0, 10_000.0, 5, 10.0, reserved_risk_usd=600.0,
    )
    assert any("open risk" in reason for reason in reasons)
    clear = main_bot.assess_app_risk_gates(
        0, 0.0, 20.0, 0.0, 600.0, 10_000.0, 5, 10.0, reserved_risk_usd=0.0,
    )
    assert clear == []


def test_settle_does_not_wait_when_the_reservation_lock_is_held(heat_store, caplog):
    caplog.set_level("INFO", logger="etrade-bot")

    async def held():
        lock = heat_store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.0)
        await lock.acquire()
        try:
            started = time.monotonic()
            # A different task, so the lock is not re-entered.
            await asyncio.create_task(main_bot.settle_heat_after_stop_armed("TSLA"))
            return time.monotonic() - started
        finally:
            await lock.release()

    elapsed = _run(held())
    assert elapsed < 0.3
    text = caplog.text
    assert "protective stop is armed" in text
    assert "does not wait" in text


def test_slow_position_read_cannot_expire_the_lock_and_double_book(heat_store, monkeypatch):
    sleeps = {"n": 0}

    async def slow_positions():
        sleeps["n"] += 1
        await asyncio.sleep(0.15)
        return {}

    monkeypatch.setattr(heat_store, "all_positions", slow_positions)
    monkeypatch.setattr(main_bot, "HEAT_RESERVATION_LOCK_TTL_MS", 40)

    async def scenario():
        results = await asyncio.gather(*[
            main_bot.admit_heat_reservation(_put(ticker), 600.0, 10_000.0, regime="stressed")
            for ticker in ("TSLA", "AAPL")
        ])
        raw = await heat_store.get(main_bot.HEAT_RESERVATIONS_KEY)
        return results, json.loads(raw)

    results, rows = _run(scenario())
    assert sleeps["n"] == 2
    assert sum(1 for row in results if row["reserved"]) == 1
    assert len(rows) == 1


def test_flag_off_does_not_reserve(heat_store, monkeypatch):
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "false")

    async def scenario():
        results = []
        for ticker in ("TSLA", "PLTR", "AVGO"):
            results.append(await main_bot.admit_heat_reservation(
                _put(ticker), 400.0, 10_000.0,
            ))
        stored = await heat_store.get(main_bot.HEAT_RESERVATIONS_KEY)
        return results, stored

    results, stored = _run(scenario())
    assert all(row["allowed"] is True and row["reserved"] is False for row in results)
    assert stored is None
