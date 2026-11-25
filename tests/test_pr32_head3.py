"""PR #32 head cc99621 probes, inverted.

The Monitor's copies assert the bugs. These assert the fix: a partial
remainder survives repeated sweeps, a deferred release frees only the ids
it captured, a retry appended mid-sweep is kept, an expired lock admits
one entry, the placement book merges by ticker, and a busy requeue is logged.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import pytest

import main_bot


def _run(coro):
    return asyncio.run(coro)


def _put(ticker, qty=None):
    payload = {
        "ticker": ticker,
        "action": "BUY",
        "instrument": "option",
        "option_right": "PUT",
        "signal_id": f"h3:{ticker}:{qty}",
    }
    if qty:
        payload["quantity"] = qty
    return payload


def _pos(entry, stop, qty=1, filled=None):
    row = {
        "qty": qty,
        "contract": {"right": "PUT"},
        "entry_premium": entry,
        "stop_premium": stop,
        "action": "BUY",
    }
    if filled is not None:
        row["filled_qty"] = filled
    return row


async def _rows(store):
    return json.loads(await store.get(main_bot.HEAT_RESERVATIONS_KEY) or "[]")


@pytest.fixture
def store(monkeypatch):
    fresh = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", fresh)
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")
    main_bot._heat_release_retry.clear()
    main_bot._heat_settle_retry.clear()
    return fresh


def test_h1_partial_remainder_survives_repeated_sweeps(store):
    async def scenario():
        await main_bot.admit_heat_reservation(_put("PLTR", 3), 300.0, 10_000.0)
        await store.set_position("PLTR", _pos(2.0, 1.0, qty=3, filled=1))
        await main_bot.settle_heat_reservation("PLTR")
        after_settle = await _rows(store)
        await main_bot.sweep_heat_reservations()
        after_one = await _rows(store)
        await main_bot.sweep_heat_reservations()
        after_two = await _rows(store)
        return after_settle, after_one, after_two

    settled, once, twice = _run(scenario())
    assert settled[0]["qty"] == 2 and settled[0]["risk_usd"] == pytest.approx(200.0)
    assert once[0]["qty"] == 2 and once[0]["risk_usd"] == pytest.approx(200.0)
    assert twice[0]["qty"] == 2 and twice[0]["risk_usd"] == pytest.approx(200.0)
    assert twice[0]["applied_filled_qty"] == 1
    assert twice[0]["planned_qty"] == 3


def test_h2_deferred_ticker_release_keeps_the_new_entry(store):
    async def scenario():
        main_bot._heat_release_retry.append({
            "id": None, "ticker": "PLTR", "reason": "close",
        })
        new = await main_bot.admit_heat_reservation(_put("PLTR", 1), 300.0, 10_000.0)
        await main_bot.sweep_heat_reservations()
        return new, await _rows(store)

    new, rows = _run(scenario())
    assert new["reserved"]
    assert len(rows) == 1
    assert rows[0]["id"] == new["id"]


def test_h2_deferral_records_the_ids_then_present(store):
    async def scenario():
        admitted = await main_bot.admit_heat_reservation(_put("PLTR", 1), 300.0, 10_000.0)
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(0.2)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        await main_bot.release_heat_reservation(ticker="PLTR", reason="close")
        pending = list(main_bot._heat_release_retry)
        await task
        await main_bot.sweep_heat_reservations()
        return admitted, pending, await _rows(store)

    admitted, pending, rows = _run(scenario())
    assert pending and pending[0]["ids"] == [admitted["id"]]
    assert rows == []


def test_h3_sweep_keeps_retries_appended_mid_sweep(store, monkeypatch):
    real_all = store.all_positions

    async def all_with_append():
        main_bot._heat_release_retry.append({
            "id": "late", "ids": ["late"], "ticker": None, "reason": "reject",
        })
        return await real_all()

    async def scenario():
        await store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps([{
                "id": "late", "ticker": "TSLA", "direction": -1,
                "risk_usd": 400.0, "qty": 1, "expires_at": time.time() + 600,
            }]),
            ex=600,
        )
        monkeypatch.setattr(store, "all_positions", all_with_append)
        await main_bot.sweep_heat_reservations()
        pending = list(main_bot._heat_release_retry)
        mid_rows = await _rows(store)
        store.all_positions = real_all
        await main_bot.sweep_heat_reservations()
        return pending, mid_rows, list(main_bot._heat_release_retry), await _rows(store)

    pending, mid_rows, later, gone = _run(scenario())
    assert any(item.get("id") == "late" for item in pending)
    assert len(mid_rows) == 1
    assert later == []
    assert gone == []


def test_h4_expired_lock_cannot_admit_both(store, monkeypatch):
    monkeypatch.setattr(main_bot, "HEAT_RESERVATION_LOCK_TTL_MS", 100)
    real_all = store.all_positions
    calls = {"n": 0}

    async def slow():
        calls["n"] += 1
        if calls["n"] == 2:
            await asyncio.sleep(0.3)
        return await real_all()

    monkeypatch.setattr(store, "all_positions", slow)

    async def scenario():
        async def second():
            await asyncio.sleep(0.15)
            return await main_bot.admit_heat_reservation(_put("PLTR"), 600.0, 10_000.0)

        results = await asyncio.gather(
            main_bot.admit_heat_reservation(_put("TSLA"), 600.0, 10_000.0),
            second(),
        )
        return results, await _rows(store)

    results, rows = _run(scenario())
    assert sum(1 for row in results if row["reserved"]) == 1
    assert len(rows) == 1
    assert not (results[0]["reserved"] and results[1]["reserved"])


def test_h5_placement_book_merges_a_held_ticker(store):
    async def scenario():
        await main_bot.admit_heat_reservation(_put("AVGO", 1), 50.0, 100_000.0)
        await store.set_position("AVGO", _pos(1.0, 0.8, qty=1))
        positions = await store.all_positions()
        _reserved, rows = await main_bot.peek_reserved_exposure(positions)
        book = main_bot.portfolio_correlation_book(positions, rows)
        assert [row["ticker"] for row in book].count("AVGO") == 1
        return main_bot.portfolio_risk.assess_correlation(
            "NVDA", -1, 50.0, book, 100_000.0, 6.1, 10.0, "stressed",
        )

    corr = _run(scenario())
    assert corr["allowed"], corr["reasons"]
    assert not any("AVGO/AVGO" in reason for reason in corr["reasons"])


def test_h6_busy_requeue_is_refused_and_logged(store, monkeypatch, caplog):
    async def account(**_kwargs):
        return 10_000.0

    monkeypatch.setattr(main_bot, "_risk_account_reference", account)
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    async def scenario():
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(3.3)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        payload = _put("TSLA")
        payload["_heat_reservation_id"] = "gone"
        out = await main_bot.ensure_queued_heat(payload)
        await task
        return out

    out = _run(scenario())
    assert out and "lock busy" in out
    assert "requeued TSLA dropped" in caplog.text


def test_m2_app_requeue_uses_the_app_gate(store, monkeypatch):
    seen = {}

    async def account(**_kwargs):
        return 10_000.0

    async def capture(payload, risk, account, **kwargs):
        seen["kwargs"] = kwargs
        seen["risk"] = risk
        return {"allowed": True, "reserved": True, "id": "app-1"}

    monkeypatch.setattr(main_bot, "_risk_account_reference", account)
    monkeypatch.setattr(main_bot, "estimate_entry_risk_usd", lambda _payload: 100.0)
    monkeypatch.setattr(main_bot, "admit_heat_reservation", capture)

    async def scenario():
        payload = _put("TSLA")
        payload[main_bot.DISPATCH_ORIGIN_FIELD] = main_bot.DISPATCH_ORIGIN_APP
        refusal = await main_bot.ensure_queued_heat(payload)
        return refusal

    assert _run(scenario()) is None
    assert seen["kwargs"]["enforce_correlation"] is False
    assert seen["kwargs"]["heat_cap_pct"] == main_bot.APP_MAX_OPEN_RISK_PCT


def test_m4_idle_sweep_does_not_lock(store, monkeypatch):
    def refuse_lock(*_args, **_kwargs):
        raise AssertionError("sweep locked an empty book")

    monkeypatch.setattr(store, "lock", refuse_lock)
    _run(main_bot.sweep_heat_reservations())


def test_s1_boot_keeps_live_rows(store):
    async def scenario():
        await main_bot.admit_heat_reservation(_put("TSLA"), 400.0, 10_000.0)
        await main_bot.drop_expired_heat_reservations("restart")
        return await _rows(store)

    assert len(_run(scenario())) == 1
