"""5.99.6 follow-ups on the heat reservation.

The requeue gate uses app_decides_entry. The lock token and the reservation
write are one compare-and-set. A close releases only the id it already
holds. A sweep that changes nothing does not write.
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
        "signal_id": f"f6:{ticker}:{qty}",
    }
    if qty:
        payload["quantity"] = qty
    return payload


def _row(ident, ticker, risk=100.0):
    return {
        "id": ident,
        "ticker": ticker,
        "direction": -1,
        "risk_usd": risk,
        "original_risk_usd": risk,
        "qty": 1,
        "planned_qty": 1,
        "applied_filled_qty": 0,
        "expires_at": time.time() + 600,
    }


async def _rows(store):
    raw = await store.get(main_bot.HEAT_RESERVATIONS_KEY)
    return json.loads(raw) if raw else []


@pytest.fixture
def store(monkeypatch):
    fresh = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", fresh)
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")
    main_bot._heat_release_retry.clear()
    main_bot._heat_settle_retry.clear()
    return fresh


def _seed_rows(store, rows):
    async def put():
        await store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps(rows),
            ex=600,
        )
    _run(put())


def test_requeue_gate_uses_app_decides_entry(store, monkeypatch):
    """A public origin field is not the dispatch origin, and APP_DECIDES off is the scanner gate."""
    seen = []

    async def account(**_kwargs):
        return 10_000.0

    async def capture(payload, risk, account_usd, **kwargs):
        seen.append(kwargs)
        return {"allowed": True, "reserved": True, "id": "queued"}

    monkeypatch.setattr(main_bot, "_risk_account_reference", account)
    monkeypatch.setattr(main_bot, "estimate_entry_risk_usd", lambda _payload: 100.0)
    monkeypatch.setattr(main_bot, "admit_heat_reservation", capture)

    async def scenario():
        public = _put("TSLA")
        public["origin"] = main_bot.DISPATCH_ORIGIN_APP
        await main_bot.ensure_queued_heat(public)

        monkeypatch.setattr(main_bot, "APP_DECIDES", False)
        disabled = _put("AAPL")
        disabled[main_bot.DISPATCH_ORIGIN_FIELD] = main_bot.DISPATCH_ORIGIN_APP
        await main_bot.ensure_queued_heat(disabled)

        monkeypatch.setattr(main_bot, "APP_DECIDES", True)
        app = _put("NVDA")
        app[main_bot.DISPATCH_ORIGIN_FIELD] = main_bot.DISPATCH_ORIGIN_APP
        await main_bot.ensure_queued_heat(app)

    _run(scenario())
    assert seen[0] == {}
    assert seen[1] == {}
    assert seen[2]["enforce_correlation"] is False
    assert seen[2]["heat_cap_pct"] == main_bot.APP_MAX_OPEN_RISK_PCT
    assert main_bot.app_decides_entry(None, False, True) is False
    assert main_bot.app_decides_entry(main_bot.DISPATCH_ORIGIN_APP, False, False) is False
    assert main_bot.app_decides_entry(main_bot.DISPATCH_ORIGIN_APP, False, True) is True


class _RaceRedis:
    """GET still shows our token. The compare-and-set does not."""

    def __init__(self, cas):
        self.kv = {}
        self.heat_writes = []
        self.cas = cas

    async def set(self, key, value=None, px=None, ex=None, nx=None, **_kwargs):
        if key == main_bot.HEAT_RESERVATIONS_KEY:
            self.heat_writes.append(value)
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    async def get(self, key):
        return self.kv.get(key)

    async def delete(self, *keys):
        for key in keys:
            if key == main_bot.HEAT_RESERVATIONS_KEY:
                self.heat_writes.append(None)
            self.kv.pop(key, None)
        return len(keys)

    async def smembers(self, _key):
        return set()

    async def eval(self, script, numkeys, *args):
        if numkeys == 2:
            if self.cas == "down":
                raise ConnectionError("redis unavailable")
            return 0
        key, token = args[0], args[1]
        if self.kv.get(key) != token:
            return 0
        if "del" in script and "pexpire" not in script:
            self.kv.pop(key, None)
        return 1


def test_cas_loss_after_lock_expiry_does_not_write(monkeypatch):
    redis = _RaceRedis("lose")
    monkeypatch.setattr(main_bot, "state", main_bot.StateStore(redis))
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")

    result = _run(main_bot.admit_heat_reservation(_put("TSLA"), 400.0, 10_000.0))
    assert result["reserved"] is False
    assert result["allowed"] is False
    assert redis.heat_writes == []
    assert main_bot.HEAT_RESERVATIONS_KEY not in redis.kv


def test_redis_unavailable_refuses_the_admit(monkeypatch):
    redis = _RaceRedis("down")
    monkeypatch.setattr(main_bot, "state", main_bot.StateStore(redis))
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")

    result = _run(main_bot.admit_heat_reservation(_put("PLTR"), 400.0, 10_000.0))
    assert result["reserved"] is False
    assert result["allowed"] is False
    assert redis.heat_writes == []


def test_fakeredis_cas_writes_only_while_the_token_matches(monkeypatch):
    fakeredis = pytest.importorskip("fakeredis.aioredis")
    monkeypatch.setattr(main_bot, "PORTFOLIO_HEAT_PCT", 10.0)
    monkeypatch.setenv("HEAT_RESERVATION_ENABLED", "true")

    async def scenario():
        client = fakeredis.FakeRedis(decode_responses=True)
        try:
            await client.eval("return 1", 0)
        except Exception as exc:
            pytest.skip(f"fakeredis eval unavailable: {type(exc).__name__}")
        store = main_bot.StateStore(client)
        monkeypatch.setattr(main_bot, "state", store)
        lock = "lock:heat-reservation"
        key = main_bot.HEAT_RESERVATIONS_KEY
        await client.set(lock, "ours", px=8_000)
        wrote = await store.compare_write_heat(lock, "ours", key, '[{"id":"ours"}]', 60)
        await client.set(lock, "theirs", px=8_000)
        lost = await store.compare_write_heat(lock, "ours", key, '[{"id":"stolen"}]', 60)
        stored = await client.get(key)
        await client.delete(lock)
        await store.delete(key)
        main_bot._heat_release_retry.clear()
        main_bot._heat_settle_retry.clear()
        admitted = await main_bot.admit_heat_reservation(_put("AVGO"), 100.0, 10_000.0)
        raw = await store.get(key)
        return wrote, lost, stored, admitted, raw

    wrote, lost, stored, admitted, raw = _run(scenario())
    assert wrote is True
    assert lost is False
    assert json.loads(stored) == [{"id": "ours"}]
    assert admitted["reserved"] is True
    assert raw and "AVGO" in raw


def test_record_close_releases_only_the_closed_reservation(store, monkeypatch):
    async def balance(*, dry_run=False):
        return {"total": 10_000.0, "available": 10_000.0}

    monkeypatch.setattr(main_bot, "_fetch_broker_balance", balance)

    async def scenario():
        _closed = _row("closed-leg", "PLTR", 200.0)
        _reentry = _row("reentry", "PLTR", 150.0)
        await store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps([_closed, _reentry]),
            ex=600,
        )
        await store.set_position("PLTR", {
            "qty": 1,
            "filled_qty": 1,
            "entry": 100.0,
            "action": "BUY",
            "_heat_reservation_id": "closed-leg",
        })
        await main_bot._record_close(
            "PLTR", 101.0, {"ticker": "PLTR", "action": "BUY"},
            close_reason="close_fill",
        )
        return await _rows(store)

    rows = _run(scenario())
    assert [row["id"] for row in rows] == ["reentry"]
    assert _run(store.get_position("PLTR")) is None


def test_record_close_deferral_read_failure_keeps_the_reentry(store, monkeypatch):
    async def balance(*, dry_run=False):
        return {"total": 10_000.0, "available": 10_000.0}

    monkeypatch.setattr(main_bot, "_fetch_broker_balance", balance)
    real_read = main_bot._read_heat_rows

    async def boom():
        raise RuntimeError("heat read failed")

    async def scenario():
        await store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps([_row("closed-leg", "PLTR"), _row("reentry", "PLTR")]),
            ex=600,
        )
        await store.set_position("PLTR", {
            "qty": 1,
            "filled_qty": 1,
            "entry": 100.0,
            "action": "BUY",
            "_heat_reservation_id": "closed-leg",
        })
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(0.25)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        monkeypatch.setattr(main_bot, "_read_heat_rows", boom)
        await main_bot._record_close(
            "PLTR", 101.0, {"ticker": "PLTR", "action": "BUY"},
            close_reason="flatten_fill",
        )
        pending = list(main_bot._heat_release_retry)
        monkeypatch.setattr(main_bot, "_read_heat_rows", real_read)
        await task
        await main_bot.sweep_heat_reservations()
        return pending, await _rows(store)

    pending, rows = _run(scenario())
    assert pending and pending[0]["ids"] == ["closed-leg"]
    assert "reentry" not in pending[0]["ids"]
    assert pending[0]["ticker"] is None
    assert [row["id"] for row in rows] == ["reentry"]


def test_deferral_read_failure_does_not_free_other_ids(store, monkeypatch, caplog):
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    real_read = main_bot._read_heat_rows

    async def boom():
        raise RuntimeError("heat read failed")

    async def scenario():
        await store.set(
            main_bot.HEAT_RESERVATIONS_KEY,
            json.dumps([_row("old", "PLTR"), _row("newer", "PLTR"), _row("other", "TSLA")]),
            ex=600,
        )
        hold = store.lock("heat-reservation", ttl_ms=8_000, wait_timeout=0.1)

        async def holder():
            await hold.acquire()
            await asyncio.sleep(0.2)
            await hold.release()

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.05)
        monkeypatch.setattr(main_bot, "_read_heat_rows", boom)
        await main_bot.release_heat_reservation(ticker="PLTR", reason="close")
        pending = list(main_bot._heat_release_retry)
        monkeypatch.setattr(main_bot, "_read_heat_rows", real_read)
        await task
        await main_bot.sweep_heat_reservations()
        return pending, await _rows(store)

    pending, rows = _run(scenario())
    assert pending == []
    assert [row["id"] for row in rows] == ["old", "newer", "other"]
    assert "until TTL" in caplog.text


def test_unchanged_sweep_does_zero_writes(store, monkeypatch):
    writes = []
    real_set = store._set_raw
    real_delete = store.delete
    real_cas = store.compare_write_heat

    async def spy_set(*args, **kwargs):
        writes.append(args[0] if args else "set")
        return await real_set(*args, **kwargs)

    async def spy_delete(*args, **kwargs):
        writes.append(("del", args))
        return await real_delete(*args, **kwargs)

    async def spy_cas(*args, **kwargs):
        writes.append("cas")
        return await real_cas(*args, **kwargs)

    async def scenario():
        row = _row("stay", "PLTR", 200.0)
        row["qty"] = 2
        row["planned_qty"] = 3
        row["applied_filled_qty"] = 1
        row["original_risk_usd"] = 300.0
        await store.set(main_bot.HEAT_RESERVATIONS_KEY, json.dumps([row]), ex=600)
        await store.set_position("PLTR", {
            "qty": 3,
            "filled_qty": 1,
            "contract": {"right": "PUT"},
            "entry_premium": 2.0,
            "stop_premium": 1.0,
            "action": "BUY",
        })
        monkeypatch.setattr(store, "_set_raw", spy_set)
        monkeypatch.setattr(store, "delete", spy_delete)
        monkeypatch.setattr(store, "compare_write_heat", spy_cas)
        await main_bot.sweep_heat_reservations()
        return await _rows(store)

    rows = _run(scenario())
    assert writes == []
    assert rows[0]["id"] == "stay"
    assert rows[0]["qty"] == 2
    assert rows[0]["applied_filled_qty"] == 1
