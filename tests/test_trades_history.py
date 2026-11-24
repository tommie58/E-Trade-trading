"""GET /trades ledger fallback and the empty-list backfill of trades:history.

The default window used to be min(count * 3, 1500) — 150 records at count=50.
A tail of entry_rejected / setup_evidence_imported / shadow rows pushed the
last position_closed out of that window. These tests use the in-memory state
backend and never touch a broker.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

import main_bot
import trade_ledger


def _run(coro):
    return asyncio.run(coro)


def _close(ticker: str, ts: str, pct: float = 1.25):
    return (
        "position_closed",
        {"ticker": ticker, "closed_at": ts, "realized_pnl_pct": pct},
        ts,
    )


def _noise(i: int):
    kind = ("entry_rejected", "setup_evidence_imported", "shadow_entry")[i % 3]
    ts = f"2026-11-23T14:{(i // 60) % 24:02d}:{(i % 60):02d}+00:00"
    return (kind, {"ticker": "ZZZ", "n": i}, ts)


def _init_ledger(path: Path, events):
    lines = []
    for seq, (event, data, ts) in enumerate(events, start=1):
        lines.append(json.dumps({
            "seq": seq,
            "ts": ts,
            "event": event,
            "data": data,
            "prev_hash": "0" * 64,
            "hash": "ab" * 32,
        }))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    trade_ledger.init(path)
    return lines


def _history():
    async def read():
        raw = await main_bot.state.list_range(
            main_bot.TRADES_HISTORY_KEY, main_bot.TRADES_HISTORY_MAX,
        )
        return [json.loads(item) for item in raw]
    return _run(read())


@pytest.fixture
def trades_env(tmp_path, monkeypatch):
    saved = {"ledger": trade_ledger._ledger, "state": main_bot.state}
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "test-secret")
    main_bot.state = main_bot.StateStore(None)
    try:
        yield tmp_path / "trade_ledger.jsonl"
    finally:
        trade_ledger._ledger = saved["ledger"]
        main_bot.state = saved["state"]


def test_noisy_tail_still_returns_closes_at_the_default_count(trades_env):
    # Three closes, then more than 150 non-close rows. The old default scan
    # (count * 3 == 150) stopped inside the noise and returned nothing.
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00", 0.5),
        _noise(1),
        _close("MSFT", "2026-11-19T15:00:00+00:00", -0.4),
        _noise(2),
        _close("UBER", "2026-11-20T20:40:16+00:00", 1.1),
    ]
    events.extend(_noise(i) for i in range(3, 203))
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))

    assert out["source"] == "ledger"
    assert out["count"] == 3
    assert [row["ticker"] for row in out["trades"]] == ["UBER", "MSFT", "AAPL"]
    assert [row["ledger_seq"] for row in out["trades"]] == [5, 3, 1]
    assert out["trades"][0]["ledger_ts"] == "2026-11-20T20:40:16+00:00"


def test_ledger_fallback_stops_at_the_cap(trades_env, monkeypatch):
    events = [_close("UBER", "2026-11-20T20:40:16+00:00")]
    events.extend(_noise(i) for i in range(40))
    _init_ledger(trades_env, events)
    monkeypatch.setattr(main_bot, "TRADES_LEDGER_FALLBACK_CAP", 20)

    hidden = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))
    assert hidden["count"] == 0
    assert hidden["source"] == "ledger"

    monkeypatch.setattr(main_bot, "TRADES_LEDGER_FALLBACK_CAP", 3000)
    found = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))
    assert found["count"] == 1
    assert found["trades"][0]["ticker"] == "UBER"


def test_backfill_pushes_oldest_first_and_reads_newest_first(trades_env, monkeypatch):
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _noise(1),
        _close("MSFT", "2026-11-19T15:00:00+00:00"),
        _noise(2),
        _close("UBER", "2026-11-20T20:40:16+00:00"),
    ]
    _init_ledger(trades_env, events)
    seen = {}
    real = main_bot.state.list_fill_if_empty

    async def spy(key, values, max_len=200):
        seen["key"] = key
        seen["values"] = list(values)
        return await real(key, values, max_len)

    monkeypatch.setattr(main_bot.state, "list_fill_if_empty", spy)

    result = _run(main_bot.backfill_trades_history())

    assert result == {"status": "filled", "rows": 3}
    assert seen["key"] == main_bot.TRADES_HISTORY_KEY
    pushed = [json.loads(raw)["ticker"] for raw in seen["values"]]
    assert pushed == ["AAPL", "MSFT", "UBER"]
    assert [row["ticker"] for row in _history()] == ["UBER", "MSFT", "AAPL"]
    assert [row["ledger_seq"] for row in _history()] == [5, 3, 1]


def test_backfill_is_idempotent_and_does_not_refill(trades_env):
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", "2026-11-20T20:40:16+00:00"),
    ]
    _init_ledger(trades_env, events)

    first = _run(main_bot.backfill_trades_history())
    second = _run(main_bot.backfill_trades_history())
    third = _run(main_bot.backfill_trades_history())
    rows = _history()

    assert first["status"] == "filled"
    assert second["status"] == "skipped"
    assert second["reason"] == "not_empty"
    assert third["status"] == "skipped"
    assert [row["ledger_seq"] for row in rows] == [2, 1]
    assert len({row["ledger_seq"] for row in rows}) == len(rows)


def test_backfill_leaves_a_non_empty_list_alone(trades_env):
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", "2026-11-20T20:40:16+00:00"),
    ]
    _init_ledger(trades_env, events)
    seed = json.dumps({"ticker": "SEED", "ledger_seq": 99, "note": "already there"})

    async def seed_and_run():
        await main_bot.state.list_push_capped(
            main_bot.TRADES_HISTORY_KEY, seed, max_len=main_bot.TRADES_HISTORY_MAX,
        )
        return await main_bot.backfill_trades_history()

    result = _run(seed_and_run())
    rows = _history()

    assert result["status"] == "skipped"
    assert result["reason"] == "not_empty"
    assert rows == [{"ticker": "SEED", "ledger_seq": 99, "note": "already there"}]


def test_concurrent_backfill_writes_one_copy(trades_env):
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("MSFT", "2026-11-19T15:00:00+00:00"),
        _close("UBER", "2026-11-20T20:40:16+00:00"),
    ]
    _init_ledger(trades_env, events)

    async def slow():
        await asyncio.sleep(0.02)

    async def scenario():
        return await asyncio.gather(
            main_bot.backfill_trades_history(before_commit=slow),
            main_bot.backfill_trades_history(before_commit=slow),
            main_bot.backfill_trades_history(before_commit=slow),
        )

    results = _run(scenario())
    rows = _history()

    assert sum(1 for r in results if r["status"] == "filled") == 1
    assert {r["status"] for r in results} <= {"filled", "skipped"}
    assert [row["ticker"] for row in rows] == ["UBER", "MSFT", "AAPL"]
    assert len({row["ledger_seq"] for row in rows}) == 3


def test_live_close_during_backfill_skips_and_does_not_duplicate(trades_env):
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", "2026-11-20T20:40:16+00:00", 2.0),
    ]
    _init_ledger(trades_env, events)
    live = {
        "ticker": "UBER",
        "closed_at": "2026-11-20T20:40:16+00:00",
        "realized_pnl_pct": 2.0,
        "ledger_seq": 2,
        "origin": "live",
    }

    async def inject():
        await main_bot.state.list_push_capped(
            main_bot.TRADES_HISTORY_KEY,
            json.dumps(live),
            max_len=main_bot.TRADES_HISTORY_MAX,
        )

    result = _run(main_bot.backfill_trades_history(before_commit=inject))
    rows = _history()

    assert result["status"] == "skipped"
    assert result["reason"] == "not_empty"
    assert rows == [live]


def test_live_review_push_does_not_duplicate_a_backfilled_seq(trades_env):
    ts = "2026-11-20T20:40:16+00:00"
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", ts, 2.0),
    ]
    _init_ledger(trades_env, events)

    async def scenario():
        filled = await main_bot.backfill_trades_history()
        before = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        await main_bot._remember_trade_review({
            "ticker": "UBER",
            "closed_at": ts,
            "realized_pnl_pct": 2.0,
        })
        after = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        return filled, before, after

    filled, before, after = _run(scenario())
    rows = _history()

    assert filled["status"] == "filled"
    assert before == after == 2
    assert [row["ticker"] for row in rows] == ["UBER", "AAPL"]
    assert [row["ledger_seq"] for row in rows] == [2, 1]


def test_backfill_writes_only_trades_history(trades_env, monkeypatch):
    events = [
        _close("UBER", "2026-11-20T20:40:16+00:00", 1.1),
        _noise(1),
    ]
    _init_ledger(trades_env, events)
    blob = trades_env.read_bytes()

    async def fail(*_args, **_kwargs):
        raise AssertionError("backfill touched a risk or ledger write path")

    monkeypatch.setattr(main_bot, "_record_close", fail)
    monkeypatch.setattr(main_bot, "_announce_learning_transitions", fail)
    monkeypatch.setattr(trade_ledger, "record", fail)
    monkeypatch.setattr(main_bot.state, "incr_trades_today", fail)
    monkeypatch.setattr(main_bot.state, "add_realized_pnl", fail)
    monkeypatch.setattr(main_bot.state, "add_realized_usd", fail)
    monkeypatch.setattr(main_bot.state, "adjust_balance", fail)

    async def seed():
        mem = main_bot.state._memory
        await mem.hincrbyfloat("daily:2026-11-24", "trades_today", 4, 10_000)
        await mem.hincrbyfloat("daily:2026-11-24", "realized_pnl_today_pct", -1.5, 10_000)
        await mem.hincrbyfloat("daily_et:2026-11-24", "trades_today", 4, 10_000)
        await mem.hincrbyfloat("daily_et:2026-11-24", "realized_pnl_today_usd", -25, 10_000)
        await main_bot.state.set("killed", "1")
        await main_bot.state.set("policy:app", json.dumps({"risk_per_trade_pct": 5}))
        await main_bot.state.set("edge:reinstated", "{}")
        await mem.sadd("open_positions:index", "UBER")

    _run(seed())

    def snap():
        mem = main_bot.state._memory
        return (
            {k: v[0] for k, v in mem._kv.items()},
            {k: dict(fields) for k, (fields, _exp) in mem._hashes.items()},
            {k: set(members) for k, members in mem._sets.items()},
        )

    before = snap()
    result = _run(main_bot.backfill_trades_history())
    after = snap()

    assert result["status"] == "filled"
    assert trades_env.read_bytes() == blob
    added = set(after[0]) - set(before[0])
    removed = set(before[0]) - set(after[0])
    assert added == {main_bot.TRADES_HISTORY_KEY}
    assert removed == set()
    for key, value in before[0].items():
        assert after[0][key] == value
    assert after[1] == before[1]
    assert after[2] == before[2]
    assert "daily:2026-11-24" in after[1]
    assert "daily_et:2026-11-24" in after[1]


def test_backfill_schedule_swallows_errors(monkeypatch):
    async def boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(main_bot, "backfill_trades_history", boom)

    async def scenario():
        main_bot._schedule_trades_history_backfill()
        await asyncio.sleep(0)
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        await asyncio.gather(*pending)

    _run(scenario())
