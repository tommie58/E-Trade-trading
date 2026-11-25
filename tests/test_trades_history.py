"""GET /trades ledger fallback and the empty-list backfill of trades:history.

The default window used to be min(count * 3, 1500) — 150 records at count=50.
A tail of entry_rejected / setup_evidence_imported / shadow rows pushed the
last position_closed out of that window. These tests use the in-memory state
backend and never touch a broker.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
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


def test_record_wrapper_returns_seq(trades_env):
    trade_ledger.init(trades_env)

    first = _run(trade_ledger.record("position_closed", {"ticker": "UBER"}))
    second = _run(trade_ledger.record("entry_rejected", {"ticker": "UBER"}))

    assert first == 1
    assert second == 2
    # Callers that ignore the return value still append.
    _run(trade_ledger.record("startup_reconcile", {"ok": True}))
    tail = trade_ledger.tail(3)
    assert [row["seq"] for row in tail] == [3, 2, 1]
    assert [row["event"] for row in tail] == [
        "startup_reconcile", "entry_rejected", "position_closed",
    ]


def test_record_wrapper_returns_none_on_ledger_failure(trades_env, monkeypatch):
    trade_ledger.init(trades_env)

    def boom(self, line):
        raise OSError("disk full")

    monkeypatch.setattr(trade_ledger.TradeLedger, "_append_line", boom)

    assert _run(trade_ledger.record("position_closed", {"ticker": "UBER"})) is None

    trade_ledger._ledger = None
    assert _run(trade_ledger.record("position_closed", {"ticker": "UBER"})) is None


def test_record_wrapper_returns_seq_when_mirror_fails(trades_env):
    trade_ledger.init(trades_env)
    previous = trade_ledger._mirror

    async def bad_mirror(seq, digest, line):
        raise RuntimeError("mirror down")

    trade_ledger.set_mirror(bad_mirror)
    try:
        seq = _run(trade_ledger.record("position_closed", {"ticker": "UBER"}))
    finally:
        trade_ledger.set_mirror(previous)

    assert seq == 1
    assert trade_ledger.tail(1)[0]["event"] == "position_closed"


def test_passed_seq_does_not_read_the_ledger_and_dedupes(trades_env, monkeypatch):
    ts = "2026-11-20T20:40:16+00:00"
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", ts, 2.0),
    ]
    _init_ledger(trades_env, events)
    _run(main_bot.backfill_trades_history())

    # `_remember_trade_review` swallows exceptions, so a raised read has to be
    # recorded here or a skipped push looks like a successful dedupe.
    reads = []

    def boom(*_args, **_kwargs):
        reads.append(1)
        raise AssertionError("review push read the ledger file")

    monkeypatch.setattr(main_bot, "_jsonl_tail_lines", boom)
    monkeypatch.setattr(main_bot, "_stamp_review_ledger_seq", boom)
    monkeypatch.setattr(main_bot, "_active_ledger_path", boom)
    monkeypatch.setattr(trade_ledger.TradeLedger, "recent", boom)
    monkeypatch.setattr(trade_ledger.TradeLedger, "tail", boom)

    async def scenario():
        before = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        await main_bot._remember_trade_review(
            {
                "ticker": "UBER",
                "closed_at": datetime.fromisoformat(ts),
                "realized_pnl_pct": 2.0,
                "origin": "live",
            },
            ledger_seq=2,
        )
        after = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        return before, after

    before, after = _run(scenario())
    rows = _history()

    assert reads == []
    assert before == after == 2
    assert [row["ledger_seq"] for row in rows] == [2, 1]
    assert rows[0]["ticker"] == "UBER"
    assert "origin" not in rows[0]


def test_passed_seq_stamps_a_new_close_without_reading_the_ledger(trades_env, monkeypatch):
    trade_ledger.init(trades_env)

    def boom(*_args, **_kwargs):
        raise AssertionError("review push read the ledger file")

    monkeypatch.setattr(main_bot, "_jsonl_tail_lines", boom)
    monkeypatch.setattr(main_bot, "_stamp_review_ledger_seq", boom)
    monkeypatch.setattr(main_bot, "_active_ledger_path", boom)

    _run(main_bot._remember_trade_review(
        {"ticker": "UBER", "closed_at": "2026-11-20T20:40:16+00:00"},
        ledger_seq=41,
    ))
    rows = _history()

    assert len(rows) == 1
    assert rows[0]["ledger_seq"] == 41
    assert rows[0]["ticker"] == "UBER"


def test_fallback_stamp_matches_closed_at_str_and_datetime(trades_env):
    ts = "2026-11-20T20:40:16+00:00"
    zulu = "2026-11-20T20:40:16Z"
    events = [
        _close("AAPL", "2026-11-18T15:00:00+00:00"),
        _close("UBER", ts, 2.0),
    ]
    _init_ledger(trades_env, events)
    when = datetime.fromisoformat(ts)

    stamped = main_bot._stamp_review_ledger_seq({
        "ticker": "uber",
        "closed_at": when,
        "realized_pnl_pct": 2.0,
    })
    assert stamped["ledger_seq"] == 2
    assert stamped["closed_at"] == when

    naive = main_bot._stamp_review_ledger_seq({
        "ticker": "UBER",
        "closed_at": datetime(2026, 11, 20, 20, 40, 16),
    })
    assert naive["ledger_seq"] == 2

    other = main_bot._stamp_review_ledger_seq({
        "ticker": "UBER",
        "closed_at": datetime(2026, 11, 20, 21, 0, tzinfo=timezone.utc),
    })
    assert other.get("ledger_seq") is None
    wrong_ticker = main_bot._stamp_review_ledger_seq({
        "ticker": "QQQ",
        "closed_at": when,
    })
    assert wrong_ticker.get("ledger_seq") is None

    # Rewrite the UBER line with a Z suffix and a datetime snapshot still matches.
    raw = trades_env.read_text(encoding="utf-8").replace(ts, zulu)
    trades_env.write_text(raw, encoding="utf-8")
    trade_ledger.init(trades_env)
    zulu_stamp = main_bot._stamp_review_ledger_seq({
        "ticker": "UBER",
        "closed_at": when,
    })
    assert zulu_stamp["ledger_seq"] == 2

    async def scenario():
        await main_bot.backfill_trades_history()
        before = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        await main_bot._remember_trade_review({
            "ticker": "UBER",
            "closed_at": when,
            "realized_pnl_pct": 2.0,
        })
        after = await main_bot.state.list_len(main_bot.TRADES_HISTORY_KEY)
        return before, after

    before, after = _run(scenario())
    rows = _history()

    assert before == after == 2
    assert [row["ledger_seq"] for row in rows] == [2, 1]


def test_fill_and_review_lua_against_a_lua_engine():
    pytest.importorskip("lupa")
    fakeredis_aioredis = pytest.importorskip("fakeredis.aioredis")
    try:
        client = fakeredis_aioredis.FakeRedis(decode_responses=True)
    except Exception as exc:
        pytest.skip(f"fakeredis client unavailable: {type(exc).__name__}")

    async def scenario():
        try:
            probe = await client.eval("return 1", 0)
        except Exception as exc:
            return exc
        if int(probe) != 1:
            return RuntimeError("lua probe failed")
        store = main_bot.StateStore(client)
        key = "trades:history:lua"
        empty = await store.list_fill_if_empty(key, [], 10)
        capped = await store.list_fill_if_empty(
            key, ["oldest", "mid", "newest"], 2,
        )
        again = await store.list_fill_if_empty(key, ["should-not-land"], 10)
        filled = await store.list_all(key)
        row = json.dumps({"ticker": "UBER", "ledger_seq": 7})
        pushed = await store.list_push_review(key, row, 7, 10)
        duplicate = await store.list_push_review(
            key, json.dumps({"ticker": "UBER", "ledger_seq": 7, "origin": "live"}), 7, 10,
        )
        fresh = await store.list_push_review(
            key, json.dumps({"ticker": "MSFT", "ledger_seq": 8}), 8, 10,
        )
        unsequenced = await store.list_push_review(
            key, json.dumps({"ticker": "NOSEQ"}), None, 10,
        )
        final = await store.list_all(key)
        return empty, capped, again, filled, pushed, duplicate, fresh, unsequenced, final

    result = _run(scenario())
    if isinstance(result, Exception):
        pytest.skip(f"lua engine unavailable: {type(result).__name__}: {result}")

    empty, capped, again, filled, pushed, duplicate, fresh, unsequenced, final = result
    assert empty is False
    assert capped is True
    assert again is False
    assert filled == ["newest", "mid"]
    assert pushed is True
    assert duplicate is False
    assert fresh is True
    assert unsequenced is True
    review = [json.loads(item) for item in final[:3]]
    assert [row["ticker"] for row in review] == ["NOSEQ", "MSFT", "UBER"]
    assert review[2]["ledger_seq"] == 7
    assert "origin" not in review[2]
    assert final[3:] == ["newest", "mid"]


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


def _exit(event, ticker, ts, **data):
    body = {"ticker": ticker}
    body.update(data)
    return (event, body, ts)


def test_take_profit_without_position_closed_is_a_trade(trades_env):
    # Premiums that round to the booked option formula: +13.6711% and +$714.
    events = [
        _exit(
            "take_profit_closed", "AMZN", "2026-11-05T15:22:00+00:00",
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=59.367,
            target_premium=59.0, mark=59.367,
        ),
        _noise(1),
        ("shadow_closed", {"ticker": "AMZN", "realized_pnl_pct": 99.0}, "2026-11-05T15:23:00+00:00"),
    ]
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))

    assert out["source"] == "ledger"
    assert out["count"] == 1
    row = out["trades"][0]
    assert row["ticker"] == "AMZN"
    assert row["close_event"] == "take_profit_closed"
    assert row["realized_pnl_pct"] == 13.6711
    assert row["realized_pnl_usd"] == 714.0
    assert row["entry"] == 52.227
    assert row["exit"] == 59.367
    assert row["reason"] == "take_profit"
    assert row["ledger_seq"] == 1


def test_unpriced_take_profit_stays_unpriced(trades_env):
    # The mark is what the decision saw, not a fill. It must not become P&L.
    events = [
        _exit(
            "take_profit_closed", "AMZN", "2026-11-05T15:22:00+00:00",
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=None,
            mark=59.367,
        ),
    ]
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))

    assert out["count"] == 1
    row = out["trades"][0]
    assert row["ticker"] == "AMZN"
    assert row["realized_pnl_pct"] is None
    assert row.get("realized_pnl_usd") is None
    assert row.get("exit") is None
    assert row["mark"] == 59.367


def test_take_profit_and_position_closed_count_once(trades_env):
    ts_tp = "2026-11-05T15:22:00+00:00"
    ts_booked = "2026-11-05T15:22:08+00:00"
    events = [
        ("position_opened", {"ticker": "AMZN", "signal_id": "sig-amzn", "action": "BUY"}, "2026-11-05T14:50:00+00:00"),
        _exit(
            "take_profit_closed", "AMZN", ts_tp,
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=59.367,
        ),
        _close("AMZN", ts_booked, 13.6711),
    ]
    events[-1][1]["account_pnl_pct"] = 4.25
    events[-1][1]["reason"] = "take_profit"
    events[-1][1]["entry"] = 52.227
    events[-1][1]["realized_pnl_usd"] = 714.0
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))

    assert out["count"] == 1
    row = out["trades"][0]
    assert row["ticker"] == "AMZN"
    assert row["close_event"] == "position_closed"
    assert row["ledger_seq"] == 3
    assert row["account_pnl_pct"] == 4.25
    assert row["realized_pnl_pct"] == 13.6711
    assert row["close_id"].startswith("order:")


def test_scheduled_exit_without_position_closed_is_a_trade(trades_env):
    events = [
        _exit(
            "eod_flatten_closed", "NVDA", "2026-11-05T20:40:00+00:00",
            qty=2, order_id="flatten-1",
            entry_premium=1.50, exit_premium=1.20,
        ),
        _exit(
            "eod_flatten_closed", "UBER", "2026-11-05T20:41:00+00:00",
            qty=1, order_id="flatten-2",
            entry_premium=2.0, exit_premium=1.5,
        ),
        _close("UBER", "2026-11-05T20:41:05+00:00", -25.0),
    ]
    events[-1][1]["reason"] = "eod_flatten"
    events[-1][1]["entry"] = 2.0
    events[-1][1]["entry_premium"] = 2.0
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))

    assert [row["ticker"] for row in out["trades"]] == ["UBER", "NVDA"]
    nvda = out["trades"][1]
    assert nvda["close_event"] == "eod_flatten_closed"
    assert nvda["reason"] == "eod_flatten"
    assert nvda["realized_pnl_pct"] == round((1.20 - 1.50) / 1.50 * 100.0, 4)
    assert nvda["realized_pnl_usd"] == round((1.20 - 1.50) * 100.0 * 2, 2)
    uber = out["trades"][0]
    assert uber["close_event"] == "position_closed"
    assert uber["ledger_seq"] == 3


def test_history_keeps_booked_closes_and_adds_the_missing_take_profit(trades_env):
    booked = _close("INTC", "2026-11-05T15:32:00+00:00", -3.105)
    events = [
        booked,
        _exit(
            "take_profit_closed", "AMZN", "2026-11-05T15:22:00+00:00",
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=59.367,
        ),
    ]
    _init_ledger(trades_env, events)

    async def seed():
        await main_bot.state.list_push_capped(
            main_bot.TRADES_HISTORY_KEY,
            json.dumps({
                "ticker": "INTC",
                "ledger_seq": 1,
                "ledger_ts": "2026-11-05T15:32:00+00:00",
                "closed_at": "2026-11-05T15:32:00+00:00",
                "realized_pnl_pct": -3.105,
                "origin": "history",
            }),
            max_len=main_bot.TRADES_HISTORY_MAX,
        )
        return await main_bot.trades_review(count=50, x_rork_secret="test-secret")

    out = _run(seed())

    assert out["source"] == "history"
    assert [row["ticker"] for row in out["trades"]] == ["INTC", "AMZN"]
    assert out["trades"][0]["origin"] == "history"
    assert out["trades"][1]["realized_pnl_pct"] == 13.6711
    assert out["trades"][1]["realized_pnl_usd"] == 714.0


def test_history_does_not_duplicate_a_booked_take_profit(trades_env):
    events = [
        ("position_opened", {"ticker": "AMZN", "signal_id": "sig-amzn"}, "2026-11-05T14:50:00+00:00"),
        _exit(
            "take_profit_closed", "AMZN", "2026-11-05T15:22:00+00:00",
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=59.367,
        ),
        _close("AMZN", "2026-11-05T15:22:08+00:00", 13.6711),
    ]
    events[-1][1]["reason"] = "take_profit"
    events[-1][1]["entry"] = 52.227
    _init_ledger(trades_env, events)

    async def seed():
        await main_bot.state.list_push_capped(
            main_bot.TRADES_HISTORY_KEY,
            json.dumps({
                "ticker": "AMZN",
                "ledger_seq": 3,
                "closed_at": "2026-11-05T15:22:08+00:00",
                "realized_pnl_pct": 13.6711,
                "reason": "take_profit",
                "entry": 52.227,
                "origin": "history",
            }),
            max_len=main_bot.TRADES_HISTORY_MAX,
        )
        return await main_bot.trades_review(count=50, x_rork_secret="test-secret")

    out = _run(seed())

    assert out["count"] == 1
    assert out["trades"][0]["origin"] == "history"
    assert out["trades"][0]["realized_pnl_pct"] == 13.6711


def test_nov5_amzn_gap_totals_the_day(trades_env):
    # The three Nov 5 history rows summed to -8.565%. AMZN's take-profit,
    # +13.6711% and +$714, was the missing close. Together they are +5.106%.
    events = [
        _exit(
            "take_profit_closed", "AMZN", "2026-11-05T15:22:00+00:00",
            qty=1, order_id="4241-order",
            entry_premium=52.227, exit_premium=59.367, mark=59.367,
        ),
        _close("INTC", "2026-11-05T15:32:00+00:00", -3.105),
        _close("QQQ", "2026-11-05T16:10:00+00:00", -2.86),
        _close("NVDA", "2026-11-05T18:05:00+00:00", -2.6),
        _noise(9),
    ]
    _init_ledger(trades_env, events)

    out = _run(main_bot.trades_review(count=50, x_rork_secret="test-secret"))
    nov5 = [
        row for row in out["trades"]
        if str(row.get("ledger_ts") or "").startswith("2026-11-05")
    ]
    amzn = next(row for row in nov5 if row["ticker"] == "AMZN")
    others = [row["realized_pnl_pct"] for row in nov5 if row["ticker"] != "AMZN"]

    assert out["version"] == "5.99.8-pro-day-trader"
    assert [row["ticker"] for row in nov5] == ["NVDA", "QQQ", "INTC", "AMZN"]
    assert len(nov5) == 4
    assert amzn["realized_pnl_pct"] == 13.6711
    assert amzn["realized_pnl_usd"] == 714.0
    assert amzn["ledger_seq"] == 1
    assert round(sum(others), 3) == -8.565
    assert round(sum(row["realized_pnl_pct"] for row in nov5), 3) == 5.106
