"""GET /study stays inside a page and a time budget.

The handler used to await the study views. Those views parse every ledger
line and rescan the chain per close, so production returned 0 bytes after
the 20s and 60s client retries. These tests never open a database.
"""
from __future__ import annotations

import asyncio
import json
import time

import main_bot
import study


def _close(seq, ticker, pct, setup="ORB", basis="premium"):
    return {
        "seq": seq,
        "ts": f"2026-11-20T15:0{seq}:00+00:00",
        "event": "position_closed",
        "data": {
            "ticker": ticker,
            "setup": setup,
            "basis": basis,
            "reason": "stop",
            "realized_pnl_pct": pct,
            "realized_pnl_usd": pct,
            "peak_mfe_pct": 0.2484,
            "peak_r": 1.2,
            "peak_basis": basis,
        },
    }


def test_zero_records_are_a_complete_empty_study():
    out = study.summarize_records([], 1, 10, deadline=1e9, clock=time.monotonic, more=False, limit=10, offset=0)
    assert out["available"] is True
    assert out["truncated"] is False
    assert out["truncated_reason"] is None
    assert out["coverage"]["closes"] == 0
    assert out["coverage"]["ledger_records"] == 0
    assert out["coverage"]["labeled_pct"] is None
    assert out["coverage"]["attribution_ready"] is False
    assert out["setup_priors"] == []
    assert out["setup_performance"] == []
    assert out["setup_expectancy"] == []
    assert out["exit_reasons"] == []
    assert out["recent_closes"] == []
    for key in ("coverage", "setup_priors", "setup_performance", "setup_expectancy", "exit_reasons", "recent_closes"):
        assert key in out


def test_time_budget_stops_mid_page_and_flags_the_partial():
    records = [_close(i, "AAA", -1.0) for i in range(1, 6)]
    calls = {"n": 0}

    def clock():
        calls["n"] += 1
        return 0.0 if calls["n"] <= 2 else 50.0

    out = study.summarize_records(
        records, 1, 10, deadline=1.0, clock=clock, more=False, limit=10, offset=0,
    )
    assert out["truncated"] is True
    assert "time budget" in out["truncated_reason"]
    assert "parsed 2 of 5" in out["truncated_reason"]
    assert out["coverage"]["closes"] == 2
    assert out["coverage"]["ledger_records"] == 2
    assert out["sections_completed"]


def test_duplicate_evidence_counts_once_and_the_newest_seq_wins():
    records = [
        _close(1, "AAA", 2.0, setup="VWAP Reclaim"),
        {
            "seq": 2,
            "ts": "2026-11-20T16:00:00+00:00",
            "event": "setup_evidence_imported",
            "data": {"import_id": "imp-1", "setup": "vwap reclaim", "won": False, "pnl_basis": "underlying", "realized_pnl_pct": -3.0},
        },
        {
            "seq": 3,
            "ts": "2026-11-20T16:05:00+00:00",
            "event": "setup_evidence_imported",
            "data": {"import_id": "imp-1", "setup": "VWAP Reclaim", "won": True, "pnl_basis": "underlying", "realized_pnl_pct": 4.0},
        },
    ]
    out = study.summarize_records(
        records, 1, 10, deadline=1e9, clock=time.monotonic, more=False, limit=10, offset=0,
    )
    priors = {row["setup_key"]: row for row in out["setup_priors"]}
    assert priors["vwap reclaim"]["samples"] == 2
    assert priors["vwap reclaim"]["evidence_trades"] == 1
    assert priors["vwap reclaim"]["wins"] == 2
    expectancy = out["setup_expectancy"]
    underlying = [row for row in expectancy if row["pnl_basis"] == "underlying"]
    assert len(underlying) == 1
    assert underlying[0]["evidence_trades"] == 1
    assert underlying[0]["expectancy_pct"] == 4.0
    perf = out["setup_performance"][0]
    assert perf["avg_giveback_pct"] is not None
    assert out["recent_closes"][0]["ticker"] == "AAA"
    assert round(out["recent_closes"][0]["peak_mfe_pct"], 4) == 24.84


def test_summary_returns_before_a_slow_page_and_does_not_scan_views():
    async def slow(limit, offset):
        await asyncio.sleep(5)
        return {"records": [_close(1, "AAA", 1.0)], "more": False}

    async def explode(*_args, **_kwargs):
        raise AssertionError("study view query ran on the request")

    saved = (
        study.coverage, study.setup_priors, study.setup_performance,
        study.setup_expectancy, study.exit_reasons, study.recent_closes,
    )
    study.coverage = explode
    study.setup_priors = explode
    study.setup_performance = explode
    study.setup_expectancy = explode
    study.exit_reasons = explode
    study.recent_closes = explode

    async def run():
        started = time.monotonic()
        out = await study.summary(load_page=slow, budget_s=0.2, window=10, offset=0, recent=5, min_samples=1)
        return out, time.monotonic() - started

    try:
        out, elapsed = asyncio.run(run())
    finally:
        (
            study.coverage, study.setup_priors, study.setup_performance,
            study.setup_expectancy, study.exit_reasons, study.recent_closes,
        ) = saved
    assert elapsed < 2
    assert out["available"] is True
    assert out["truncated"] is True
    assert "time budget" in out["truncated_reason"]
    assert out["coverage"]["closes"] == 0
    assert out["page"]["limit"] == 10


def test_summary_clamps_the_page_and_reports_a_short_file_completely():
    seen = {}

    async def load(limit, offset):
        seen["limit"] = limit
        seen["offset"] = offset
        return {"records": [], "more": False}

    out = asyncio.run(study.summary(load_page=load, budget_s=2, window=999999, offset=0))
    assert seen["limit"] == study.STUDY_LEDGER_WINDOW_MAX
    assert out["truncated"] is False
    assert out["available"] is True

    async def more(limit, offset):
        return {"records": [_close(1, "AAA", -2.5)], "more": True}

    paged = asyncio.run(study.summary(load_page=more, budget_s=2, window=10, offset=0))
    assert paged["truncated"] is True
    assert "bounded ledger page" in paged["truncated_reason"]
    assert paged["coverage"]["closes"] == 1
    assert paged["page"]["returned"] == 1


def test_offset_past_the_cap_does_not_read_the_page():
    async def load(limit, offset):
        raise AssertionError("page read past the cap")

    out = asyncio.run(study.summary(
        load_page=load, budget_s=2, window=10, offset=study.STUDY_LEDGER_WINDOW_MAX,
    ))
    assert out["truncated"] is True
    assert "offset is outside" in out["truncated_reason"]
    assert out["setup_priors"] == []


def test_clock_already_expired_does_not_call_the_loader():
    calls = {"n": 0}

    def clock():
        calls["n"] += 1
        return 0.0 if calls["n"] == 1 else 100.0

    async def load(limit, offset):
        raise AssertionError("loader ran after the budget")

    out = asyncio.run(study.summary(load_page=load, budget_s=0.05, window=10, offset=0, clock=clock))
    assert out["truncated"] is True
    assert "before the ledger page was read" in out["truncated_reason"]


def test_offline_without_a_loader_stays_unavailable():
    previous = study._engine
    study._engine = None
    try:
        out = asyncio.run(study.summary(window=10, offset=0, budget_s=1))
    finally:
        study._engine = previous
    assert out["available"] is False
    assert out["truncated"] is False
    assert "error" in out


def test_sql_page_is_a_bounded_tail_and_not_the_views():
    executed = []

    class _Result:
        def fetchall(self):
            return [(1, json.dumps(_close(1, "AAA", 1.0)))]

    class _Conn:
        async def exec_driver_sql(self, sql):
            executed.append(sql)
            return _Result()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    previous = study._engine
    study._engine = _Engine()
    try:
        out = asyncio.run(study.sql_ledger_page(10, 3, 1.5))
    finally:
        study._engine = previous
    joined = " ".join(executed).lower()
    assert "limit 11" in joined
    assert "offset 3" in joined
    assert "statement_timeout" in joined
    assert "bot_trade_closes" not in joined
    assert "bot_ledger_events" not in joined
    assert "bot_setup_priors" not in joined
    assert out["more"] is False
    assert out["records"][0]["event"] == "position_closed"


def test_file_page_reads_only_the_requested_tail(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    lines = []
    for seq in range(1, 41):
        lines.append(json.dumps(_close(seq, "AAA", 0.1)))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(main_bot, "_active_ledger_path", lambda: path)
    caps = []
    real = main_bot._jsonl_tail_lines

    def wrapped(file_path, cap):
        caps.append(cap)
        return real(file_path, cap)

    monkeypatch.setattr(main_bot, "_jsonl_tail_lines", wrapped)
    page = main_bot._study_file_page(5, 0)
    assert caps == [6]
    assert [row["seq"] for row in page["records"]] == [36, 37, 38, 39, 40]
    assert page["more"] is True

    older = main_bot._study_file_page(3, 2)
    assert [row["seq"] for row in older["records"]] == [36, 37, 38]
    assert max(caps) <= study.STUDY_LEDGER_WINDOW_MAX + 1

    empty = tmp_path / "empty.jsonl"
    empty.write_bytes(b"")
    monkeypatch.setattr(main_bot, "_active_ledger_path", lambda: empty)
    assert main_bot._study_file_page(5, 0) is None


def test_endpoint_keeps_the_old_keys_and_adds_truncation(monkeypatch):
    async def load(limit, offset):
        assert limit <= study.STUDY_LEDGER_WINDOW_MAX
        return {"records": [_close(1, "MSFT", -0.4, setup="mean reversion")], "more": False}

    monkeypatch.setattr(main_bot, "_study_ledger_page", load)
    monkeypatch.setattr(main_bot, "_study_views", {"ok": True, "created": 1})
    out = asyncio.run(main_bot.study_endpoint(1, 10, 0, 25))
    assert out["available"] is True
    assert out["truncated"] is False
    assert out["views"]["ok"] is True
    assert out["coverage"]["closes"] == 1
    assert out["page"]["limit"] == 25
    assert "setup_priors" in out and "recent_closes" in out
