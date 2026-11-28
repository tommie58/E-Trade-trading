"""Why /pro-trader/report, /trades, and /learning disagree, and where they must not.

The ledger is not rewritten. Inclusion differences that are definitions stay
visible in the reconciliation block. Differences that were bugs (a shorter
raw window, unpriced closes in the realized sum, seq-only dedupe) are closed.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import main_bot
import pro_trader
import trade_ledger


def _rec(seq, event, data, ts):
    return {"seq": seq, "ts": ts, "event": event, "data": data, "prev_hash": "0", "hash": "1"}


def _close(seq, ticker, realized, account, ts, **extra):
    data = {
        "ticker": ticker,
        "realized_pnl_pct": realized,
        "account_pnl_pct": account,
        "entry": 1.0,
        "exit": 1.1,
    }
    data.update(extra)
    return _rec(seq, "position_closed", data, ts)


def _causes(block):
    return {item["cause"]: item for item in block["difference_by_cause"]}


def test_zero_records_reconcile_to_zero():
    block = pro_trader.reconcile_report_sources([], history_rows=[], history_read=True, window=3000)
    sources = block["sources"]
    assert sources["pro_trader_report"]["count"] == 0
    assert sources["pro_trader_report"]["realized_pnl_pct_sum"] == 0.0
    assert sources["learning"]["count"] == 0
    assert sources["learning"]["realized_pnl_pct_sum"] == 0.0
    assert sources["trades"]["count"] == 0
    assert sources["trades"]["account_pnl_pct_sum"] == 0.0
    assert sources["trades"]["served_from"] == "ledger"
    assert block["reconciled_difference"]["checks_out"] is True
    assert "EDGE_LOOKBACK" in sources["pro_trader_report"]["inclusion_rule"]
    assert "account_pnl_pct" in sources["trades"]["inclusion_rule"]
    assert "live_closes" in sources["learning"]["inclusion_rule"]
    causes = _causes(block)
    for name in (
        "price_unknown", "unmeasured_realized_pnl", "missing_timestamp",
        "duplicate_identity", "partial_stop_fill", "shadow_closed",
        "flatten_partial_unpriced", "account_vs_realized", "window",
    ):
        assert causes[name]["count"] == 0 or name in {"account_vs_realized", "window"}


def test_partial_duplicate_and_definition_gaps_are_named():
    ts = "2026-11-20T20:40:16+00:00"
    ts_b = "2026-11-20T20:41:00+00:00"
    records = [
        _close(1, "AAA", -3.0, -3.4, ts),
        _close(1, "AAA", -3.0, -3.4, ts),
        _close(2, "BBB", -1.5, -1.2, ts_b),
        _close(3, "CCC", -9.0, -9.0, "2026-11-20T20:42:00+00:00", price_unknown=True, exit=None),
        _close(4, "DDD", None, None, "2026-11-20T20:43:00+00:00"),
        _close(5, "EEE", -0.5, -0.1, ""),
        _rec(6, "partial_stop_fill", {"ticker": "FFF", "realized_pnl_pct": -0.2, "account_pnl_pct": -0.2}, ts),
        _rec(7, "shadow_closed", {"ticker": "GGG", "realized_pnl_pct": -4.0}, ts),
        _rec(8, "flatten_partial_unpriced", {"ticker": "HHH", "price_unknown": True}, ts),
        _close(9, "III", None, -0.05, "2026-11-20T20:44:00+00:00", pnl_pct=-0.25),
    ]
    # The unmeasured row stored realized None, then the alias row also passes
    # realized None via _close before extra overwrites. Force the alias row.
    records[-1]["data"].pop("realized_pnl_pct")
    block = pro_trader.reconcile_report_sources(records, history_rows=[], history_read=True, window=3000)
    sources = block["sources"]
    report = sources["pro_trader_report"]
    learning = sources["learning"]
    # AAA once, BBB, EEE (no timestamp), III (alias). Duplicate AAA is not a second report row.
    assert report["count"] == 4
    assert report["realized_pnl_pct_sum"] == -5.25
    # AAA, AAA again, BBB. EEE has no timestamp. III has no realized_pnl_pct.
    assert learning["count"] == 3
    assert learning["realized_pnl_pct_sum"] == -7.5
    assert sources["trades"]["served_from"] == "ledger"
    # Unique position_closed: AAA, BBB, CCC, DDD, EEE, III.
    assert sources["trades"]["ledger"]["count"] == 6
    diff = block["reconciled_difference"]
    assert diff["checks_out"] is True
    assert diff["learning_minus_report"] == learning["count"] - report["count"]
    assert diff["trades_ledger_minus_report"] == (
        diff["trades_ledger_explained_by_unpriced_or_unmeasured"]
    )
    causes = _causes(block)
    assert causes["price_unknown"]["count"] == 1
    assert causes["price_unknown"]["counts"]["pro_trader_report"] == 0
    assert causes["price_unknown"]["counts"]["learning"] == 0
    assert causes["unmeasured_realized_pnl"]["count"] == 1
    assert causes["missing_timestamp"]["count"] == 1
    assert causes["missing_timestamp"]["counts"]["learning"] == 0
    assert causes["duplicate_identity"]["count"] == 1
    assert causes["duplicate_identity"]["counts"]["learning"] == 1
    assert causes["duplicate_identity"]["counts"]["pro_trader_report"] == 0
    assert causes["partial_stop_fill"]["count"] == 1
    assert causes["partial_stop_fill"]["account_pnl_pct_sum"] == -0.2
    assert causes["shadow_closed"]["count"] == 1
    assert causes["flatten_partial_unpriced"]["count"] == 1
    assert causes["alias_pnl_field"]["count"] == 1
    assert causes["account_vs_realized"]["realized_pnl_pct_sum"] == report["realized_pnl_pct_sum"]
    assert "different units" in causes["account_vs_realized"]["detail"]
    assert "1200" in causes["window"]["detail"]

    corrected = pro_trader.report_closes(records)
    built = pro_trader.build_report([], corrected)
    assert built["realized_pnl_pct_sum"] == report["realized_pnl_pct_sum"]
    learning_trades = main_bot.edge.edge_trades(pro_trader.reconcile_reports.apply_sign_corrections(records))
    assert len(learning_trades) == learning["count"]


def test_sign_correction_and_reused_seq_do_not_hide_a_different_close():
    ts = "2026-11-20T20:40:16+00:00"
    later = "2026-11-21T15:00:00+00:00"
    records = [
        _close(1, "AAA", -4.0, -1.0, ts),
        _rec(2, "pnl_sign_corrected", {
            "targets": [{
                "seq": 1, "ts": ts, "ticker": "AAA",
                "now": {"realized_pnl_pct": 4.0, "account_pnl_pct": 1.0},
            }],
        }, later),
        _close(1, "BBB", -0.5, -0.2, later),
    ]
    block = pro_trader.reconcile_report_sources(records, history_read=False, window=3000)
    report = block["sources"]["pro_trader_report"]
    assert report["count"] == 2
    assert report["realized_pnl_pct_sum"] == 3.5
    assert block["sources"]["learning"]["count"] == 2
    assert _causes(block)["sign_correction"]["count"] == 1
    assert block["reconciled_difference"]["checks_out"] is True
    # Same correction through the bot's own applicator matches.
    bot_fixed = main_bot.apply_pnl_sign_corrections(records)
    again = pro_trader.reconcile_report_sources(bot_fixed, history_read=True, history_rows=[], window=3000)
    assert again["sources"]["pro_trader_report"]["realized_pnl_pct_sum"] == 3.5


def test_history_list_is_the_trades_source_and_keeps_account_percent():
    ts = "2026-11-20T20:40:16+00:00"
    records = [
        _close(1, "AAA", -6.804, -4.0, ts),
        _close(2, "BBB", -1.0, -3.2017, "2026-11-20T21:00:00+00:00"),
    ]
    history = [
        {"ticker": "BBB", "ledger_seq": 2, "closed_at": "2026-11-20T21:00:00+00:00",
         "realized_pnl_pct": -1.0, "account_pnl_pct": -3.2017},
        {"ticker": "AAA", "ledger_seq": 1, "closed_at": ts,
         "realized_pnl_pct": -6.804, "account_pnl_pct": -4.0},
        {"ticker": "AAA", "ledger_seq": 1, "ledger_ts": ts, "closed_at": ts,
         "realized_pnl_pct": -6.804, "account_pnl_pct": -4.0},
    ]
    block = pro_trader.reconcile_report_sources(
        records, history_rows=history, history_read=True, window=3000,
    )
    trades = block["sources"]["trades"]
    assert trades["served_from"] == "history"
    assert trades["count"] == 2
    assert trades["account_pnl_pct_sum"] == -7.2017
    assert trades["realized_pnl_pct_sum"] == -7.804
    assert trades["ledger"]["count"] == 2
    assert _causes(block)["trades_history_vs_ledger"]["count"] == 0
    assert "default 50" in trades["inclusion_rule"]


def test_report_payload_uses_the_corrected_window(tmp_path, monkeypatch):
    ts = "2026-11-20T20:40:16+00:00"
    events = [
        _close(1, "AAA", -4.0, -1.25, ts),
        _rec(2, "pnl_sign_corrected", {
            "targets": [{
                "seq": 1, "ts": ts, "ticker": "AAA",
                "now": {"realized_pnl_pct": 4.0, "account_pnl_pct": 1.25},
                "entry_action": "BUY",
            }],
        }, "2026-11-20T21:00:00+00:00"),
        _close(3, "CCC", -9.0, -9.0, "2026-11-20T21:05:00+00:00", price_unknown=True, exit=None),
        _rec(4, "pro_trader_decision", {"verdict": "ALLOW", "regime": "TREND", "playbook": "orb"}, "2026-11-20T21:06:00+00:00"),
    ]
    path = tmp_path / "ledger.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in events) + "\n", encoding="utf-8")
    saved = trade_ledger._ledger
    trade_ledger.init(path)
    try:
        report = asyncio.run(main_bot._pro_trader_report_payload())
    finally:
        trade_ledger._ledger = saved
    assert report["version"] == "5.99.18-pro-day-trader"
    assert report["realized_pnl_pct_sum"] == 4.0
    recon = report["reconciliation"]
    assert recon["sources"]["pro_trader_report"]["count"] == 1
    assert recon["sources"]["pro_trader_report"]["realized_pnl_pct_sum"] == 4.0
    assert recon["sources"]["learning"]["count"] == 1
    assert recon["reconciled_difference"]["checks_out"] is True
    assert _causes(recon)["price_unknown"]["count"] == 1
    assert report["decisions"] == 1


def test_ledger_fallback_keeps_a_reused_seq_and_drops_a_true_duplicate(tmp_path, monkeypatch):
    path = tmp_path / "ledger.jsonl"
    ts_a = "2026-11-18T15:00:00+00:00"
    ts_b = "2026-11-19T15:00:00+00:00"
    rows = [
        _close(1, "AAA", 1.0, 0.1, ts_a),
        _close(1, "BBB", -2.0, -0.2, ts_b),
        _close(1, "BBB", -2.0, -0.2, ts_b),
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr(main_bot, "_active_ledger_path", lambda: path)
    found = main_bot.ledger_review_rows(10, 50)
    assert [(row["ticker"], row["ledger_seq"]) for row in found] == [("BBB", 1), ("AAA", 1)]
