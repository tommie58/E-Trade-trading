"""App-link journal count. Reporting only — no order, no stop, no heat.

2026-11-25 15:26 UTC the heartbeat compared as 0 open positions against a
book of 4. The integer open_positions was 0 and no legs were sent. INTC
had been open since 15:01, so this was not the 9:24 fills waiting on the
next heartbeat. The book rows have no status field. positions_open is the
list the journal can map, and a closed status is not an open row.
"""
from __future__ import annotations

import main_bot


def _book():
    def pos(right: str):
        return {
            "qty": 1,
            "action": "BUY",
            "instrument": "option",
            "contract": {"right": right},
            "origin": "scanner",
        }
    return {
        "AVGO": pos("PUT"),
        "INTC": pos("PUT"),
        "PLTR": pos("PUT"),
        "TSLA": pos("PUT"),
    }


def _beat(**extra):
    beat = {
        "received_epoch": 1_000.0,
        "mode": "live",
        "armed": True,
        "open_positions": 0,
    }
    beat.update(extra)
    return beat


def _kinds(result):
    return [row["kind"] for row in result["divergences"]]


def test_count_of_zero_without_legs_is_the_live_mismatch():
    result = main_bot.assess_app_link(_beat(), _book(), True, False, 1_010.0, stale_after=210)
    assert "position_mismatch" in _kinds(result)
    detail = next(row["detail"] for row in result["divergences"] if row["kind"] == "position_mismatch")
    assert "journals 0" in detail
    assert "holds 4" in detail


def test_positions_open_list_is_the_journal_and_closed_rows_do_not_count():
    rows = [
        {"symbol": "AVGO", "qty": 1, "side": "short", "status": "open"},
        {"symbol": "INTC", "qty": 1, "side": "short", "status": "open"},
        {"symbol": "PLTR", "qty": 1, "side": "short", "status": "OPEN"},
        {"symbol": "TSLA", "qty": 1, "side": "short", "status": "open"},
        {"symbol": "QQQ", "qty": 1, "side": "long", "status": "closed"},
    ]
    result = main_bot.assess_app_link(
        _beat(positions_open=rows), _book(), True, False, 1_010.0, stale_after=210,
    )
    assert "position_mismatch" not in _kinds(result)
    assert main_bot._positions_open_count(rows) == 4


def test_leg_aliases_match_the_book():
    legs = [
        {"ticker": "AVGO", "contracts": 1, "status": "open"},
        {"underlying": "INTC", "quantity": 1},
        {"symbol": "PLTR", "qty": 1, "status": "open"},
        {"ticker": "TSLA", "contracts": 1, "status": "cancelled"},
        {"ticker": "TSLA", "qty": 1, "status": "open"},
    ]
    parsed = main_bot._parse_app_legs(legs)
    assert parsed == {"AVGO": 1, "INTC": 1, "PLTR": 1, "TSLA": 1}
    result = main_bot.assess_app_link(
        _beat(open_positions=0, legs=legs), _book(), True, False, 1_010.0, stale_after=210,
    )
    assert "position_mismatch" not in _kinds(result)
    assert "contract_mismatch" not in _kinds(result)


def test_positions_open_view_marks_puts_short_and_open():
    rows = main_bot.positions_open_view(_book())
    assert len(rows) == 4
    assert {row["symbol"] for row in rows} == {"AVGO", "INTC", "PLTR", "TSLA"}
    assert all(row["status"] == "open" for row in rows)
    assert all(row["side"] == "short" for row in rows)
    assert all(row["qty"] == 1 for row in rows)
