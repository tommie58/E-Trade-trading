"""Session VWAP is the scanner's own samples. Massive is only the cross-check.

2026-11-25: SPY's sample sat about 42 bps under the Massive RTH minute VWAP
because each volume delta was priced at one snapshot. The opening print is
an anchor. Later volume is the average of the two bracketing sample prices.
A fresh but wrong Massive value still fail-closes.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import main_bot


ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 25, 10, 26, 30, tzinfo=ET)
# Brackets from 9:30. Volume is heaviest on the first drop, the same shape
# as a sample that landed near 700.3 while the path averaged near 704.
PRICES = [710.0, 700.4, 700.2, 700.1, 700.0]
DELTAS = [120_000.0, 20_000.0, 10_000.0, 8_000.0]
MINUTES = [9 * 60 + 30, 9 * 60 + 40, 9 * 60 + 50, 10 * 60, 10 * 60 + 10]


def _samples():
    rows = []
    volume = 0.0
    for minute, price in zip(MINUTES, PRICES):
        if rows:
            volume += DELTAS[len(rows) - 1]
        else:
            volume = 200.0
        when = datetime(2026, 11, 25, minute // 60, minute % 60, tzinfo=ET)
        rows.append({
            "ts": when.isoformat(),
            "last": price,
            "total_volume": volume,
        })
    return rows


def _end_weighted(samples):
    """The old rule: the later snapshot prices the whole delta."""
    num = den = 0.0
    prev_vol = None
    for sample in samples:
        price = float(sample["last"])
        vol = float(sample["total_volume"])
        if prev_vol is not None and vol > prev_vol:
            delta = vol - prev_vol
            num += price * delta
            den += delta
        prev_vol = vol
    return num / den


# Independently computed from PRICES and DELTAS. Not derived by calling
# sampled_vwap. End-price weighting of this series is 700.3354. The bracket
# average is 703.9994. Switching the accumulator back to the later price
# alone fails the 703.9994 assertion.
EXPECTED_END_WEIGHTED = 700.3354
EXPECTED_BRACKET = 703.9994


def test_bracketed_samples_stay_inside_the_ceiling_on_todays_shape():
    samples = _samples()
    end_weighted = _end_weighted(samples)
    bracketed = main_bot.scanner_mod.sampled_vwap(samples)
    assert end_weighted == pytest.approx(EXPECTED_END_WEIGHTED, abs=0.001)
    assert bracketed == pytest.approx(EXPECTED_BRACKET, abs=0.001)
    end_gap = abs(EXPECTED_BRACKET - end_weighted) / EXPECTED_BRACKET * 10000.0
    bracket_gap = abs(EXPECTED_BRACKET - bracketed) / EXPECTED_BRACKET * 10000.0
    assert end_gap > 25
    assert bracket_gap <= 25
    # The published 10:26 ET pair was 700.38 vs 703.44, about 44 bps.
    published = main_bot._vwap_diff_bps(703.4367, 700.3759)
    assert abs(published) > 25

    folded = None
    for sample in samples:
        folded = main_bot.scanner_mod.fold_session_sample(folded, sample, NOW)
    assert folded["sum_pv"] / folded["sum_v"] == pytest.approx(bracketed, abs=0.02)
    # The 9:30 print is stored and does not price the following volume alone.
    assert folded["count"] == len(DELTAS)


def test_a_fresh_bad_massive_feed_still_fail_closes():
    samples = _samples()
    sampled = main_bot.scanner_mod.sampled_vwap(samples)
    agreed = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": sampled, "reason": None},
        {"vwap": sampled, "reason": None, "age_seconds": 30.0},
    )
    assert agreed["measured"] is True
    assert abs(agreed["diff_bps"]) < 1

    bad = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": sampled, "reason": None},
        {"vwap": 730.0, "reason": None, "age_seconds": 20.0},
    )
    assert bad["measured"] is False
    assert bad["reason"] == "crosscheck_gap_bps"
    assert abs(bad["diff_bps"]) > 25

    stale = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": sampled, "reason": None},
        {"vwap": None, "reason": "massive_rth_stale", "age_seconds": 400.0},
    )
    assert stale["measured"] is False
    assert stale["reason"] == "massive_rth_stale"


def test_open_sample_is_not_replaced_by_massive_bars():
    first = datetime(2026, 11, 25, 9, 30, tzinfo=ET)
    fields = {
        "sum_pv": repr(700.3759 * 100_000.0),
        "sum_v": repr(100_000.0),
        "count": "27",
        "first_ts": first.isoformat(),
        "last_ts": datetime(2026, 11, 25, 10, 25, tzinfo=ET).isoformat(),
        "last_volume": repr(8_000_000.0),
        "last_price": repr(700.3759),
    }
    assert main_bot.scanner_mod.session_vwap_needs_seed(fields, NOW) is False
    bars = [{
        "t": int(first.timestamp() * 1000),
        "vw": 703.4367,
        "v": 1000,
    }]
    plan = main_bot.scanner_mod.plan_session_vwap_seed(fields, bars, NOW, max_age_s=300)
    assert plan["applied"] is False
    assert plan["reason"] == "not_needed"
    assert plan["fields"] is None
