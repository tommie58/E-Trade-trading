"""Offline replay of the public SPY bars committed for the gap-block evidence.

The snapshot is Yahoo's daily bar and 9:30 ET minute. Refresh it with
`python3 scripts/replay_gap_block_impact.py --fetch`. This test does not
touch the network, a broker, or POST /policy.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "replay_gap_block_impact",
    ROOT / "scripts" / "replay_gap_block_impact.py",
)
replay_mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay_mod)


def test_public_sessions_since_nov5_do_not_trip_the_rejection_checks():
    rows = replay_mod.replay(replay_mod.load_snapshot())
    assert [row["date"] for row in rows][0] == "2026-11-05"
    assert rows[-1]["date"] == "2026-11-25"
    assert len(rows) == 15
    for row in rows:
        assert row["opening_minute_present"] is True
        assert row["daily_bar_verified"] is True
        assert row["session_bar"] == "clear"
        assert row["trust_cap"] == "clear"
        assert row["unmeasured"] == "clear"
        assert row["vendor_change"] == "not_replayed"
        assert row["vendor_percent"] == "not_replayed"
        assert row["checks"] == []
    nov23 = next(row for row in rows if row["date"] == "2026-11-23")
    nov25 = next(row for row in rows if row["date"] == "2026-11-25")
    assert nov23["gap_pct"] == 5.004
    assert nov23["block_minutes"] == 390
    assert nov25["gap_pct"] == -2.168
    assert nov25["block_minutes"] == 390
    quiet = [row for row in rows if row["date"] not in ("2026-11-23", "2026-11-25")]
    assert all(row["block_minutes"] == 0 for row in quiet)
    assert all(abs(row["gap_pct"]) < 2 for row in quiet)
