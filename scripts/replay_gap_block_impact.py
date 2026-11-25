#!/usr/bin/env python3
"""Replay the SPY gap entry gate on public bars since 2026-11-05.

Reads the Yahoo Finance chart API (daily bars and the 9:30 ET minute) or the
committed snapshot in scripts/data/. It does not call a broker, an order
route, or POST /policy.

Vendor change and vendor percent need Massive changeClose and
changeClosePercentage on the quote the bot saw. Those fields are not in a
public daily or minute bar, so those two checks are reported as not replayed.
They are not filled in with a guess.

    python3 scripts/replay_gap_block_impact.py           # snapshot, markdown table
    python3 scripts/replay_gap_block_impact.py --fetch   # refresh the snapshot from Yahoo
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main_bot  # noqa: E402

PR = main_bot.portfolio_risk
SNAPSHOT = Path(__file__).resolve().parent / "data" / "spy_gap_replay_2026-11-05_2026-11-25.json"
FIRST_DAY = "2026-11-05"
LAST_DAY = "2026-11-25"
RTH_MINUTES = 390  # 9:30–16:00 ET
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart/SPY"
EST = timezone(timedelta(hours=-5))  # America/New_York is EST for this whole window


def _get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _daily_bars() -> list:
    start = int(datetime(2026, 11, 3, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2026, 11, 26, tzinfo=timezone.utc).timestamp())
    url = f"{YAHOO}?period1={start}&period2={end}&interval=1d&includePrePost=false"
    chart = _get(url)["chart"]["result"][0]
    quote = chart["indicators"]["quote"][0]
    rows = []
    for i, ts in enumerate(chart["timestamp"]):
        rows.append({
            "date": datetime.fromtimestamp(ts, timezone.utc).date().isoformat(),
            "open": quote["open"][i],
            "high": quote["high"][i],
            "low": quote["low"][i],
            "close": quote["close"][i],
        })
    return rows


def _opening_minute(day: str) -> dict | None:
    y, m, d = (int(part) for part in day.split("-"))
    start = datetime(y, m, d, 14, 25, tzinfo=timezone.utc)  # 9:25 ET
    end = datetime(y, m, d, 14, 35, tzinfo=timezone.utc)
    url = (
        f"{YAHOO}?period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        "&interval=1m&includePrePost=false"
    )
    chart = _get(url)["chart"]["result"][0]
    stamps = chart.get("timestamp") or []
    quote = chart["indicators"]["quote"][0]
    for i, ts in enumerate(stamps):
        local = datetime.fromtimestamp(ts, timezone.utc).astimezone(EST)
        if local.hour == 9 and local.minute == 30:
            return {
                "open": quote["open"][i],
                "high": quote["high"][i],
                "low": quote["low"][i],
                "close": quote["close"][i],
            }
    return None


def fetch_snapshot() -> dict:
    daily = _daily_bars()
    by_date = {row["date"]: row for row in daily}
    sessions = []
    for row in daily:
        if row["date"] < FIRST_DAY or row["date"] > LAST_DAY:
            continue
        prior_dates = [item["date"] for item in daily if item["date"] < row["date"]]
        if not prior_dates:
            raise SystemExit(f"no prior close for {row['date']}")
        prior = by_date[prior_dates[-1]]
        sessions.append({
            "date": row["date"],
            "prior_close": prior["close"],
            "prior_close_date": prior["date"],
            "daily": {k: row[k] for k in ("open", "high", "low", "close")},
            "opening_minute": _opening_minute(row["date"]),
        })
    return {
        "source": "Yahoo Finance chart API, SPY, unadjusted",
        "fetched_on": "2026-11-25",
        "first_day": FIRST_DAY,
        "last_day": LAST_DAY,
        "vendor_change_quote": None,
        "vendor_percent_quote": None,
        "sessions": sessions,
    }


def _fired(verdict: dict, needle: str) -> bool:
    return needle in str(verdict.get("reason") or "")


def replay_session(row: dict) -> dict:
    """One session against the public cash bar. Vendor checks are not run."""
    daily = row["daily"]
    minute = row.get("opening_minute") or {}
    open_px = daily.get("open")
    prev = row.get("prior_close")
    # The bot's day-bar quote carries the session open. The 9:30 minute is the
    # first bar that can contain that open. Fall back to the daily range when
    # the opening minute is missing.
    last = minute.get("close", daily.get("close"))
    high = minute.get("high", daily.get("high"))
    low = minute.get("low", daily.get("low"))
    bar = PR.verify_opening_gap(
        open_px, prev, last=last, high=high, low=low, max_trust_pct=None,
    )
    trust = PR.verify_opening_gap(open_px, prev, max_trust_pct=PR.SPY_GAP_MAX_TRUST_PCT)
    combined = PR.verify_opening_gap(
        open_px, prev, last=last, high=high, low=low,
        max_trust_pct=PR.SPY_GAP_MAX_TRUST_PCT,
    )
    # Same formula on the completed daily bar, so a check that only fails
    # while the opening minute is narrow is visible separately.
    daily_bar = PR.verify_opening_gap(
        open_px, prev, last=daily.get("close"),
        high=daily.get("high"), low=daily.get("low"),
        max_trust_pct=PR.SPY_GAP_MAX_TRUST_PCT,
    )
    raw = PR.gap_pct(open_px, prev)
    fired = []
    if open_px is None or prev is None or raw is None:
        fired.append("unmeasured")
    if _fired(bar, "session bar"):
        fired.append("session_bar")
    if _fired(trust, "trust cap"):
        fired.append("trust_cap")
    verified = bool(combined.get("verified"))
    gap = combined.get("gap_pct") if verified else raw
    if not verified:
        block = RTH_MINUTES
        why = "rejected or unmeasured on the public bar, and this source has no later quote that clears it"
    elif abs(gap) + 1e-12 >= PR.SPY_GAP_PAUSE:
        block = RTH_MINUTES
        why = "verified gap at or above 2% — full regular session sit-out"
    else:
        block = 0
        why = "verified under 2% on the opening bar"
    return {
        "date": row["date"],
        "prior_close": prev,
        "open": open_px,
        "gap_pct": gap,
        "verified": verified,
        "checks": fired,
        "session_bar": "fired" if "session_bar" in fired else "clear",
        "vendor_change": "not_replayed",
        "vendor_percent": "not_replayed",
        "trust_cap": "fired" if "trust_cap" in fired else "clear",
        "unmeasured": "fired" if "unmeasured" in fired else "clear",
        "daily_bar_verified": bool(daily_bar.get("verified")),
        "opening_minute_present": bool(minute),
        "block_minutes": block,
        "block_why": why,
    }


def replay(snapshot: dict) -> list:
    return [replay_session(row) for row in snapshot["sessions"]]


def _totals(rows: list) -> list:
    names = (
        ("session_bar", "Bar range (session bar)"),
        ("vendor_change", "Vendor change mismatch"),
        ("vendor_percent", "Vendor percent mismatch"),
        ("trust_cap", "25% trust cap"),
        ("unmeasured", "Unmeasured or unverified open"),
    )
    out = []
    for key, label in names:
        if key.startswith("vendor"):
            out.append({
                "check": label,
                "sessions": "not replayed",
                "average_minutes": "not replayed",
                "longest_minutes": "not replayed",
            })
            continue
        blocked = [row for row in rows if row[key] == "fired"]
        if not blocked:
            out.append({
                "check": label,
                "sessions": 0,
                "average_minutes": None,
                "longest_minutes": None,
            })
            continue
        minutes = [row["block_minutes"] for row in blocked]
        out.append({
            "check": label,
            "sessions": len(blocked),
            "average_minutes": round(sum(minutes) / len(minutes), 1),
            "longest_minutes": max(minutes),
        })
    sit = [row for row in rows if row["verified"] and abs(row["gap_pct"]) + 1e-12 >= PR.SPY_GAP_PAUSE]
    out.append({
        "check": "Verified |gap| ≥ 2% sit-out (not a rejection check)",
        "sessions": len(sit),
        "average_minutes": None if not sit else round(sum(r["block_minutes"] for r in sit) / len(sit), 1),
        "longest_minutes": None if not sit else max(r["block_minutes"] for r in sit),
    })
    return out


def _gap_cell(row: dict) -> str:
    if row["gap_pct"] is None:
        return "unmeasured"
    flag = "" if row["verified"] else " unverified"
    return f"{row['gap_pct']:+.3f}%{flag}"


def _checks_cell(row: dict) -> str:
    if not row["checks"]:
        return "none"
    return ", ".join(row["checks"])


def _minutes_cell(row: dict) -> str:
    if row["block_minutes"] >= RTH_MINUTES:
        return f"whole session ({RTH_MINUTES} min)"
    if row["block_minutes"] == 0:
        return "0"
    return str(row["block_minutes"])


def markdown(rows: list) -> str:
    lines = [
        "| Date | Measured gap | Checks that fired | Entries blocked |",
        "| --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['date']} | {_gap_cell(row)} | {_checks_cell(row)} | {_minutes_cell(row)} |"
        )
    lines.append("")
    lines.append("| Check | Sessions blocked | Average block | Longest block |")
    lines.append("| --- | --- | --- | --- |")
    for item in _totals(rows):
        avg = item["average_minutes"]
        longest = item["longest_minutes"]
        avg_s = "—" if avg is None else (f"{avg:g} min" if isinstance(avg, (int, float)) else str(avg))
        long_s = "—" if longest is None else (
            f"{longest:g} min" if isinstance(longest, (int, float)) else str(longest)
        )
        if avg == "not replayed":
            avg_s = "not replayed"
        if longest == "not replayed":
            long_s = "not replayed"
        lines.append(f"| {item['check']} | {item['sessions']} | {avg_s} | {long_s} |")
    return "\n".join(lines)


def load_snapshot(path: Path = SNAPSHOT) -> dict:
    return json.loads(path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="refresh the Yahoo snapshot")
    args = parser.parse_args()
    if args.fetch:
        snapshot = fetch_snapshot()
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT.write_text(json.dumps(snapshot, indent=2) + "\n")
        print(f"wrote {SNAPSHOT}", file=sys.stderr)
    else:
        snapshot = load_snapshot()
    rows = replay(snapshot)
    print(markdown(rows))
    print()
    print(
        "Vendor change and vendor percent were not replayed. "
        "The public bars have no Massive changeClose or changeClosePercentage, "
        "and those fields were not estimated."
    )


if __name__ == "__main__":
    main()
