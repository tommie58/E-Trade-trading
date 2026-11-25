#!/usr/bin/env python3
"""Replay the Nov 5-25 setup-disable flag against the published baseline.

The trade CSV is every close the Nov 25 Quant Lab tables name with a ticker.
Net P&L is the table's dollar figure minus the report's $1.04 commission when
the table printed a whole-dollar gross. There are no account ids.

The tables do not list all 57 closes. The setup file and the day file are the
published aggregates. This script does not invent the missing rows.

  python3 scripts/replay_setup_disable.py
"""
from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List

COMMISSION = Decimal("1.04")
DEFAULT_DISABLED = ("breakdown", "vwap_rejection", "momentum")
ROOT = Path(__file__).resolve().parents[1]
TRADES = ROOT / "scripts" / "data" / "nov5_25_closed_trades.csv"
SETUPS = ROOT / "scripts" / "data" / "nov5_25_published_setups.csv"
DAYS = ROOT / "scripts" / "data" / "nov5_25_published_days.csv"


def canonical_setup_id(label: object) -> str:
    """Same fold as main_bot.canonical_setup_id / edge.normalize_setup.

    Exact match after the fold. 'momentum' is not 'momentum fade'.
    """
    text = str(label or "").strip().lower().replace("_", " ")
    text = text.replace("+ adx", "").replace("+adx", "")
    return " ".join(text.split())


def disabled_ids(tokens: Iterable[str] = DEFAULT_DISABLED) -> frozenset:
    return frozenset(canonical_setup_id(token) for token in tokens)


def _money(value: object) -> Decimal:
    return Decimal(str(value).strip())


def load_csv(path: Path) -> List[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def max_drawdown(pnls: Iterable[Decimal]) -> Decimal:
    """Peak-to-trough of the cumulative sum. Day-close when pnls are days."""
    equity = Decimal("0")
    peak = Decimal("0")
    worst = Decimal("0")
    for pnl in pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        drop = peak - equity
        if drop > worst:
            worst = drop
    return worst


def worst_day(rows: List[dict], pnl_key: str = "net") -> tuple:
    if not rows:
        return ("", Decimal("0"))
    worst = min(rows, key=lambda row: row[pnl_key])
    return (str(worst["close_date"]), worst[pnl_key])


def replay_trades(trades: List[dict], disabled: frozenset) -> Dict[str, object]:
    kept_by_day: Dict[str, Decimal] = {}
    base_by_day: Dict[str, Decimal] = {}
    complete_days = set()
    skipped = []
    for row in trades:
        day = row["close_date"]
        net = _money(row["net_pnl"])
        setup = canonical_setup_id(row["setup"])
        base_by_day[day] = base_by_day.get(day, Decimal("0")) + net
        if str(row.get("day_complete") or "").strip().lower() == "yes":
            complete_days.add(day)
        else:
            complete_days.discard(day)
        if setup in disabled:
            skipped.append({**row, "setup_id": setup, "net": net})
            continue
        kept_by_day[day] = kept_by_day.get(day, Decimal("0")) + net
    # A day is complete only when every row for that date says so.
    for day in list(complete_days):
        if any(
            row["close_date"] == day and str(row.get("day_complete") or "").lower() != "yes"
            for row in trades
        ):
            complete_days.discard(day)
    base_days = [
        {"close_date": day, "net": base_by_day[day]}
        for day in sorted(complete_days)
    ]
    kept_days = [
        {"close_date": day, "net": kept_by_day.get(day, Decimal("0"))}
        for day in sorted(complete_days)
    ]
    base_total = sum(base_by_day.values(), Decimal("0"))
    skipped_total = sum((row["net"] for row in skipped), Decimal("0"))
    return {
        "rows": len(trades),
        "skipped": skipped,
        "skipped_n": len(skipped),
        "skipped_net": skipped_total,
        "base_net": base_total,
        "kept_net": base_total - skipped_total,
        "delta": -skipped_total,
        "complete_base_days": base_days,
        "complete_kept_days": kept_days,
    }


def replay_setup_table(rows: List[dict], disabled: frozenset) -> Dict[str, object]:
    book_gross = Decimal("0")
    book_n = 0
    skip_gross = Decimal("0")
    skip_n = 0
    for row in rows:
        n = int(row["trades"])
        gross = _money(row["gross_pnl"])
        book_gross += gross
        book_n += n
        if canonical_setup_id(row["setup"]) in disabled:
            skip_gross += gross
            skip_n += n
    book_net = book_gross - COMMISSION * book_n
    skip_net = skip_gross - COMMISSION * skip_n
    return {
        "trades": book_n,
        "book_net": book_net,
        "skipped_n": skip_n,
        "skipped_net": skip_net,
        "kept_n": book_n - skip_n,
        "kept_net": book_net - skip_net,
        "delta": -skip_net,
    }


def _fmt(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}"


def _print_days(title: str, rows: List[dict]) -> None:
    print(title)
    if not rows:
        print("  (none)")
        return
    for row in rows:
        print(f"  {row['close_date']}  {_fmt(row['net'])}")
    day, pnl = worst_day(rows)
    print(f"  worst day: {day} {_fmt(pnl)}")
    print(f"  max drawdown day-close: {_fmt(max_drawdown(row['net'] for row in rows))}")


def main() -> None:
    disabled = disabled_ids()
    trades = load_csv(TRADES)
    setups = load_csv(SETUPS)
    days = [
        {"close_date": row["close_date"], "net": _money(row["net_pnl"])}
        for row in load_csv(DAYS)
    ]
    named = replay_trades(trades, disabled)
    book = replay_setup_table(setups, disabled)
    print("setup ids disabled:", ", ".join(sorted(disabled)))
    print("match: exact canonical id. 'momentum' does not match 'momentum fade'.")
    print()
    print("=== named closes in the trade CSV ===")
    print(f"file: {TRADES.relative_to(ROOT)}")
    print(f"rows: {named['rows']} (the tables do not name all 57 closes)")
    print(f"trades skipped: {named['skipped_n']}")
    for row in named["skipped"]:
        print(
            f"  {row['close_date']} {row['ticker']} {row['setup_id']} {_fmt(row['net'])}"
        )
    print(f"named P&L without flag: {_fmt(named['base_net'])}")
    print(f"named P&L with flag: {_fmt(named['kept_net'])}")
    print(f"named P&L delta (with - without): {_fmt(named['delta'])}")
    print("kept setups momentum fade and orb stay. Every named close is already")
    print("1 contract, so the 1-contract cap does not change this sample.")
    print()
    print("=== days whose every close is in the CSV (not a full equity curve) ===")
    kept_by_date = {row["close_date"]: row["net"] for row in named["complete_kept_days"]}
    for row in named["complete_base_days"]:
        print(
            f"  {row['close_date']}  without {_fmt(row['net'])}"
            f"  with {_fmt(kept_by_date.get(row['close_date'], Decimal('0')))}"
        )
    print("These two days are not consecutive. Drawdown is not taken across the gap.")
    print()
    print("=== full book from the published setup table ===")
    print(f"closes: {book['trades']}")
    print(f"book net without flag: {_fmt(book['book_net'])}")
    print(f"trades skipped: {book['skipped_n']}")
    print(f"skipped net: {_fmt(book['skipped_net'])}")
    print(f"book net with flag: {_fmt(book['kept_net'])}")
    print(f"P&L delta (with - without): {_fmt(book['delta'])}")
    print("The 1-contract cap does not change dollars here: all 57 closes were 1 lot.")
    print()
    _print_days("=== published day curve, flag OFF (all 57 closes) ===", days)
    print()
    print("Full-book worst day and max drawdown WITH the flag are not identified.")
    print("The tables do not date every disabled close. Removing an undated")
    print("winner (AMZN is dated; other vwap-rejection winners are not) can")
    print("make a day worse, so the unpublished rows are not filled in.")
    dated_skip = named["skipped_n"]
    print(
        f"Dated disabled closes in the CSV: {dated_skip} of {book['skipped_n']}."
    )


if __name__ == "__main__":
    main()
