#!/usr/bin/env python3
"""Replay SETUP_DISABLE on the Nov 5-25 closes.

Source: scripts/data/closes_nov5_25_setup_pnl.csv
Columns: ticker, close_ts_ct, setup, pnl. pnl is realized gross before the
$1.04 per-close commission. No account ids.

The flag skips breakdown, vwap rejection, and momentum. Match is exact after
the same fold as the bot, so momentum does not match momentum fade.

  python3 scripts/replay_setup_disable.py
"""
from __future__ import annotations

import csv
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, List, Optional

COMMISSION = Decimal("1.04")
DEFAULT_DISABLED = ("breakdown", "vwap_rejection", "momentum")
ROOT = Path(__file__).resolve().parents[1]
TRADES = ROOT / "scripts" / "data" / "closes_nov5_25_setup_pnl.csv"
AMZN_NOV5_TS = "2026-11-05T09:22:11"
AMZN_NOV5_GROSS = Decimal("714")


def canonical_setup_id(label: object) -> str:
    """Same fold as main_bot.canonical_setup_id. Exact, not a substring."""
    text = str(label or "").strip().lower().replace("_", " ")
    text = text.replace("+ adx", "").replace("+adx", "")
    return " ".join(text.split())


def disabled_ids(tokens: Iterable[str] = DEFAULT_DISABLED) -> frozenset:
    return frozenset(canonical_setup_id(token) for token in tokens)


def _money(value: object) -> Decimal:
    return Decimal(str(value).strip())


def _fmt(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _fmt_pf(value: Optional[Decimal]) -> str:
    if value is None:
        return "inf"
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def load_closes(path: Path = TRADES) -> List[dict]:
    """Closes in close_ts_ct order. pnl stays gross."""
    rows: List[dict] = []
    with path.open(newline="") as handle:
        for raw in csv.DictReader(handle):
            ts = str(raw["close_ts_ct"]).strip()
            rows.append({
                "ticker": str(raw["ticker"]).strip(),
                "close_ts_ct": ts,
                "close_date": ts[:10],
                "setup": canonical_setup_id(raw.get("setup")),
                "gross": _money(raw["pnl"]),
            })
    rows.sort(key=lambda row: row["close_ts_ct"])
    return rows


def is_amzn_nov5_winner(row: dict) -> bool:
    """The Nov 5 AMZN vwap-rejection take-profit of +714 gross."""
    return (
        row.get("ticker") == "AMZN"
        and row.get("close_ts_ct") == AMZN_NOV5_TS
        and row.get("setup") == "vwap rejection"
        and row.get("gross") == AMZN_NOV5_GROSS
    )


def max_drawdown(nets: Iterable[Decimal]) -> Decimal:
    """Peak-to-trough of cumulative net, starting at 0, in the given order."""
    equity = Decimal("0")
    peak = Decimal("0")
    worst = Decimal("0")
    for pnl in nets:
        equity += pnl
        if equity > peak:
            peak = equity
        drop = peak - equity
        if drop > worst:
            worst = drop
    return worst


def _profit_factor(nets: List[Decimal]) -> Optional[Decimal]:
    wins = sum((n for n in nets if n > 0), Decimal("0"))
    losses = sum((n for n in nets if n < 0), Decimal("0"))
    if losses == 0:
        return None
    return wins / abs(losses)


def summarize(closes: List[dict], disabled: frozenset, *, apply_flag: bool) -> Dict[str, object]:
    """One book. Flag on drops disabled setups. Net is gross minus $1.04 per kept close.

    Drawdown walks kept closes in close_ts_ct order. A day that exists in
    `closes` but has nothing kept prints 0.
    """
    kept: List[dict] = []
    skipped: List[dict] = []
    for row in closes:
        if apply_flag and row["setup"] in disabled:
            skipped.append(row)
        else:
            kept.append(row)
    nets = [row["gross"] - COMMISSION for row in kept]
    by_day: Dict[str, Decimal] = {}
    for row, net in zip(kept, nets):
        day = row["close_date"]
        by_day[day] = by_day.get(day, Decimal("0")) + net
    days = [
        {"close_date": day, "net": by_day.get(day, Decimal("0"))}
        for day in sorted({row["close_date"] for row in closes})
    ]
    if days:
        worst = min(days, key=lambda row: (row["net"], row["close_date"]))
        worst_date, worst_net = worst["close_date"], worst["net"]
    else:
        worst_date, worst_net = "", Decimal("0")
    return {
        "kept_n": len(kept),
        "skipped_n": len(skipped),
        "skipped": skipped,
        "net": sum(nets, Decimal("0")),
        "worst_date": worst_date,
        "worst_net": worst_net,
        "max_dd": max_drawdown(nets),
        "pf": _profit_factor(nets),
        "days": days,
    }


def _header(label: str, result: Dict[str, object]) -> None:
    print(label)
    print(f"  kept closes: {result['kept_n']}")
    print(f"  trades skipped: {result['skipped_n']}")
    print(f"  net after $1.04/close: {_fmt(result['net'])}")
    print(f"  worst day: {result['worst_date']} {_fmt(result['worst_net'])}")
    print(f"  max drawdown (close order, cumulative net from 0): {_fmt(result['max_dd'])}")
    print(f"  PF (net wins / abs net losses): {_fmt_pf(result['pf'])}")


def _print_days(without: Dict[str, object], with_flag: Dict[str, object]) -> None:
    print("per-day net after commission")
    print(f"  {'date':<12} {'without':>12} {'with':>12}")
    by_with = {row["close_date"]: row["net"] for row in with_flag["days"]}
    for row in without["days"]:
        day = row["close_date"]
        print(f"  {day:<12} {_fmt(row['net']):>12} {_fmt(by_with.get(day, Decimal('0'))):>12}")


def main() -> None:
    disabled = disabled_ids()
    closes = load_closes()
    without = summarize(closes, disabled, apply_flag=False)
    flagged = summarize(closes, disabled, apply_flag=True)
    ex = [row for row in closes if not is_amzn_nov5_winner(row)]
    ex_without = summarize(ex, disabled, apply_flag=False)
    ex_flagged = summarize(ex, disabled, apply_flag=True)

    print("setup ids disabled:", ", ".join(sorted(disabled)))
    print("match: exact canonical id. 'momentum' does not match 'momentum fade'.")
    print(f"closes: {len(closes)}  file: {TRADES.relative_to(ROOT)}")
    print("pnl in the file is gross. Net subtracts $1.04 per kept close.")
    print("Drawdown is the close-ordered cumulative net, starting at 0.")
    print("It is not account equity and does not include the Nov 12 deposit.")
    print()
    _header("WITHOUT flag", without)
    print()
    _header("WITH flag", flagged)
    print(f"  P&L delta (with - without): {_fmt(flagged['net'] - without['net'])}")
    print("  skipped:")
    for row in flagged["skipped"]:
        print(
            f"    {row['close_ts_ct']} {row['ticker']} {row['setup']} "
            f"gross {_fmt(row['gross'])}"
        )
    print()
    _print_days(without, flagged)
    print()
    print(
        "SENSITIVITY ex-AMZN 2026-11-05T09:22:11 vwap rejection gross +714 "
        "(the flag skips this close):"
    )
    print(
        "  without  "
        f"net {_fmt(ex_without['net'])}  "
        f"skipped {ex_without['skipped_n']}  "
        f"worst {ex_without['worst_date']} {_fmt(ex_without['worst_net'])}  "
        f"maxDD {_fmt(ex_without['max_dd'])}  "
        f"PF {_fmt_pf(ex_without['pf'])}"
    )
    print(
        "  with      "
        f"net {_fmt(ex_flagged['net'])}  "
        f"skipped {ex_flagged['skipped_n']}  "
        f"worst {ex_flagged['worst_date']} {_fmt(ex_flagged['worst_net'])}  "
        f"maxDD {_fmt(ex_flagged['max_dd'])}  "
        f"PF {_fmt_pf(ex_flagged['pf'])}"
    )


if __name__ == "__main__":
    main()
