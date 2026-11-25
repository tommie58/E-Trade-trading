#!/usr/bin/env python3
"""Replay sampled VWAP, bar VWAP, the QQQ 648 bad tick, and the official gap.

Reads fixtures/data_quality/spy_qqq_session.json. The bars are synthetic
prices shaped like the public Nov 24 session. No account data and no API key.
A 403 dependency is shown as status=no_data. The sampled VWAP is not copied
into that row.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pro_trader.data_quality as dq  # noqa: E402


FIXTURE = ROOT / "fixtures" / "data_quality" / "spy_qqq_session.json"


def _load():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _print_symbol(name, block, now):
    bars = block["bars"]
    view = dq.completed_rth_vwap(bars, now)
    samples = block.get("samples") or []
    # Bracket VWAP, same rule as the scanner: the first print only anchors.
    num = den = 0.0
    prev_px = prev_vol = None
    for sample in samples:
        price = float(sample["last"])
        vol = float(sample["total_volume"])
        if prev_px is not None and vol > prev_vol:
            weight = (prev_px + price) / 2.0
            delta = vol - prev_vol
            num += weight * delta
            den += delta
        prev_px, prev_vol = price, vol
    sampled = round(num / den, 4) if den > 0 else None
    diff = dq.diff_bps(view.get("vwap"), sampled)
    print(f"[{name}] bar_vwap={view.get('vwap')} bars={view.get('bar_count')} "
          f"age_s={view.get('age_seconds')} sampled_vwap={sampled} diff_bps={diff}")
    ticks = dq.filter_bad_ticks(bars, name)
    print(f"[{name}] bad_ticks={ticks['flagged_count']}")
    for item in ticks["flagged"]:
        print(f"[{name}] bad_tick field={item['field']} high={item['high']} "
              f"low={item['low']} clamped_high={item['clamped_high']} "
              f"clamped_low={item['clamped_low']}")
    official = dq.snapshot_gap_pct(block.get("official_open"), block.get("official_prev_close"))
    current = dq.snapshot_gap_pct(block.get("current_open"), block.get("current_prev_close"))
    first = dq.snapshot_gap_pct(block.get("first_bar_open"), block.get("official_prev_close"))
    print(f"[{name}] gap_snapshot={official} gap_current={current} "
          f"first_bar_open_gap={first} (first bar is not the snapshot input)")
    dq.note_bars_feed({"status": dq.SHADOW_OK, "reason": None})
    dq.note_iv_feed({"status": dq.SHADOW_OK, "reason": None})
    dq.note_symbol_bars(name, {"status": dq.SHADOW_OK, "reason": None})
    ok_row = dq.publish_vwap_row(name, {
        "sampled_vwap": sampled,
        "gate_vwap": sampled,
        "bar_vwap": view.get("vwap"),
        "bar_age_s": view.get("age_seconds"),
        "bar_count": view.get("bar_count"),
    })
    print(f"[{name}] ledger status={ok_row['status']} bar_vwap={ok_row['bar_vwap']} "
          f"diff_bps={ok_row['diff_bps']}")
    dq.note_bars_feed(dq.classify_http_dependency(403, has_data=False))
    denied = dq.publish_vwap_row(name, {
        "sampled_vwap": sampled,
        "gate_vwap": sampled,
        "bar_vwap": view.get("vwap"),
        "bar_age_s": view.get("age_seconds"),
        "bar_count": view.get("bar_count"),
    })
    print(f"[{name}] ledger status={denied['status']} reason={denied['reason']} "
          f"bar_vwap={denied['bar_vwap']} sampled_vwap={denied['sampled_vwap']}")


def main() -> None:
    payload = _load()
    now = datetime.fromisoformat(payload["as_of"])
    print("DATA QUALITY REPLAY")
    print(payload.get("note") or "")
    print(f"as_of={payload['as_of']}")
    budget = dq.call_budget()
    print(
        "agg budget now "
        f"{budget['current_requests_per_hour']}/hour "
        f"({budget['current_observed_spacing_s']}s, one GET per symbol); "
        f"at 60s {budget['proposal_requests_per_hour']}/hour"
    )
    for name, block in payload["symbols"].items():
        _print_symbol(name, block, now)


if __name__ == "__main__":
    main()
