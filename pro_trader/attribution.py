"""Daily attribution: what the desk allowed, what it would have blocked, and
where the session leaked.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from pro_trader.corrective import build_corrective_actions

JOURNAL_KEYS = (
    "regime",
    "raw_regime",
    "confidence",
    "phase",
    "bias",
    "playbook",
    "verdict",
    "size_multiplier",
    "reason_codes",
    "summary",
    "spread_pct",
    "max_spread_pct",
    "aligned",
    "exposure",
    "mode",
    "applied",
    "index",
    "efficiency",
    "chop_ratio",
    "vol_ratio",
    "opening_range",
    "breadth",
    "feed",
    "exit_alignment",
    "hard_rails",
    "risk",
    "ticker",
    "setup",
    "score",
    "pacing",
    "playbook_id",
    "rotation",
)


def journal_metadata(decision: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """JSON-safe slice stored on the ledger and the position."""
    if not isinstance(decision, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in JOURNAL_KEYS:
        if key in decision:
            out[key] = decision.get(key)
    codes = out.get("reason_codes")
    if isinstance(codes, list):
        out["reason_codes"] = [str(c) for c in codes[:12]]
    return out


def _pnl(row: Dict[str, Any]) -> Optional[float]:
    for key in ("realized_pnl_pct", "pnl_pct", "realized_pnl"):
        raw = row.get(key)
        if isinstance(raw, bool) or raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value == value and abs(value) != float("inf"):
            return value
    return None


def build_report(
    decisions: Iterable[Dict[str, Any]],
    closes: Iterable[Dict[str, Any]] = (),
    *,
    pacing: Optional[Dict[str, Any]] = None,
    feed: Optional[Dict[str, Any]] = None,
    min_trades: int = 4,
) -> Dict[str, Any]:
    """Shadow-vs-actual attribution and the day's worst alignment leaks."""
    rows = [d for d in decisions if isinstance(d, dict)]
    by_regime: Dict[str, Dict[str, Any]] = {}
    by_playbook: Dict[str, Dict[str, Any]] = {}
    blocked = allowed = reduced = 0
    shadow_blocks_taken = 0
    for row in rows:
        regime = str(row.get("regime") or "UNKNOWN")
        book = str(row.get("playbook") or "unknown")
        verdict = str(row.get("verdict") or "ALLOW")
        bucket = by_regime.setdefault(regime, _bucket())
        play = by_playbook.setdefault(book, _bucket())
        for dest in (bucket, play):
            dest["decisions"] += 1
            dest[verdict.lower() if verdict.lower() in {"allow", "reduce", "block"} else "allow"] += 1
        if verdict == "BLOCK":
            blocked += 1
            if row.get("mode") in {"shadow", "off"} and row.get("accepted_by_filters") is True:
                shadow_blocks_taken += 1
                bucket["shadow_taken"] += 1
        elif verdict == "REDUCE":
            reduced += 1
        else:
            allowed += 1

    close_rows = [c for c in closes if isinstance(c, dict)]
    leaks: List[Dict[str, Any]] = []
    avoided = 0.0
    realized = 0.0
    for close in close_rows:
        meta = close.get("pro_trader") if isinstance(close.get("pro_trader"), dict) else {}
        if not meta and isinstance(close.get("edge"), dict):
            meta = close["edge"].get("pro_trader") if isinstance(close["edge"].get("pro_trader"), dict) else {}
        pnl = _pnl(close)
        if pnl is None:
            continue
        realized += pnl
        regime = str(meta.get("regime") or "UNKNOWN")
        bucket = by_regime.setdefault(regime, _bucket())
        bucket["closes"] += 1
        bucket["realized_pnl_pct"] = round(float(bucket.get("realized_pnl_pct") or 0) + pnl, 4)
        verdict = str(meta.get("verdict") or "")
        if verdict in {"BLOCK", "REDUCE"} and pnl < 0 and meta.get("mode") in {"shadow", "off", None}:
            avoided += abs(pnl)
            leaks.append({
                "kind": "shadow_would_have_avoided_loss",
                "regime": regime,
                "playbook": meta.get("playbook"),
                "ticker": close.get("ticker"),
                "pnl_pct": round(pnl, 4),
                "reason_codes": (meta.get("reason_codes") or [])[:6],
            })
        elif verdict == "ALLOW" and pnl < 0:
            leaks.append({
                "kind": "allowed_loss",
                "regime": regime,
                "playbook": meta.get("playbook"),
                "ticker": close.get("ticker"),
                "pnl_pct": round(pnl, 4),
                "reason_codes": (meta.get("reason_codes") or [])[:6],
            })

    leaks.sort(key=lambda item: float(item.get("pnl_pct") or 0))
    recommendations = _recommendations(by_regime, leaks, shadow_blocks_taken)
    return {
        "decisions": len(rows),
        "allowed": allowed,
        "reduced": reduced,
        "blocked": blocked,
        "by_regime": by_regime,
        "by_playbook": by_playbook,
        "counterfactual": {
            "shadow_blocks_that_were_actually_taken": shadow_blocks_taken,
            "loss_pct_shadow_would_have_avoided": round(avoided, 4),
            "note": (
                "Shadow mode placed the real order and stored the verdict. "
                "The avoided-loss figure sums closes whose shadow verdict was "
                "BLOCK or REDUCE and that finished red."
            ),
        },
        "realized_pnl_pct_sum": round(realized, 4),
        "leaks": leaks[:8],
        "recommendations": recommendations,
        "pacing": pacing or {},
        "min_trades_per_day": int((pacing or {}).get("min_trades_per_day") or min_trades),
        "trades_completed": int((pacing or {}).get("trades_completed") or 0),
        "playbook_mix": _mix(rows),
        "shadow_counterfactual_trades": shadow_blocks_taken,
        "corrective_actions": build_corrective_actions(
            rows, close_rows, pacing=pacing, feed=feed, min_trades=min_trades,
        ),
    }


def _mix(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    mix: Dict[str, int] = {}
    for row in rows:
        if str(row.get("verdict") or "") not in {"ALLOW", "REDUCE"}:
            continue
        name = str(row.get("playbook") or "")
        if name:
            mix[name] = mix.get(name, 0) + 1
    return mix


def _bucket() -> Dict[str, Any]:
    return {
        "decisions": 0, "allow": 0, "reduce": 0, "block": 0,
        "shadow_taken": 0, "closes": 0, "realized_pnl_pct": 0.0,
    }


def _recommendations(by_regime: Dict[str, Dict[str, Any]], leaks: List[Dict[str, Any]], shadow_taken: int) -> List[str]:
    notes: List[str] = []
    if shadow_taken:
        notes.append(
            f"{shadow_taken} shadow BLOCK verdict(s) still reached the broker. "
            "Stay in shadow until that list looks right, then set PRO_TRADER_MODE=active."
        )
    for regime, bucket in sorted(by_regime.items()):
        pnl = float(bucket.get("realized_pnl_pct") or 0)
        if bucket.get("closes", 0) >= 2 and pnl < 0:
            notes.append(
                f"{regime} closed {bucket['closes']} trades summing {pnl:.2f}%. "
                "Keep the playbook's score and size cuts when you leave shadow."
            )
    kinds = {item.get("kind") for item in leaks}
    if "shadow_would_have_avoided_loss" in kinds:
        notes.append(
            "The largest leaks are trades the desk would have blocked or sized down. "
            "That is the shadow evidence active mode is for."
        )
    chop = by_regime.get("CHOP_RANGE") or {}
    if chop.get("allow", 0) + chop.get("reduce", 0) > 0 and chop.get("block", 0) == 0:
        notes.append(
            "Chop day with no blocks. Confirm midday scores were actually A+; "
            "the playbook is supposed to pass those and drop the rest."
        )
    if not notes:
        notes.append("No alignment leak stood out in this sample. Keep collecting shadow decisions before going active.")
    return notes[:6]
