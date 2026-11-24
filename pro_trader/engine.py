"""Desk: classify, adjudicate, remember. Order effect is a separate pure step
so shadow mode can be proven identical to the baseline sizer.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from pro_trader.pacing import pace_state
from pro_trader.playbook import adjudicate, rotation_ids
from pro_trader.regime import classify
from pro_trader.risk import HARD_RAILS, clamp_multiplier, risk_scale

MODES = ("off", "shadow", "active")
DEFAULT_MODE = "shadow"
SESSION_CAP = 400


def normalize_mode(value: Any, default: str = DEFAULT_MODE) -> str:
    text = str(value or "").strip().lower()
    if text in MODES:
        return text
    return default if default in MODES else DEFAULT_MODE


def passthrough(mode: str = "off") -> Dict[str, Any]:
    """Mode `off`: the desk does not classify and does not touch the order."""
    return {
        "mode": "off" if mode == "off" else normalize_mode(mode),
        "regime": None,
        "playbook": "baseline",
        "verdict": "ALLOW",
        "size_multiplier": 1.0,
        "reason_codes": ["mode_off"],
        "summary": "pro trader off — baseline order path",
        "applied": False,
        "morning_loss_shutoff": False,
        "scanner_policy": "84-A",
        "hard_rails": dict(HARD_RAILS),
        "exit_alignment": {
            "sequence": list(HARD_RAILS["trail_sequence"]),
            "flatten_et": HARD_RAILS["flatten_et"],
            "had_native_trail_sticky": True,
            "breathe": True,
            "faster_profit_lock": False,
            "breakeven_trigger_r": 0.75,
            "breakeven_trigger_pct": 0.08,
        },
    }


def order_effect(decision: Optional[Dict[str, Any]], qty: Any) -> Dict[str, Any]:
    """What happens to an order that already passed the existing gates.

    Shadow and off return the same quantity and the same allow. Active may
    refuse, or floor the quantity down. It never returns a larger quantity.
    A one-lot cannot be cut to a fraction: a multiplier at or under 0.50
    stands the one-lot down, because that contract is already the full ticket.
    """
    try:
        quantity = abs(int(float(qty)))
    except (TypeError, ValueError):
        quantity = 0
    mode = normalize_mode((decision or {}).get("mode") if isinstance(decision, dict) else None, default="shadow")
    verdict = str((decision or {}).get("verdict") or "ALLOW") if isinstance(decision, dict) else "ALLOW"
    raw_mult = (decision or {}).get("size_multiplier") if isinstance(decision, dict) else 1.0
    if mode in {"off", "shadow"}:
        return {
            "allow": True,
            "qty": quantity,
            "applied": False,
            "floor_bound": False,
            "size_multiplier": 1.0,
            "reason": None,
        }
    if verdict == "BLOCK" or quantity < 1:
        return {
            "allow": False,
            "qty": 0,
            "applied": True,
            "floor_bound": False,
            "size_multiplier": 0.0,
            "reason": (decision or {}).get("summary") if isinstance(decision, dict) else "pro trader blocked",
        }
    mult = clamp_multiplier(raw_mult)
    # risk_scale also haircuts a configured risk above 8%. The decision's
    # multiplier is already that combined scale when Desk.evaluate built it.
    # A one-lot cannot be cut to a fraction. Flooring at 1 keeps the quota
    # alive on a single-contract account; the 8% cap still binds upstream.
    if mult >= 0.999:
        return {
            "allow": True,
            "qty": quantity,
            "applied": False,
            "floor_bound": False,
            "size_multiplier": 1.0,
            "reason": None,
        }
    sized = max(1, int(math.floor(quantity * mult + 1e-9)))
    sized = min(sized, quantity)
    return {
        "allow": True,
        "qty": sized,
        "applied": sized != quantity,
        "floor_bound": sized == quantity and mult < 0.999,
        "size_multiplier": mult,
        "reason": None,
    }


def evaluate(
    observation: Optional[Dict[str, Any]],
    candidate: Optional[Dict[str, Any]],
    *,
    mode: Any = DEFAULT_MODE,
    memory: Optional[Dict[str, Any]] = None,
    configured_risk_pct: Any = 8.0,
    daily_loss_limit_pct: Any = None,
    trades_completed: Any = 0,
    min_trades: Any = 4,
    realized_pnl_today_pct: Any = None,
) -> Dict[str, Any]:
    """One decision. Does not mutate `memory`; returns the next memory on the result."""
    selected = normalize_mode(mode)
    if selected == "off":
        decision = passthrough("off")
        decision["memory"] = dict(memory or {"regime": None, "pending": None, "pending_count": 0})
        decision["pacing"] = pace_state(
            (observation or {}).get("minute_et") if isinstance(observation, dict) else None,
            trades_completed, min_trades,
        )
        return decision
    classification, memory_out = classify(observation, memory)
    pace = pace_state(
        (observation or {}).get("minute_et") if isinstance(observation, dict) else None,
        trades_completed, min_trades,
    )
    cand = dict(candidate or {})
    if realized_pnl_today_pct is not None and cand.get("realized_pnl_today_pct") is None:
        cand["realized_pnl_today_pct"] = realized_pnl_today_pct
    if daily_loss_limit_pct is not None and cand.get("daily_loss_limit_pct") is None:
        cand["daily_loss_limit_pct"] = daily_loss_limit_pct
    verdict = adjudicate(classification, cand, pace)
    # Combine the playbook scale with the 8% ceiling haircut. Block stays 0.
    if verdict["verdict"] == "BLOCK":
        scaled = risk_scale(configured_risk_pct, 1.0, daily_loss_limit_pct)
        scaled["size_multiplier"] = 0.0
        scaled["effective_risk_pct"] = 0.0
    else:
        scaled = risk_scale(configured_risk_pct, verdict["size_multiplier"], daily_loss_limit_pct)
        verdict["size_multiplier"] = scaled["size_multiplier"]
        if scaled["size_multiplier"] < 0.999 and verdict["verdict"] == "ALLOW":
            verdict["verdict"] = "REDUCE"
    cand = candidate or {}
    decision = {
        "mode": selected,
        "applied": False,
        **classification,
        **verdict,
        "risk": scaled,
        "hard_rails": dict(HARD_RAILS),
        "ticker": cand.get("ticker"),
        "setup": cand.get("setup"),
        "score": cand.get("score"),
        "memory": memory_out,
    }
    # Public copy must not share the desk's memory dict.
    decision["memory"] = {
        "regime": memory_out.get("regime"),
        "pending": memory_out.get("pending"),
        "pending_count": memory_out.get("pending_count") or 0,
    }
    return decision


class Desk:
    """Session state: debounced regime and the decisions taken today."""

    def __init__(self) -> None:
        self.memory: Dict[str, Any] = {"regime": None, "pending": None, "pending_count": 0}
        self.decisions: List[Dict[str, Any]] = []
        self.last: Optional[Dict[str, Any]] = None
        self.trades_completed = 0
        self.min_trades = 4

    def note_fill(self) -> None:
        """Count a fill toward today's pace. The bot may instead pass trades_today."""
        self.trades_completed += 1

    def evaluate(
        self,
        observation: Optional[Dict[str, Any]],
        candidate: Optional[Dict[str, Any]],
        *,
        mode: Any = DEFAULT_MODE,
        configured_risk_pct: Any = 8.0,
        daily_loss_limit_pct: Any = None,
        trades_completed: Any = None,
        min_trades: Any = None,
        realized_pnl_today_pct: Any = None,
    ) -> Dict[str, Any]:
        if min_trades is not None:
            self.min_trades = int(min_trades)
        done = self.trades_completed if trades_completed is None else int(trades_completed)
        self.trades_completed = max(0, done)
        decision = evaluate(
            observation,
            candidate,
            mode=mode,
            memory=self.memory,
            configured_risk_pct=configured_risk_pct,
            daily_loss_limit_pct=daily_loss_limit_pct,
            trades_completed=self.trades_completed,
            min_trades=self.min_trades,
            realized_pnl_today_pct=realized_pnl_today_pct,
        )
        self.memory = dict(decision.get("memory") or self.memory)
        # Callers persist `decision` without the memory key.
        public = dict(decision)
        public.pop("memory", None)
        self.last = public
        return public

    def remember(self, decision: Dict[str, Any], *, accepted: Optional[bool] = None) -> None:
        row = dict(decision)
        row.pop("memory", None)
        if accepted is not None:
            row["accepted_by_filters"] = bool(accepted)
        self.decisions.append(row)
        if len(self.decisions) > SESSION_CAP:
            self.decisions = self.decisions[-SESSION_CAP:]

    def session_counts(self) -> Dict[str, int]:
        counts = {"decisions": 0, "allow": 0, "reduce": 0, "block": 0}
        for row in self.decisions:
            counts["decisions"] += 1
            verdict = str(row.get("verdict") or "").lower()
            if verdict in {"allow", "reduce", "block"}:
                counts[verdict] += 1
        return counts

    def status(self, *, mode: Any = None, feed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        last = self.last or {}
        selected = normalize_mode(mode if mode is not None else last.get("mode") or DEFAULT_MODE)
        return {
            "mode": selected,
            "default_mode": DEFAULT_MODE,
            "regime": self.memory.get("regime"),
            "raw_regime": last.get("raw_regime"),
            "confidence": last.get("confidence"),
            "phase": last.get("phase"),
            "bias": last.get("bias"),
            "playbook": last.get("playbook"),
            "size_multiplier": last.get("size_multiplier"),
            "verdict": last.get("verdict"),
            "reason_codes": last.get("reason_codes"),
            "summary": last.get("summary"),
            "feed_health": feed if feed is not None else last.get("feed"),
            "exit_alignment": last.get("exit_alignment"),
            "hard_rails": dict(HARD_RAILS),
            "index": last.get("index"),
            "efficiency": last.get("efficiency"),
            "vol_ratio": last.get("vol_ratio"),
            "session": self.session_counts(),
            "scanner_policy": "84-A",
            "morning_loss_shutoff": False,
            "scanner_note": (
                "Scanner stays on. No permanent morning-loss shutoff. "
                "The rotation keeps several playbooks per regime, and pacing admits "
                "verified B+ setups from that rotation when the session is behind "
                "MIN_TRADES_PER_DAY. Hard rails are not relaxed."
            ),
            "pending_regime": self.memory.get("pending"),
            "pending_count": self.memory.get("pending_count") or 0,
            "pacing": last.get("pacing") or pace_state(None, self.trades_completed, self.min_trades),
            "min_trades_per_day": (last.get("pacing") or {}).get("min_trades_per_day", self.min_trades),
            "trades_completed": self.trades_completed,
            "rotation": last.get("rotation") or rotation_ids(str(self.memory.get("regime") or "")),
            "playbook_mix": _playbook_mix(self.decisions),
        }


def _playbook_mix(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    mix: Dict[str, int] = {}
    for row in decisions:
        if str(row.get("verdict") or "") not in {"ALLOW", "REDUCE"}:
            continue
        name = str(row.get("playbook") or "")
        if not name:
            continue
        mix[name] = mix.get(name, 0) + 1
    return mix
