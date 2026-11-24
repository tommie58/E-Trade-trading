"""Adaptive Pro Day Trader realignment engine.

Pure decisions. The bot calls `Desk.evaluate` and `order_effect`. Shadow mode
records the verdict and leaves the order alone. Active mode may only block or
scale size down, inside the locked rails in `risk.HARD_RAILS`.
"""
from pro_trader.attribution import build_report, journal_metadata
from pro_trader.corrective import build_corrective_actions
from pro_trader.reconcile_reports import (
    dedupe_review_rows,
    reconcile_report_sources,
    report_closes,
    review_identity,
)
from pro_trader.engine import Desk, evaluate, interpret_mode, normalize_mode, order_effect, passthrough
from pro_trader.execution import guard_live_quote
from pro_trader.pacing import MIN_TRADES_DEFAULT, clamp_min_trades, pace_state
from pro_trader.regime import classify
from pro_trader.risk import HARD_RAILS, clamp_multiplier, position_exit_fields, risk_scale

__all__ = [
    "Desk",
    "HARD_RAILS",
    "MIN_TRADES_DEFAULT",
    "build_corrective_actions",
    "build_report",
    "clamp_min_trades",
    "clamp_multiplier",
    "classify",
    "dedupe_review_rows",
    "evaluate",
    "interpret_mode",
    "pace_state",
    "guard_live_quote",
    "journal_metadata",
    "normalize_mode",
    "order_effect",
    "passthrough",
    "position_exit_fields",
    "reconcile_report_sources",
    "report_closes",
    "review_identity",
    "risk_scale",
]
