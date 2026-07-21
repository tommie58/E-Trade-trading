"""
E*TRADE Trading Bot — corrected drop-in
=======================================

This is the production webhook bot the Rork app dispatches to. It is a corrected
version of the earlier `main_bot.py` that was failing live OPTION orders with
E*TRADE "Missing required parameters".

ROOT CAUSE OF THE OLD FAILURE
-----------------------------
`pyetrade.ETradeOrder.place_option_order(**kwargs)` does NOT accept a pre-built
nested `order={...}` payload. Internally it does:

    place_option_order(**kwargs)      # sets securityType="OPTN"
      -> place_equity_order(**kwargs) # calls check_order(**kwargs)
        -> build_order_payload(...)   # builds Order/Instrument/Product ITSELF
                                      # from FLAT kwargs and parses expiryDate
                                      # (a "YYYY-MM-DD" string) via dateutil.

The old code passed `order=order_payload` (a nested dict). pyetrade ignored that
unknown `order=` kwarg, `check_order` found none of the required flat params
(symbol, orderAction, priceType, quantity, callPut, strikePrice, expiryDate) and
raised "Missing required parameters".

THE FIX
-------
Pass FLAT keyword arguments and let pyetrade build the payload + auto-preview.
`expiryDate` is passed as a plain "YYYY-MM-DD" string (dateutil handles it).

V2.9.0 (merged trailing-stop drop-in)
-------------------------------------
Merges the user-deployed "2.8.0-trailing-stop" variant back into this corrected
bot. Key decisions:

- ENTRY orders (BUY_OPEN) are ALWAYS plain LIMIT (from `option_limit_price`)
  or MARKET. Never STOP/TRAILING_STOP on an entry — a stop price type turns
  the entry into a trigger order that may never fill, and E*TRADE rejects
  trailing stops on option opens anyway.
- EXIT orders (SELL_CLOSE) default to MARKET so protective exits always fill.
  The app's Auto-Exit engine tracks the ratcheting trailing stop client-side
  and dispatches the SELL_CLOSE the moment it's breached. An optional resting
  broker-side STOP is supported when the payload sends `broker_stop: true`
  plus a `stop_price` (or `trail_stop`).
- Quantity comes from `option_contracts` (the field the app actually sends),
  with `contracts` and `quantity` as fallbacks.
- Tokens persist to the DB (`preload_tokens` on startup) so restarts don't
  unlink the account, and a keepalive worker proactively renews the E*TRADE
  access token every ~50 minutes so it never goes idle-inactive (E*TRADE
  deactivates tokens after ~2h idle; they hard-expire at midnight ET).
- The webhook secret is accepted from the JSON body `secret` field OR the
  `X-Rork-Secret` header (the app sends both).
- Response statuses stay in the vocabulary the app's parser understands:
  `queued`, `processed_directly` (with nested `result`), `skipped`, `error`.

V5.0.0 (redis-async foundation)
-------------------------------
• ALL trading state (positions, stop guards, daily counters, kill switch,
  idempotency keys, cooldowns) now lives in Redis via state_store.py — the
  old STATE_FILE + in-memory dicts are migrated once at startup and retired.
  Multi-worker deployments are safe; Railway/Render cold starts lose nothing.
• Critical sections (order placement, close, reconciliation, stop guards) run
  under Redis distributed locks (SET NX PX + token + Lua release).
• Order placement moved to raw async JSON payloads against E*TRADE's Order
  API (httpx + OAuth1, etrade_async.py) with an atomic Entry+Stop OTOCO
  best-effort and automatic fallback to pyetrade. pyetrade remains for simple
  reads (quotes, accounts, order listing) wrapped in asyncio.to_thread.
• A background reconciliation engine (reconciliation.py) polls broker orders
  + positions every RECONCILE_INTERVAL_SECONDS under the reconcile lock and
  auto-heals Redis state (fill sync, ghost positions, orphaned stops,
  unprotected positions → guard re-arm). Reconciled Redis state is the single
  source of truth for the stop guard and close logic.
• Option positions now get the same broker-resting protection as equities:
  an async option guard polls the entry fill and rests a SELL_CLOSE STOP on
  the same OCC contract, state persisted in Redis so it survives restarts.

V5.1.0 (hardening)
------------------
• STARTUP RECONCILIATION: a blocking broker→Redis reconciliation pass runs at
  boot — BEFORE the placement worker accepts any job — so cold starts begin
  from broker truth, not stale state.
• REAL-TIME ALERTING (alerts.py): kill switch, failed guards, unprotected
  positions, circuit-breaker trips and API problems are logged, kept in Redis
  (`alerts:recent`, GET /alerts) and pushed to ALERT_WEBHOOK_URL
  (Slack/Discord/generic JSON), with per-key dedupe to stop alert storms.
• IMMUTABLE TRADE LEDGER (trade_ledger.py): every order, position open/close,
  protective stop, guard result and safety event is appended to a
  hash-chained JSONL file. GET /ledger returns the tail and verifies the
  chain end-to-end.
• DISTRIBUTED CIRCUIT BREAKER: the consecutive-failure breaker moved to Redis
  (`breaker:*`), so ALL workers halt order placement together. It auto-resets
  via TTL after the cooldown and immediately on account relink; trips are
  alerted and ledgered.
• RECONCILIATION CADENCE: default every 5 minutes in-session (was 30s) and
  15 minutes off-hours — reconciliation is a periodic safety net; fill-time
  protection stays event-driven via the stop guards.

Run:
    pip install -r requirements.txt
    cp .env.example .env   # fill in keys + WEBHOOK_SECRET + REDIS_URL
    uvicorn main_bot:app --host 0.0.0.0 --port 8000
"""
from fastapi import FastAPI, HTTPException, Body, Query, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator, ValidationError
from typing import Optional, Dict, Any, List, Tuple
import pyetrade
import os
import json
import math
import re
import logging
import random
import uuid
import asyncio
import hashlib
import hmac
import time
from pathlib import Path
from datetime import datetime, date, time as dtime, timezone
from zoneinfo import ZoneInfo
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from requests_oauthlib import OAuth1Session
from urllib.parse import quote
from dotenv import load_dotenv

try:  # package-style import (python -m bot.main_bot) or flat (uvicorn main_bot:app)
    from .state_store import StateStore, LockNotAcquired
    from .etrade_async import ETradeAsyncClient, ETradeAPIError, OTOCOUnsupported
    from . import reconciliation
    from . import alerts
    from . import trade_ledger
except ImportError:
    from state_store import StateStore, LockNotAcquired
    from etrade_async import ETradeAsyncClient, ETradeAPIError, OTOCOUnsupported
    import reconciliation
    import alerts
    import trade_ledger

load_dotenv()

# ==================== CONFIG ====================
# Defaults match the deployed production bot: this bot exists to trade live.
# Set ETRADE_ENV=sandbox / LIVE_TRADING=false explicitly for testing.
ENV = os.getenv("ETRADE_ENV", "production").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "true").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")

is_sandbox = ENV == "sandbox"

# Bump on every deploy-relevant change. Reported by /health and /etrade/auth/start
# so the app/user can verify the running container matches the repo code.
BOT_VERSION = "5.5.0-balance-db-fix"

# ---- Safety / parity config (mirrors etrade_bot_handler.py) ----
# Gate parity with the Rork app (app defaults: minScore 85 / trending 80).
# A stricter bot default silently rejects every score the app clears — the
# thresholds MUST match unless deliberately overridden via env.
MIN_SCORE = int(os.getenv("MIN_SCORE", "85"))
MIN_SCORE_TRENDING = int(os.getenv("MIN_SCORE_TRENDING", "80"))
MIN_RVOL = float(os.getenv("MIN_RVOL", "1.5"))
MIN_MTF = int(os.getenv("MIN_MTF", "3"))
ALLOWED_SETUPS = {
    s.strip().lower()
    for s in os.getenv(
        "ALLOWED_SETUPS",
        "ema cross + adx,ema cross,bull flag + adx,bull flag,bear flag + adx,"
        "bear flag,volume breakout,breakout,vwap reclaim,momentum",
    ).split(",")
    if s.strip()
}
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "2.0"))
DAILY_TRADE_LIMIT = int(os.getenv("DAILY_TRADE_LIMIT", "20"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))
PORTFOLIO_HEAT_PCT = float(os.getenv("PORTFOLIO_HEAT_PCT", "6.0"))
TICKER_COOLDOWN_MINUTES = int(os.getenv("TICKER_COOLDOWN_MINUTES", "15"))
# Stop-guard: how long an entry may sit unfilled before it is cancelled, and
# how often the guard polls the broker for fill state.
ENTRY_FILL_TIMEOUT_MIN = int(os.getenv("ENTRY_FILL_TIMEOUT_MIN", "20"))
STOP_GUARD_POLL_SECONDS = int(os.getenv("STOP_GUARD_POLL_SECONDS", "10"))
# Legacy JSON state file — migrated into Redis once at startup, then retired.
STATE_FILE = Path(os.getenv("ETRADE_STATE_FILE", ".etrade_state.json"))

# ---- V5 architecture config ----
# Raw async Order API (httpx + OAuth1) is the primary placement path; pyetrade
# remains the fallback and handles simple reads (quotes, auth, renew).
USE_RAW_ORDER_API = os.getenv("USE_RAW_ORDER_API", "true").lower() == "true"
# Attempt an atomic Entry+Stop (OTOCO-style) submission. E*TRADE's public API
# doesn't officially support OCO/OTOCO, so a rejection falls back to entry +
# stop guard and is remembered for OTOCO_UNSUPPORTED_TTL to stop retrying.
ENABLE_RAW_OTOCO = os.getenv("ENABLE_RAW_OTOCO", "true").lower() == "true"
OTOCO_UNSUPPORTED_TTL = int(os.getenv("OTOCO_UNSUPPORTED_TTL", str(24 * 3600)))
# Background reconciliation engine cadence (seconds). Periodic safety net —
# NOT the fast path (stop guards are event-driven), so 5–10 min is the right
# cadence: broker-API friendly, still catches drift quickly. A blocking pass
# also runs at startup before any order is accepted.
RECONCILE_INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "300"))
RECONCILE_OFFHOURS_SECONDS = int(os.getenv("RECONCILE_OFFHOURS_SECONDS", "900"))
RECONCILE_AUTO_HEAL = os.getenv("RECONCILE_AUTO_HEAL", "true").lower() == "true"
STARTUP_RECONCILE_TIMEOUT_SECONDS = int(os.getenv("STARTUP_RECONCILE_TIMEOUT_SECONDS", "90"))
# Real-time alerting: Slack/Discord/generic JSON webhook. Alerts always land
# in the log and GET /alerts even without a webhook.
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")
ALERT_DEDUPE_SECONDS = int(os.getenv("ALERT_DEDUPE_SECONDS", "300"))
# Immutable trade ledger — append-only, hash-chained JSONL (GET /ledger).
TRADE_LEDGER_FILE = Path(os.getenv("TRADE_LEDGER_FILE", "trade_ledger.jsonl"))
# Option stop protection: when the app doesn't send an explicit option stop
# premium, protect at this percent below the entry premium (0 disables).
OPTION_STOP_LOSS_PCT = float(os.getenv("OPTION_STOP_LOSS_PCT", "30"))

# ---- V5.2 hardening ----
# Exponential backoff for ALL E*TRADE API calls. The same env vars configure
# the raw async client (etrade_async.py). Non-idempotent calls (order
# placement) are NEVER blind-retried — a failed place may still have landed.
# 429 throttles follow the exact schedule: 2s → 4s → 8s (base × 2^attempt,
# capped at max, no jitter). Other transient errors keep jitter.
ETRADE_RETRY_ATTEMPTS = max(1, int(os.getenv("ETRADE_RETRY_ATTEMPTS", "4")))
ETRADE_RETRY_BASE_SECONDS = float(os.getenv("ETRADE_RETRY_BASE_SECONDS", "2"))
ETRADE_RETRY_MAX_SECONDS = float(os.getenv("ETRADE_RETRY_MAX_SECONDS", "8"))
# STOP PLACEMENT TIMEOUT: a guarded entry must have its protective stop
# RESTING at the broker within this many seconds of entry placement. At the
# deadline with no stop: an unfilled entry is auto-cancelled; a filled entry
# is emergency-flattened at market (a fill cannot be cancelled). 0 disables.
STOP_PLACEMENT_TIMEOUT_SECONDS = int(os.getenv("STOP_PLACEMENT_TIMEOUT_SECONDS", "60"))


def _utcnow() -> datetime:
    """Timezone-aware UTC now (datetime.utcnow() is deprecated in 3.12+)."""
    return datetime.now(timezone.utc)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Trading Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== GLOBALS ====================
# Distributed state store (Redis-first; loud in-memory fallback for local dev).
# Replaced at startup by StateStore.create(REDIS_URL).
state: StateStore = StateStore(None)
engine = None
async_session = None
# Circuit breaker — DISTRIBUTED (Redis `breaker:*` keys) so every worker halts
# order placement together. Opens after MAX_CONSECUTIVE_FAILURES broker API
# failures within a rolling BREAKER_FAILURE_WINDOW_SECONDS; auto-resets via
# TTL after the cooldown, and immediately on account relink.
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "5"))
CIRCUIT_BREAKER_COOLDOWN_SECONDS = int(os.getenv("CIRCUIT_BREAKER_COOLDOWN_SECONDS", str(10 * 60)))
BREAKER_FAILURE_WINDOW_SECONDS = int(os.getenv("BREAKER_FAILURE_WINDOW_SECONDS", "600"))
BREAKER_OPEN_KEY = "breaker:open"
BREAKER_FAILURES_KEY = "breaker:failures"
QUEUE_KEY = "etrade:placement_queue"
_worker_task = None
_worker_stop = False

# In-memory token cache
_current_tokens: Optional[Dict[str, str]] = None

# Cached E*TRADE accountIdKey. Order APIs require the opaque accountIdKey from
# /accounts/list — NOT the visible account number. Users typically set
# ETRADE_ACCOUNT_ID to the account number, which E*TRADE rejects with
# "Code: 102, Please enter valid Account Key". We resolve the real key once
# per session and cache it here (cleared on relink/disconnect).
_resolved_account_id_key: Optional[str] = None

# Pending OAuth request tokens (token -> secret). E*TRADE request tokens are
# single-use and expire ~5 minutes after issue. Memory is the PRIMARY store —
# the DB row is only a backup — so a DB failure can never cause /auth/complete
# to exchange a stale token (the cause of "oauth_problem=token_rejected").
_pending_request_tokens: Dict[str, str] = {}
_latest_request_token: Optional[str] = None
_MAX_PENDING_REQUEST_TOKENS = 5

# ==================== SAFETY STATE (Redis-backed) ====================
# Positions, stop guards, daily counters, kill switch, idempotency keys and
# ticker cooldowns all live in the distributed StateStore (state_store.py):
#   open_positions:{ticker} · stop_guard:{ticker} · daily:{date} · killed
# Daily counters are keyed by date, so the old midnight reset is implicit.
# The legacy STATE_FILE is migrated into Redis once at startup, then retired.


# ==================== MARKET CALENDAR ====================
_ET_ZONE = ZoneInfo("America/New_York")

# NYSE/Nasdaq full-closure holidays (observed dates).
_MARKET_HOLIDAYS = {
    # 2025
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26",
    "2025-06-19", "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    # 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    # 2027
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
    "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}

# 1:00pm ET early closes.
_MARKET_HALF_DAYS = {
    "2025-07-03", "2025-11-28", "2025-12-24",
    "2026-11-27", "2026-12-24",
    "2027-11-26",
}


def _is_market_open() -> bool:
    """Exchange-aware regular-session check: 9:30–16:00 US/Eastern (13:00 close
    on half days), weekdays only, NYSE holidays excluded. DST-safe via zoneinfo."""
    now_et = _utcnow().astimezone(_ET_ZONE)
    if now_et.weekday() >= 5:
        return False
    day = now_et.date().isoformat()
    if day in _MARKET_HOLIDAYS:
        return False
    close = dtime(13, 0) if day in _MARKET_HALF_DAYS else dtime(16, 0)
    return dtime(9, 30) <= now_et.time() <= close


# ==================== WEBHOOK AUTH / IDEMPOTENCY ====================
def _verify_webhook_auth(
    raw: bytes,
    body_secret: Optional[str],
    secret_header: Optional[str],
    sig_header: Optional[str],
) -> None:
    """Accept the shared secret (body `secret` field or X-Rork-Secret header)
    or an HMAC-SHA256 signature of the raw body (X-Rork-Signature)."""
    if not WEBHOOK_SECRET:
        return
    if sig_header:
        expected = hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
        provided = sig_header.split("=", 1)[-1].strip()
        if hmac.compare_digest(expected, provided):
            return
        raise HTTPException(401, "invalid signature")
    if body_secret == WEBHOOK_SECRET or secret_header == WEBHOOK_SECRET:
        return
    raise HTTPException(403, "Unauthorized")


def _signal_key(p: dict) -> str:
    # intent is part of the key so a close is never deduped against the entry
    # that opened the position.
    base = (
        f"{p.get('ticker')}|{p.get('action')}|{p.get('entry')}|{p.get('stop')}|"
        f"{p.get('target')}|{p.get('timestamp')}|{str(p.get('intent') or 'open').lower()}"
    )
    return "sig:" + hashlib.sha1(base.encode()).hexdigest()


def _is_close_payload(p: dict) -> bool:
    """An exit/close must bypass entry gating — closing always reduces risk."""
    if str(p.get("intent") or "").lower() == "close":
        return True
    if str(p.get("order_action") or "").upper() == "SELL_CLOSE":
        return True
    return str(p.get("action") or "").upper() in {"EXIT", "CLOSE"}


# ==================== ENTRY FILTERS (live entries only) ====================
async def _passes_entry_filters(p: dict) -> Tuple[bool, List[str]]:
    """Server-side re-filter mirroring the Rork app's gating, evaluated against
    the reconciled Redis state (single source of truth). Quality checks apply
    only when the payload carries the field (score/rvol/mtf/setup); risk
    limits (kill switch, daily loss, trade count, positions, heat, duplicate
    ticker) always apply."""
    blocked: List[str] = []
    if await state.is_killed():
        blocked.append("kill switch active")

    regime = str(p.get("regime") or "").lower()
    score = p.get("score")
    if isinstance(score, (int, float)):
        required = MIN_SCORE_TRENDING if "trending" in regime else MIN_SCORE
        if score < required:
            blocked.append(f"score {score} < {required}")
    rvol = p.get("rvol")
    if isinstance(rvol, (int, float)) and rvol < MIN_RVOL:
        blocked.append(f"rvol {rvol} < {MIN_RVOL}")
    mtf = p.get("mtf_alignment")
    if isinstance(mtf, str) and mtf.strip():
        try:
            if int(mtf.split("/")[0]) < MIN_MTF:
                blocked.append(f"mtf {mtf} < {MIN_MTF}/5")
        except (ValueError, IndexError):
            blocked.append("mtf_alignment unparseable")
    setup = p.get("setup")
    if isinstance(setup, str) and setup.strip() and ALLOWED_SETUPS and setup.lower().strip() not in ALLOWED_SETUPS:
        blocked.append(f"setup '{setup}' not allowlisted")

    daily = await state.get_daily()
    open_positions = await state.all_positions()
    if daily["realized_pnl_today_pct"] <= -abs(DAILY_LOSS_LIMIT_PCT):
        blocked.append(f"daily loss limit ({daily['realized_pnl_today_pct']:.2f}%)")
    if daily["trades_today"] >= DAILY_TRADE_LIMIT:
        blocked.append(f"daily trade limit ({DAILY_TRADE_LIMIT}) reached")
    if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
        blocked.append(f"max positions ({MAX_CONCURRENT_POSITIONS}) open")
    ticker = str(p.get("ticker") or "").upper()
    if ticker and ticker in open_positions:
        blocked.append(f"already in {ticker}")

    try:
        account_ref = float(os.getenv("ACCOUNT_SIZE", "50000"))
        open_risk = sum(
            abs(float(pos.get("entry") or 0) - float(pos.get("stop") or 0)) * float(pos.get("qty") or 0)
            for pos in open_positions.values()
            if pos.get("entry") and pos.get("stop")
        )
        new_risk = account_ref * (RISK_PER_TRADE_PCT / 100.0)
        heat = (open_risk + new_risk) / max(account_ref, 1) * 100.0
        if heat > PORTFOLIO_HEAT_PCT:
            blocked.append(f"portfolio heat {heat:.2f}% > {PORTFOLIO_HEAT_PCT}%")
    except (TypeError, ValueError):
        pass

    return len(blocked) == 0, blocked


def _occ_symbol(ticker: str, expiry: str, right: str, strike: float) -> str:
    """Build an OCC 21-char option symbol: ROOT(6) + YYMMDD + C/P + STRIKE*1000(8)."""
    try:
        d = datetime.strptime(str(expiry)[:10], "%Y-%m-%d")
        date_part = d.strftime("%y%m%d")
    except ValueError:
        date_part = "000000"
    cp = "C" if str(right).upper().startswith("C") else "P"
    return f"{ticker.upper().ljust(6)}{date_part}{cp}{int(round(float(strike) * 1000)):08d}"

Base = declarative_base()


class ETradeSessionState(Base):
    __tablename__ = "etrade_session_state"
    id = Column(String(50), primary_key=True, default="active_state")
    oauth_token = Column(Text, nullable=False)
    oauth_token_secret = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ==================== MODELS ====================
class WebhookPayload(BaseModel):
    # Secret can also arrive via the X-Rork-Secret header — see /webhook.
    secret: Optional[str] = None
    ticker: str
    action: str
    mode: Optional[str] = "paper"
    instrument: Optional[str] = "stock"
    strike: Optional[float] = None
    strike_hint: Optional[float] = None
    expiry: Optional[str] = None
    expiration_hint: Optional[str] = None
    # Discrete expiration components the app also sends (used as a fallback to
    # rebuild a clean "YYYY-MM-DD" string for pyetrade).
    expiration_year: Optional[int] = None
    expiration_month: Optional[int] = None
    expiration_day: Optional[int] = None
    option_right: Optional[str] = None
    call_put: Optional[str] = None
    contracts: Optional[int] = None
    option_contracts: Optional[int] = None
    quantity: Optional[int] = None
    limit_price: Optional[float] = None
    option_limit_price: Optional[float] = None
    position_size_shares: Optional[int] = None
    # Stop / trailing-stop telemetry from the app. Informational on entries —
    # the Auto-Exit engine handles exits app-side by dispatching SELL_CLOSE.
    # Used broker-side ONLY on a SELL_CLOSE with broker_stop=true.
    stop: Optional[float] = None
    trail_stop: Optional[float] = None
    trail_amount: Optional[float] = None
    stop_price: Optional[float] = None
    trailing_stop_amount: Optional[float] = None
    trailing_stop_percent: Optional[float] = None
    broker_stop: Optional[bool] = None
    exit_limit_price: Optional[float] = None
    # Option stop protection: explicit protective stop PREMIUM for the option
    # guard (falls back to OPTION_STOP_LOSS_PCT below the entry premium).
    option_stop_price: Optional[float] = None

    class Config:
        # Keep unknown fields instead of silently dropping them. A trimmed
        # payload model was exactly what caused "strike=None, expiry=None" on
        # a previously deployed variant — the app DID send strike_hint /
        # expiration_hint, but pydantic stripped them before payload.dict().
        extra = "allow"

    @validator("action")
    def validate_action(cls, v):
        if str(v).upper() not in {"BUY", "SELL", "EXIT", "CLOSE"}:
            raise ValueError("Invalid action")
        return str(v).upper()


# ==================== TOKEN PERSISTENCE ====================
def save_tokens(token: str, token_secret: str):
    global _current_tokens, _resolved_account_id_key
    logger.info("=== NEW TOKENS RECEIVED ===")
    _current_tokens = {"oauth_token": token, "oauth_token_secret": token_secret}
    _resolved_account_id_key = None  # re-resolve accountIdKey for the new session
    # A fresh link is an explicit user action — clear any tripped breaker so
    # the relinked session starts clean instead of rejecting with 503.
    try:
        asyncio.create_task(_reset_circuit_breaker("account relinked"))
    except RuntimeError:
        pass  # no running loop (e.g. import-time) — TTL reset still applies
    if async_session:
        try:
            asyncio.create_task(_save_tokens_to_db(token, token_secret))
        except Exception as e:
            logger.warning(f"Failed to persist tokens to DB: {e}")


async def _save_tokens_to_db(token: str, token_secret: str):
    if not async_session:
        return
    try:
        async with async_session() as session:
            async with session.begin():
                state = ETradeSessionState(id="active_tokens", oauth_token=token, oauth_token_secret=token_secret)
                await session.merge(state)
        logger.info("✅ Tokens saved to database")
    except Exception as e:
        logger.error(f"Error saving tokens to DB: {e}")


def load_tokens() -> Optional[Dict[str, str]]:
    global _current_tokens
    if _current_tokens:
        return _current_tokens
    token = os.getenv("ETRADE_ACCESS_TOKEN")
    secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
    if token and secret:
        _current_tokens = {"oauth_token": token, "oauth_token_secret": secret}
        return _current_tokens
    return None


async def _load_tokens_from_db():
    if not async_session:
        return None
    try:
        async with async_session() as session:
            state = await session.get(ETradeSessionState, "active_tokens")
            if state:
                return {"oauth_token": state.oauth_token, "oauth_token_secret": state.oauth_token_secret}
    except Exception as e:
        logger.error(f"Error loading tokens from DB: {e}")
    return None


async def preload_tokens():
    global _current_tokens
    if async_session:
        try:
            tokens = await _load_tokens_from_db()
            if tokens:
                _current_tokens = tokens
                logger.info("✅ Tokens pre-loaded from database into memory")
        except Exception as e:
            logger.warning(f"Could not preload tokens from DB: {e}")


# ==================== OAUTH ====================
REQUEST_TOKEN_URL = "https://api.etrade.com/oauth/request_token"
AUTHORIZE_URL = "https://us.etrade.com/e/t/etws/authorize"
ACCESS_TOKEN_URL = "https://api.etrade.com/oauth/access_token"


@app.api_route("/etrade/auth/start", methods=["GET", "POST"])
@app.api_route("/link", methods=["GET", "POST"])
async def etrade_auth_start():
    global _latest_request_token
    try:
        etrade_session = OAuth1Session(client_key=CONSUMER_KEY, client_secret=CONSUMER_SECRET, callback_uri="oob")
        # Request tokens are disposable — fetching is safe to retry with
        # backoff (unlike the access-token exchange, whose verifier is
        # single-use and must NEVER be retried).
        fetch_response = await asyncio.to_thread(
            _sync_etrade_call, etrade_session.fetch_request_token, REQUEST_TOKEN_URL,
            source="request_token",
        )
        token_val = fetch_response.get("oauth_token")
        secret_val = fetch_response.get("oauth_token_secret")
        if not token_val or not secret_val:
            raise Exception("Failed to get request token from E*TRADE")
        # E*TRADE request tokens are base64-style and often contain '+', '/'
        # and '='. Unencoded, the browser decodes '+' as a space and E*TRADE
        # rejects the token with "Due to a logon delay or other issue...".
        # Percent-encode both query params so the token survives intact.
        # CRITICAL: percent-encode ONLY the parameter VALUES, never the
        # separators. Encoding the whole query string turns `token=` into
        # `token%3D`, so E*TRADE receives no token param at all and shows
        # "Due to a logon delay or other issue..." on the authorize page.
        encoded_key = quote(str(CONSUMER_KEY or ""), safe="")
        encoded_token = quote(str(token_val), safe="")
        auth_url = f"{AUTHORIZE_URL}?key={encoded_key}&token={encoded_token}"

        # Hard guard: never return a malformed URL. The literal separators
        # `?key=` and `&token=` must survive, and the token value must decode
        # back to exactly the raw token E*TRADE issued.
        from urllib.parse import parse_qs, urlsplit
        parsed_query = parse_qs(urlsplit(auth_url).query)
        if parsed_query.get("token", [None])[0] != str(token_val) or parsed_query.get("key", [None])[0] != str(CONSUMER_KEY or ""):
            logger.error(f"[{BOT_VERSION}] MALFORMED AUTH URL blocked — separators were encoded")
            raise Exception("Internal error building authorize URL — please retry")

        # PRIMARY store: in-memory. Keep the last few so a duplicate tap on
        # "Link" doesn't orphan the token the user actually authorized.
        _pending_request_tokens[str(token_val)] = str(secret_val)
        while len(_pending_request_tokens) > _MAX_PENDING_REQUEST_TOKENS:
            _pending_request_tokens.pop(next(iter(_pending_request_tokens)))
        _latest_request_token = str(token_val)

        # BACKUP store: DB (best-effort — never fail the start on a DB error).
        if async_session:
            try:
                async with async_session() as session:
                    async with session.begin():
                        state = ETradeSessionState(id="active_state", oauth_token=str(token_val), oauth_token_secret=str(secret_val))
                        await session.merge(state)
            except Exception as db_err:
                logger.warning(f"Could not persist request token to DB (memory store still active): {db_err}")

        # Diagnostic (token masked): confirms which code version built the URL
        # and whether the token needed percent-encoding ('+', '/', '=' chars).
        raw = str(token_val)
        specials = "".join(sorted({c for c in raw if c in "+/="})) or "none"
        logger.info(
            f"[{BOT_VERSION}] auth URL generated | token={raw[:4]}...{raw[-4:]} "
            f"len={len(raw)} special_chars={specials} encoded_tail=...{auth_url[-24:]}"
        )
        # Return the request token so the app can cache it and echo it back on
        # /auth/complete — lets us verify we exchange the SAME token the user
        # authorized, even across duplicate starts.
        return {"status": "success", "auth_url": auth_url, "authorize_url": auth_url, "request_token": str(token_val), "bot_version": BOT_VERSION}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, detail=str(e))


@app.post("/etrade/auth/complete")
@app.post("/complete-link")
async def etrade_auth_complete(data: dict = Body(...)):
    global _latest_request_token
    try:
        verifier = str(data.get("oauth_verifier") or data.get("verifier") or data.get("code") or "").strip()
        if not verifier:
            raise HTTPException(400, "Missing verification code")

        # The app echoes back the request token it received from /auth/start.
        provided = data.get("request_token") or data.get("oauth_token")
        provided = str(provided).strip() if provided else None

        token_val: Optional[str] = None
        secret_val: Optional[str] = None

        # 1) Exact match on the token the app says the user authorized.
        if provided and provided in _pending_request_tokens:
            token_val, secret_val = provided, _pending_request_tokens[provided]
        # 2) Latest request token issued by this process.
        elif _latest_request_token and _latest_request_token in _pending_request_tokens:
            token_val = _latest_request_token
            secret_val = _pending_request_tokens[token_val]
        # 3) DB backup (e.g. the bot restarted between start and complete).
        elif async_session:
            try:
                async with async_session() as session:
                    cached = await session.get(ETradeSessionState, "active_state")
                    if cached:
                        token_val = cached.oauth_token
                        secret_val = cached.oauth_token_secret
            except Exception as db_err:
                logger.warning(f"DB lookup for request token failed: {db_err}")

        if not token_val or not secret_val:
            raise HTTPException(400, "No active request token found. Tap Link Account again and complete within 5 minutes.")

        # NEVER silently exchange a different token than the one the user
        # authorized — E*TRADE would reject it (oauth_problem=token_rejected).
        if provided and token_val != provided:
            logger.error("Request token mismatch: app authorized a different token than the one stored")
            raise HTTPException(409, "The link session was replaced by a newer one. Tap Link Account again and use the newest code.")

        etrade_session = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, resource_owner_key=token_val, resource_owner_secret=secret_val, verifier=verifier)
        try:
            access_tokens = etrade_session.fetch_access_token(ACCESS_TOKEN_URL)
        except Exception as oauth_err:
            msg = str(oauth_err)
            logger.error(f"Access token exchange failed: {msg}")
            if "token_rejected" in msg or "401" in msg:
                raise HTTPException(400, "E*TRADE rejected the request token — it expired (5-minute limit) or was already used. Tap Link Account again and paste the fresh code right away.")
            if "verifier" in msg.lower():
                raise HTTPException(400, "E*TRADE rejected the verification code. Double-check the code and try again.")
            raise HTTPException(500, detail=msg)

        final_token = access_tokens.get("oauth_token")
        final_secret = access_tokens.get("oauth_token_secret")
        if not final_token or not final_secret:
            raise HTTPException(500, "E*TRADE did not return access tokens")
        save_tokens(final_token, final_secret)

        # Request tokens are single-use — clear all pending state.
        _pending_request_tokens.clear()
        _latest_request_token = None

        return {
            "status": "success",
            "message": "E*TRADE account linked successfully",
            "linked": True,
            "env": ENV,
            "oauth_token": final_token,
            "oauth_token_secret": final_secret,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Complete link failed: {e}")
        raise HTTPException(500, detail=str(e))


@app.get("/etrade/account")
async def get_etrade_account():
    tokens = load_tokens()
    if not tokens:
        return {"status": "not_linked", "linked": False}
    # Best-effort account enumeration so the app can show which account will
    # receive orders. Never fail the linked check on an enumeration error.
    accounts_out = []
    try:
        accounts_api = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens["oauth_token"], tokens["oauth_token_secret"],
            dev=is_sandbox,
        )
        resp = await _etrade_call(accounts_api.list_accounts, resp_format="json", source="list_accounts")
        raw = (((resp or {}).get("AccountListResponse") or {}).get("Accounts") or {}).get("Account") or []
        if isinstance(raw, dict):
            raw = [raw]
        for a in raw:
            accounts_out.append({
                "accountIdKey": a.get("accountIdKey"),
                "accountId": a.get("accountId"),
                "accountType": a.get("accountType"),
                "accountStatus": a.get("accountStatus"),
            })
    except Exception as e:
        logger.warning(f"Account enumeration failed (still linked): {e}")
    # Real balances so the app can size positions off the TRUE account value
    # instead of a stale default. Best-effort — never fail the linked check.
    balances = await _fetch_broker_balance() or {}
    return {
        "status": "linked",
        "linked": True,
        "accounts": accounts_out,
        "equity": balances.get("total"),
        "cash_buying_power": balances.get("available"),
    }


# ==================== RENEW ====================
@app.post("/etrade/auth/renew")
async def etrade_auth_renew(data: dict = Body(...)):
    try:
        access_token = data.get("access_token") or os.getenv("ETRADE_ACCESS_TOKEN")
        access_token_secret = data.get("access_token_secret") or os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
        if not access_token or not access_token_secret:
            tokens = load_tokens()
            if tokens:
                access_token = access_token or tokens["oauth_token"]
                access_token_secret = access_token_secret or tokens["oauth_token_secret"]
        if not access_token or not access_token_secret:
            raise HTTPException(400, "Missing access tokens")
        try:
            accounts = pyetrade.ETradeAccounts(CONSUMER_KEY, CONSUMER_SECRET, access_token, access_token_secret, dev=is_sandbox)
            await asyncio.to_thread(accounts.list_accounts, resp_format="json")
            return {"status": "success", "message": "Tokens are still valid", "renewed": False}
        except Exception:
            logger.info("Current tokens appear invalid. Attempting renewal...")
        auth_manager = pyetrade.ETradeAccessManager(CONSUMER_KEY, CONSUMER_SECRET, access_token, access_token_secret)
        renewed = await _etrade_call(auth_manager.renew_access_token, source="token_renew")
        if renewed:
            save_tokens(auth_manager.oauth_token, auth_manager.oauth_token_secret)
            return {"status": "success", "message": "Tokens renewed successfully", "renewed": True}
        raise HTTPException(400, "Token renewal failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Renew failed: {e}")
        raise HTTPException(500, detail="Renewal failed")


@app.post("/etrade/disconnect")
async def etrade_disconnect():
    global _current_tokens, _resolved_account_id_key
    _current_tokens = None
    _resolved_account_id_key = None
    logger.info("User requested account disconnect")
    return {"status": "success", "message": "Disconnect request received"}


# ==================== QUOTE ====================
@app.get("/etrade/quote")
async def get_quotes(symbols: str = Query(...)):
    tokens = load_tokens()
    if not tokens:
        raise HTTPException(401, "E*TRADE account not linked")
    market = pyetrade.ETradeMarket(CONSUMER_KEY, CONSUMER_SECRET, tokens["oauth_token"], tokens["oauth_token_secret"], dev=is_sandbox)
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    try:
        # Exponential backoff covers throttles, 5xx AND the post-renewal 401s
        # the old hand-rolled fixed-sleep loop existed for.
        return await _etrade_call(market.get_quote, symbol_list, resp_format="json", source="quote")
    except Exception as e:
        raise HTTPException(500, detail=str(e))


# ==================== DATABASE ====================
async def init_db():
    """Initialize the async DB engine used for token persistence.

    FIX FOR THE DAILY RELINK CYCLE: hosting platforms (Railway, Render, …)
    provide DATABASE_URL as "postgres://…" or "postgresql://…" WITHOUT the
    async driver suffix. create_async_engine requires asyncpg in the URL —
    previously the raw URL was passed straight through, engine creation
    failed silently, async_session stayed None, and tokens only lived in RAM
    (lost on every container restart → forced daily relink). This normalizes
    the URL to postgresql+asyncpg:// (preserving credentials/host/params),
    enables pool_pre_ping for stale-connection recovery, and logs the exact
    hostname/credential problem when the connection still fails before
    falling back to SQLite so the bot keeps running.
    """
    global engine, async_session
    engine = None
    async_session = None

    if DATABASE_URL:
        url = DATABASE_URL.strip()
        original_url_for_log = url
        # Normalize common postgres URLs to the asyncpg driver.
        if url.startswith("postgres://"):
            url = "postgresql+asyncpg://" + url[len("postgres://"):]
        elif url.startswith("postgresql://") and "+asyncpg" not in url:
            url = "postgresql+asyncpg://" + url[len("postgresql://"):]
        if url.startswith("postgresql+") and "+asyncpg" not in url:
            # e.g. postgresql+psycopg2://… → force asyncpg
            _prefix, rest = url.split("://", 1)
            url = "postgresql+asyncpg://" + rest

        try:
            import asyncpg  # noqa: F401  # ensure the driver is installed
            engine = create_async_engine(
                url,
                echo=False,
                pool_pre_ping=True,  # detect/recover stale cloud-DB connections
            )
            safe_log = url.split("@", 1)[1] if "@" in url else url
            logger.info(f"✅ DATABASE_URL normalized — postgres engine created @ {safe_log}")
        except ImportError:
            logger.warning(
                "asyncpg not installed — cannot use postgres DATABASE_URL "
                "(pip install asyncpg). Tokens will use the SQLite fallback "
                "and will NOT persist across restarts."
            )
            engine = None
        except Exception as conn_err:
            masked = original_url_for_log.split("@")[-1] if "@" in original_url_for_log else original_url_for_log
            logger.error(
                f"❌ Database connection FAILED (wrong hostname, port, credentials, "
                f"SSL, or network issue): {conn_err} | DATABASE_URL host (masked): {masked}"
            )
            logger.warning("→ Falling back to SQLite — tokens will NOT survive restarts until DATABASE_URL is fixed")
            engine = None

    if engine is None:
        try:
            engine = create_async_engine("sqlite+aiosqlite:///etrade_cache.db", echo=False)
        except Exception as sqlite_err:
            logger.error(f"Even the SQLite fallback failed: {sqlite_err}")
            engine = None
            async_session = None
            return  # bot continues without DB; tokens won't persist at all

    try:
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Database ready (tokens will persist across restarts)")
    except Exception as e:
        logger.error(f"Database table creation / sessionmaker error: {e}")
        async_session = None  # downstream checks must see the failure


# ==================== ACCOUNT KEY RESOLUTION ====================
async def _resolve_account_id_key(tokens: Dict[str, str]) -> str:
    """Return the E*TRADE accountIdKey required by order/balance APIs.

    E*TRADE rejects the plain account number with
    "Error 102: Please enter valid Account Key". The real accountIdKey is an
    opaque value only obtainable from /accounts/list. If ETRADE_ACCOUNT_ID is
    set, match it against BOTH accountId and accountIdKey; otherwise (or if no
    match) fall back to the first ACTIVE account. Cached per linked session.
    """
    global _resolved_account_id_key
    if _resolved_account_id_key:
        return _resolved_account_id_key

    accounts_api = pyetrade.ETradeAccounts(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens["oauth_token"], tokens["oauth_token_secret"],
        dev=is_sandbox,
    )
    resp = await _etrade_call(accounts_api.list_accounts, resp_format="json", source="resolve_account")
    account_list = (((resp or {}).get("AccountListResponse") or {}).get("Accounts") or {}).get("Account") or []
    if isinstance(account_list, dict):
        account_list = [account_list]
    if not account_list:
        raise Exception("E*TRADE returned no accounts for the linked session")

    target = (TARGET_ACCOUNT_ID or "").strip()
    chosen = None
    if target:
        for acct in account_list:
            if target in {str(acct.get("accountId", "")), str(acct.get("accountIdKey", ""))}:
                chosen = acct
                break
        if not chosen:
            logger.warning(
                f"⚠️ ETRADE_ACCOUNT_ID does not match any accountId/accountIdKey "
                f"among {len(account_list)} account(s) — falling back to first ACTIVE account"
            )
    if not chosen:
        chosen = next(
            (a for a in account_list if str(a.get("accountStatus", "")).upper() == "ACTIVE"),
            account_list[0],
        )

    key = chosen.get("accountIdKey")
    if not key:
        raise Exception("Matched E*TRADE account has no accountIdKey")
    _resolved_account_id_key = str(key)
    acct_id = str(chosen.get("accountId", ""))
    logger.info(f"✅ Resolved accountIdKey for account ****{acct_id[-4:]} (desc={chosen.get('accountDesc', 'n/a')})")
    return _resolved_account_id_key


# ==================== SAFETY (distributed circuit breaker) ====================
async def _breaker_is_open() -> bool:
    return await state.exists(BREAKER_OPEN_KEY)


async def _reset_circuit_breaker(reason: str) -> None:
    try:
        if await state.exists(BREAKER_OPEN_KEY):
            logger.info(f"🔓 Circuit breaker reset — {reason}")
            await alerts.send("info", "circuit_breaker_reset",
                              f"Circuit breaker reset — {reason}",
                              dedupe_key="breaker_reset")
        await state.delete(BREAKER_OPEN_KEY, BREAKER_FAILURES_KEY)
    except Exception as e:
        logger.warning(f"breaker reset failed: {e}")


async def _record_api_success() -> None:
    """A successful broker call closes the failure streak."""
    await state.delete(BREAKER_FAILURES_KEY)


async def _record_api_failure(source: str, error: str) -> None:
    """Count a broker/API failure toward the DISTRIBUTED breaker; trip it
    (TTL = cooldown → auto-reset) and alert when the threshold is crossed."""
    try:
        fails = await state.incr(BREAKER_FAILURES_KEY, ex=BREAKER_FAILURE_WINDOW_SECONDS)
    except Exception as e:
        logger.warning(f"breaker failure count error: {e}")
        return
    logger.warning(f"⚠️ API failure {fails}/{MAX_CONSECUTIVE_FAILURES} ({source}): {error}")
    if fails >= MAX_CONSECUTIVE_FAILURES and not await state.exists(BREAKER_OPEN_KEY):
        await state.set(BREAKER_OPEN_KEY, source, ex=CIRCUIT_BREAKER_COOLDOWN_SECONDS)
        logger.error(
            f"⛔ Circuit breaker OPEN after {fails} consecutive API failures ({source}) "
            f"— all workers halted; auto-resets in {CIRCUIT_BREAKER_COOLDOWN_SECONDS // 60} minutes"
        )
        await trade_ledger.record("circuit_breaker_tripped", {
            "source": source, "failures": fails, "last_error": str(error)[:300],
            "cooldown_seconds": CIRCUIT_BREAKER_COOLDOWN_SECONDS,
        })
        await alerts.send(
            "critical", "circuit_breaker_tripped",
            f"Order placement HALTED after {fails} consecutive API failures ({source}): {error}",
            dedupe_key="breaker_trip",
        )


async def check_risk_limits():
    if await _breaker_is_open():
        raise HTTPException(
            503,
            f"Circuit breaker open — broker API failing; auto-resets within "
            f"{CIRCUIT_BREAKER_COOLDOWN_SECONDS // 60} minutes (or relink the account)",
        )


# ==================== E*TRADE CALL BACKOFF ====================
# Every E*TRADE API call goes through exponential backoff with jitter — the
# raw async client retries inside etrade_async._request; the pyetrade paths
# retry through _etrade_call / _sync_etrade_call below.
_RETRYABLE_ERROR_MARKERS = (
    "408", "429", "500", "502", "503", "504",
    "timeout", "timed out", "connection", "temporarily", "unavailable",
    "reset by peer", "max retries", "401", "unauthorized",
)


def _backoff_delay(attempt: int, exact: bool = False) -> float:
    """Exponential backoff: base * 2^attempt capped at the max.
    exact=True (429 throttle) honours the full schedule — 2s, 4s, 8s with the
    defaults — because a throttle wait must never be shortened. Other errors
    apply a [0.5, 1.0] jitter factor to de-synchronize workers."""
    delay = min(ETRADE_RETRY_MAX_SECONDS, ETRADE_RETRY_BASE_SECONDS * (2 ** attempt))
    if exact:
        return delay
    return delay * (0.5 + random.random() * 0.5)


def _is_throttle(e: Exception) -> bool:
    """429 rate-limit errors: the broker never processed the request, so a
    retry is safe even for order placement."""
    return "429" in str(e)


def _is_retryable_error(e: Exception) -> bool:
    """Transient broker failures worth retrying: throttles (429), 5xx, network
    drops, and 401s (E*TRADE tokens briefly 401 right after issue/renewal).
    Validation rejections (1011/2040/2009, missing params) are NOT retried."""
    text = str(e).lower()
    return any(marker in text for marker in _RETRYABLE_ERROR_MARKERS)


def _sync_etrade_call(fn, *args, source: str = "etrade", **kwargs):
    """Exponential-backoff wrapper for SYNC pyetrade calls running inside a
    worker thread (e.g. contract snapping) — blocking sleeps are fine there."""
    for attempt in range(ETRADE_RETRY_ATTEMPTS):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt >= ETRADE_RETRY_ATTEMPTS - 1 or not _is_retryable_error(e):
                raise
            delay = _backoff_delay(attempt, exact=_is_throttle(e))
            logger.warning(
                f"E*TRADE {source} failed (attempt {attempt + 1}/{ETRADE_RETRY_ATTEMPTS}): {e} "
                f"— retrying in {delay:.1f}s"
            )
            time.sleep(delay)


async def _etrade_call(fn, *args, source: str = "etrade", idempotent: bool = True, **kwargs):
    """Run a sync pyetrade call in a thread with exponential backoff + jitter.
    idempotent=False (order placement) executes EXACTLY once — a failed place
    may still have reached the broker, so a blind retry risks a double order
    (the reconciliation engine heals whatever state results)."""
    for attempt in range(ETRADE_RETRY_ATTEMPTS):
        try:
            return await asyncio.to_thread(fn, *args, **kwargs)
        except Exception as e:
            # A 429 was never processed by the broker — safe to retry even for
            # non-idempotent calls (order placement). Anything else follows the
            # exactly-once rule for non-idempotent requests.
            throttled = _is_throttle(e)
            if (not idempotent and not throttled) or attempt >= ETRADE_RETRY_ATTEMPTS - 1 or not _is_retryable_error(e):
                raise
            delay = _backoff_delay(attempt, exact=throttled)
            logger.warning(
                f"E*TRADE {source} failed (attempt {attempt + 1}/{ETRADE_RETRY_ATTEMPTS}): {e} "
                f"— retrying in {delay:.1f}s"
            )
            await asyncio.sleep(delay)


# ==================== STOP GUARD ====================
# Makes the protective stop REST AT THE BROKER. The entry order goes in first;
# an async watcher polls the broker until the entry fills, then places a real
# STOP order at E*TRADE sized to the filled quantity (re-placed as partial
# fills grow — never two live stops at once). Entries with zero fill by
# ENTRY_FILL_TIMEOUT_MIN are cancelled. Guard state persists to STATE_FILE and
# resumes after a restart.
_TERMINAL_ORDER_STATUSES = {"EXECUTED", "CANCELLED", "REJECTED", "EXPIRED"}


def _order_id_from_place(placed: Any) -> Optional[str]:
    """Extract the numeric orderId from a PlaceOrderResponse."""
    try:
        body = placed.get("PlaceOrderResponse", placed) if isinstance(placed, dict) else {}
        ids = body.get("OrderIds") or body.get("orderIds") or []
        if isinstance(ids, dict):
            ids = [ids]
        for entry in ids:
            oid = entry.get("orderId") if isinstance(entry, dict) else entry
            if oid:
                return str(oid)
        oid = body.get("orderId") or body.get("OrderId")
        return str(oid) if oid else None
    except (AttributeError, TypeError):
        return None


def _orders_client(tokens: Dict[str, str]) -> "pyetrade.ETradeOrder":
    return pyetrade.ETradeOrder(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens["oauth_token"], tokens["oauth_token_secret"],
        dev=is_sandbox,
    )


def _raw_client(tokens: Dict[str, str]) -> ETradeAsyncClient:
    """Async httpx client with raw JSON Order API payloads (primary path)."""
    return ETradeAsyncClient(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens["oauth_token"], tokens["oauth_token_secret"],
        sandbox=is_sandbox,
    )


async def _place_order_smart(kind: str, common: dict, tokens: Dict[str, str]) -> Any:
    """Primary path: raw async JSON payload against E*TRADE's Order API
    (full control, true multi-leg capable). Fallback: pyetrade flat kwargs —
    the proven v4 path. `common` uses the flat pyetrade vocabulary so both
    paths share one source of truth."""
    if USE_RAW_ORDER_API:
        try:
            client = _raw_client(tokens)
            acct = common["accountIdKey"]
            cid = str(common["clientOrderId"])
            price_type = str(common.get("priceType") or "MARKET")
            limit_price = common.get("limitPrice")
            stop_price = common.get("stopPrice")
            if kind == "option":
                return await client.place_option(
                    acct, cid, str(common["symbol"]), str(common["callPut"]),
                    float(common["strikePrice"]), str(common["expiryDate"]),
                    str(common["orderAction"]), int(common["quantity"]),
                    price_type, limit_price=limit_price, stop_price=stop_price,
                )
            return await client.place_equity(
                acct, cid, str(common["symbol"]), str(common["orderAction"]),
                int(common["quantity"]), price_type,
                limit_price=limit_price, stop_price=stop_price,
            )
        except ETradeAPIError as e:
            logger.warning(f"RAW order path rejected ({e}) — falling back to pyetrade")
        except Exception as e:
            logger.warning(f"RAW order path error ({e}) — falling back to pyetrade")
    orders = _orders_client(tokens)
    fn = orders.place_option_order if kind == "option" else orders.place_equity_order
    # NON-IDEMPOTENT — executed exactly once (no blind retry of a place).
    return await _etrade_call(fn, source=f"place_{kind}", idempotent=False, **common)


_INSUFFICIENT_FUNDS_QTY_RE = re.compile(
    r"maximum allowable quantity was estimated to be\s+(\d+)", re.IGNORECASE
)


def _max_qty_from_insufficient_funds(message: str) -> Optional[int]:
    """Parse E*TRADE error 8400's embedded max quantity hint.

    E*TRADE rejects over-sized orders with:
      "Code: 8400 ... insufficient funds ... the maximum allowable quantity
       was estimated to be 57."
    Returns the parsed integer, or None when the message carries no hint.
    """
    if not message or ("8400" not in message and "insufficient funds" not in message.lower()):
        return None
    m = _INSUFFICIENT_FUNDS_QTY_RE.search(message)
    if not m:
        return None
    try:
        qty = int(m.group(1))
        return qty if qty > 0 else None
    except ValueError:
        return None


FUNDS_SAFETY_MARGIN = float(os.getenv("FUNDS_SAFETY_MARGIN", "0.95"))


async def _place_entry_with_funds_clamp(
    kind: str, common: dict, tokens: Dict[str, str], unit_cost: Optional[float] = None,
) -> Tuple[Any, int]:
    """Place an ENTRY order with three layers of funds protection:

    1. PRE-FLIGHT (proactive): when the per-unit cost is known, clamp the
       quantity so the estimated cost fits within tracked available funds
       (95% safety margin) — and refuse outright when even ONE unit is
       unaffordable, so oversized orders never reach the broker at all.
    2. BACKSTOP (reactive): on an 8400 insufficient-funds rejection anyway,
       clamp to the broker's own max-quantity hint and retry ONCE with a
       fresh clientOrderId.
    3. LEDGER: debit the tracked balance by the placed cost so back-to-back
       entries can't over-spend between broker balance refreshes.

    Never used for closes — a close must always cover the full position.
    Returns (broker_response, quantity_actually_sent).
    """
    requested = int(common["quantity"])
    unit_name = "contract" if kind == "option" else "share"
    if unit_cost is not None and unit_cost > 0:
        available = await _available_funds()
        if available is not None:
            budget = max(0.0, available) * FUNDS_SAFETY_MARGIN
            affordable = int(budget // unit_cost)
            if affordable < 1:
                raise Exception(
                    f"insufficient funds: available ≈${available:.2f}, one {unit_name} of "
                    f"{common.get('symbol')} costs ≈${unit_cost:.2f} — trade refused before reaching broker"
                )
            if requested > affordable:
                logger.warning(
                    f"💰 Pre-flight size clamp: {common.get('symbol')} qty {requested} → {affordable} "
                    f"(available ≈${available:.2f}, {unit_name} cost ≈${unit_cost:.2f})"
                )
                common = dict(common)
                common["quantity"] = affordable
                requested = affordable
        else:
            logger.warning(
                f"⚠️ No balance available for pre-flight sizing of {common.get('symbol')} — "
                f"relying on broker-side 8400 clamp backstop"
            )

    placed_qty = requested
    try:
        final = await _place_order_smart(kind, common, tokens)
    except Exception as e:
        max_qty = _max_qty_from_insufficient_funds(str(e))
        if max_qty is None:
            raise
        clamped = max(1, int(max_qty * 0.95))
        if clamped >= requested:
            raise  # hint doesn't actually reduce the order — don't loop
        retry = dict(common)
        retry["quantity"] = clamped
        # A rejected order may still burn the clientOrderId — retry with a
        # fresh one (E*TRADE caps clientOrderId at 20 chars).
        retry["clientOrderId"] = (str(common["clientOrderId"])[:19] + "R")
        logger.warning(
            f"💰 Insufficient funds for qty={requested} {common.get('symbol')} — "
            f"broker max ≈{max_qty}; retrying ONCE with clamped qty={clamped}"
        )
        final = await _place_order_smart(kind, retry, tokens)
        logger.info(f"✅ Clamped retry accepted: {clamped}x {common.get('symbol')} (was {requested})")
        placed_qty = clamped
    if unit_cost is not None and unit_cost > 0:
        try:
            await state.adjust_balance(-float(unit_cost) * placed_qty)
        except Exception as e:
            logger.warning(f"balance debit failed (non-fatal): {e}")
    return final, placed_qty


async def _order_state(order_id: Optional[str], client_id: Optional[str]) -> Tuple[str, int]:
    """Return (status, total filled quantity) for an order, matched by orderId
    or clientOrderId in the account's recent orders. ('NOT_FOUND', 0) when the
    order is not in the list."""
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")
    acct_key = await _resolve_account_id_key(tokens)
    orders = _orders_client(tokens)
    resp = await _etrade_call(orders.list_orders, acct_key, resp_format="json", source="list_orders")
    root = (resp or {}).get("OrdersResponse", {}) if isinstance(resp, dict) else {}
    order_list = root.get("Order") or []
    if isinstance(order_list, dict):
        order_list = [order_list]
    for o in order_list:
        if not isinstance(o, dict):
            continue
        oid = str(o.get("orderId") or "")
        details = o.get("OrderDetail") or []
        if isinstance(details, dict):
            details = [details]
        matches = bool(order_id) and oid == str(order_id)
        if not matches and client_id:
            matches = any(
                str(d.get("clientOrderId") or "") == client_id
                for d in details if isinstance(d, dict)
            )
        if not matches:
            continue
        status = "OPEN"
        filled = 0
        for d in details:
            if not isinstance(d, dict):
                continue
            status = str(d.get("status") or status).upper()
            instruments = d.get("Instrument") or []
            if isinstance(instruments, dict):
                instruments = [instruments]
            for inst in instruments:
                if not isinstance(inst, dict):
                    continue
                try:
                    filled += int(float(inst.get("filledQuantity") or 0))
                except (TypeError, ValueError):
                    pass
        return status, filled
    return "NOT_FOUND", 0


async def _cancel_order_safe(order_id: Optional[str]) -> bool:
    """Best-effort broker order cancel. Returns True only when the cancel call
    succeeded — callers must treat False as 'the order may still be live'."""
    if not order_id:
        return False
    try:
        tokens = load_tokens()
        if not tokens:
            return False
        acct_key = await _resolve_account_id_key(tokens)
        orders = _orders_client(tokens)
        await _etrade_call(orders.cancel_order, acct_key, int(order_id), resp_format="json", source="cancel_order")
        logger.info(f"[STOP GUARD] cancelled order {order_id}")
        return True
    except Exception as e:
        logger.warning(f"order cancel failed ({order_id}): {e}")
        return False


async def _place_protective_stop(ticker: str, action: str, qty: int, stop_price: float) -> dict:
    """Rest a protective STOP order at E*TRADE for a filled equity entry."""
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")
    acct_key = await _resolve_account_id_key(tokens)
    exit_side = "SELL" if action == "BUY" else "BUY_TO_COVER"
    client_id = str(uuid.uuid4().int)[:18]
    common = dict(
        resp_format="json",
        accountIdKey=acct_key,
        symbol=ticker,
        orderAction=exit_side,
        clientOrderId=client_id,
        priceType="STOP",
        stopPrice=round(float(stop_price), 2),
        quantity=int(qty),
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        allOrNone=False,
    )
    placed = await _place_order_smart("equity", common, tokens)
    order_id = _order_id_from_place(placed)
    logger.info(
        f"[STOP GUARD] protective stop RESTING at broker: {exit_side} {ticker} "
        f"qty={qty} stop={stop_price:.2f} (order={order_id})"
    )
    await trade_ledger.record("protective_stop_placed", {
        "ticker": ticker, "kind": "equity", "side": exit_side,
        "qty": int(qty), "stop": round(float(stop_price), 2), "order_id": order_id,
    })
    return {"order_id": order_id, "client_id": client_id, "qty": int(qty), "stop": round(float(stop_price), 2)}


async def _place_option_protective_stop(ticker: str, contract: dict, qty: int, stop_premium: float) -> dict:
    """Rest a protective SELL_CLOSE STOP at E*TRADE on the same OCC contract
    for a filled option entry — the option-side equivalent of the equity stop
    guard. `stop_premium` is the option PREMIUM trigger, tick-rounded."""
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")
    if not contract or not contract.get("right") or not contract.get("expiration"):
        raise Exception(f"option guard for {ticker} missing contract details")
    acct_key = await _resolve_account_id_key(tokens)
    client_id = str(uuid.uuid4().int)[:18]
    stop_px = _round_to_option_tick(float(stop_premium), "down")
    common = dict(
        resp_format="json",
        accountIdKey=acct_key,
        symbol=ticker,
        orderAction="SELL_CLOSE",
        clientOrderId=client_id,
        priceType="STOP",
        stopPrice=stop_px,
        quantity=int(qty),
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        allOrNone=False,
        callPut=str(contract["right"]).upper(),
        strikePrice=float(contract["strike"]),
        expiryDate=str(contract["expiration"])[:10],
    )
    placed = await _place_order_smart("option", common, tokens)
    order_id = _order_id_from_place(placed)
    logger.info(
        f"[STOP GUARD] option protective stop RESTING at broker: SELL_CLOSE {ticker} "
        f"{contract.get('right')} {contract.get('strike')} {contract.get('expiration')} "
        f"qty={qty} stop_premium={stop_px:.2f} (order={order_id})"
    )
    await trade_ledger.record("protective_stop_placed", {
        "ticker": ticker, "kind": "option", "side": "SELL_CLOSE",
        "qty": int(qty), "stop_premium": stop_px, "order_id": order_id,
        "contract": {"right": contract.get("right"), "strike": contract.get("strike"),
                     "expiration": contract.get("expiration")},
    })
    return {"order_id": order_id, "client_id": client_id, "qty": int(qty), "stop": stop_px}


async def _emergency_flatten(ticker: str, guard: dict, qty: int) -> Optional[str]:
    """LAST-RESORT market close of a filled-but-unprotected entry (stop
    placement timeout). Certainty of fill beats price — always MARKET."""
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")
    acct_key = await _resolve_account_id_key(tokens)
    client_id = str(uuid.uuid4().int)[:18]
    is_option = str(guard.get("kind") or "equity") == "option"
    if is_option:
        contract = dict(guard.get("contract") or {})
        if not contract.get("right") or not contract.get("expiration"):
            raise Exception(f"cannot flatten {ticker} — option contract details missing")
        common = dict(
            resp_format="json",
            accountIdKey=acct_key,
            symbol=ticker,
            orderAction="SELL_CLOSE",
            clientOrderId=client_id,
            priceType="MARKET",
            quantity=int(qty),
            orderTerm="GOOD_FOR_DAY",
            marketSession="REGULAR",
            allOrNone=False,
            callPut=str(contract["right"]).upper(),
            strikePrice=float(contract["strike"]),
            expiryDate=str(contract["expiration"])[:10],
        )
        placed = await _place_order_smart("option", common, tokens)
    else:
        exit_side = "SELL" if str(guard.get("action") or "BUY").upper() == "BUY" else "BUY_TO_COVER"
        common = dict(
            resp_format="json",
            accountIdKey=acct_key,
            symbol=ticker,
            orderAction=exit_side,
            clientOrderId=client_id,
            priceType="MARKET",
            quantity=int(qty),
            orderTerm="GOOD_FOR_DAY",
            marketSession="REGULAR",
            allOrNone=False,
        )
        placed = await _place_order_smart("equity", common, tokens)
    return _order_id_from_place(placed)


async def _finish_guard(ticker: str, result: str) -> None:
    await state.update_guard(ticker, done=True, result=result)
    logger.info(f"[STOP GUARD] {ticker} finished: {result}")
    await trade_ledger.record("guard_finished", {"ticker": ticker, "result": result})


async def _stop_guard_worker(ticker: str) -> None:
    """Poll the entry order; once (partially) filled, rest a protective STOP at
    the broker sized to the filled quantity (equity STOP, or SELL_CLOSE STOP on
    the same OCC contract for options). Cancel entries with zero fill at the
    deadline. Guard state lives in Redis (survives restarts); the per-ticker
    distributed lock guarantees exactly one worker guards a ticker across all
    instances."""
    lock = state.lock(f"guard:{ticker}", ttl_ms=(STOP_GUARD_POLL_SECONDS + 60) * 1000, wait_timeout=0.5)
    if not await lock.try_acquire():
        logger.info(f"[STOP GUARD] {ticker} already guarded by another worker — skipping")
        return
    logger.info(f"[STOP GUARD] watching {ticker} entry fill")
    try:
        while True:
            await lock.extend()
            guard = await state.get_guard(ticker)
            if not guard or guard.get("done"):
                return
            is_option = str(guard.get("kind") or "equity") == "option"

            status = str(guard.get("last_status") or "OPEN")
            filled = int(guard.get("last_filled") or 0)
            try:
                status, filled = await _order_state(guard.get("entry_order_id"), guard.get("entry_client_id"))
            except Exception as e:
                logger.warning(f"[STOP GUARD] {ticker} poll failed: {e}")

            # TIMEOUT CIRCUIT BREAKER anchor — the moment the FIRST fill is
            # detected, the stop-placement clock restarts: the protective stop
            # must rest at the broker within STOP_PLACEMENT_TIMEOUT_SECONDS of
            # the fill (not of entry placement), or the breaker below trips.
            if filled > 0 and not guard.get("fill_detected_ts") and STOP_PLACEMENT_TIMEOUT_SECONDS > 0:
                guard = await state.update_guard(
                    ticker,
                    fill_detected_ts=time.time(),
                    stop_deadline_ts=time.time() + STOP_PLACEMENT_TIMEOUT_SECONDS,
                ) or guard

            guarded = int(guard.get("guarded_qty") or 0)
            if filled > guarded:
                # (Re)place the protective stop for the total filled quantity.
                can_place = True
                if guard.get("stop_order_id"):
                    # Replace flow: only place a new stop if the old one is truly
                    # cancelled — never risk two live stops double-selling.
                    can_place = await _cancel_order_safe(guard.get("stop_order_id"))
                if can_place:
                    try:
                        if is_option:
                            stop_info = await _place_option_protective_stop(
                                ticker, dict(guard.get("contract") or {}), filled,
                                float(guard.get("stop") or 0),
                            )
                        else:
                            stop_info = await _place_protective_stop(
                                ticker, str(guard.get("action") or "BUY"), filled,
                                float(guard.get("stop") or 0),
                            )
                        guard = await state.update_guard(
                            ticker,
                            guarded_qty=filled,
                            stop_order_id=stop_info["order_id"],
                            stop_client_id=stop_info["client_id"],
                        ) or guard
                        pos = await state.get_position(ticker)
                        if pos:
                            pos["stop_order_id"] = stop_info["order_id"]
                            pos["filled_qty"] = filled
                            await state.set_position(ticker, pos)
                        guarded = filled
                    except Exception as e:
                        logger.error(f"[STOP GUARD] {ticker} stop placement FAILED (will retry): {e}")
                        await alerts.send(
                            "critical", "stop_placement_failed",
                            f"{ticker}: protective stop placement failed (guard will retry): {e}",
                            dedupe_key=f"guard_fail:{ticker}",
                        )

            guard = await state.update_guard(ticker, last_filled=filled, last_status=status) or guard

            # STOP PLACEMENT TIMEOUT CIRCUIT BREAKER — the bracket must be
            # complete (a protective stop RESTING at the broker) within
            # STOP_PLACEMENT_TIMEOUT_SECONDS. The clock starts at entry
            # placement and RE-ANCHORS to the first detected fill (above), so a
            # fill always gets the full window. Past the deadline with NO stop:
            #   • unfilled entry → cancel the entry order immediately
            #   • filled entry   → cancel any live remainder immediately, then
            #                      emergency market-flatten (a fill cannot be
            #                      cancelled — flattening is the equivalent).
            # The stop-placement attempt above always runs FIRST, so this only
            # fires when protection genuinely failed to stick in time.
            stop_deadline = float(guard.get("stop_deadline_ts") or 0)
            if stop_deadline and not guard.get("stop_order_id") and time.time() >= stop_deadline:
                if filled > 0:
                    if status not in _TERMINAL_ORDER_STATUSES:
                        await _cancel_order_safe(guard.get("entry_order_id"))
                    try:
                        flatten_id = await _emergency_flatten(ticker, guard, filled)
                        await _record_close(ticker, None, {"ticker": ticker, "action": guard.get("action") or "BUY"})
                        await _finish_guard(ticker, "stop_timeout_flattened")
                        await trade_ledger.record("stop_timeout_flattened", {
                            "ticker": ticker, "qty": filled, "flatten_order_id": flatten_id,
                            "timeout_seconds": STOP_PLACEMENT_TIMEOUT_SECONDS,
                        })
                        await alerts.send(
                            "critical", "stop_timeout_flattened",
                            f"{ticker}: no protective stop resting {STOP_PLACEMENT_TIMEOUT_SECONDS}s after entry "
                            f"— position flattened at market (qty={filled}, order={flatten_id})",
                            dedupe_key=f"stop_timeout:{ticker}",
                        )
                        return
                    except Exception as e:
                        logger.error(f"[STOP GUARD] {ticker} emergency flatten FAILED (will retry): {e}")
                        await alerts.send(
                            "critical", "stop_timeout_flatten_failed",
                            f"{ticker}: UNPROTECTED past the {STOP_PLACEMENT_TIMEOUT_SECONDS}s stop deadline and "
                            f"the emergency flatten failed (guard keeps retrying): {e}",
                            dedupe_key=f"stop_timeout_fail:{ticker}",
                        )
                        # Fall through — next poll retries the stop placement
                        # first, then this flatten again if it still won't stick.
                else:
                    await _cancel_order_safe(guard.get("entry_order_id"))
                    # Re-poll after the cancel: a fill may have raced it. If it
                    # did, keep looping — the stop gets placed next iteration
                    # (or the flatten branch above fires).
                    late_filled = 0
                    try:
                        _s2, late_filled = await _order_state(guard.get("entry_order_id"), guard.get("entry_client_id"))
                    except Exception as e:
                        logger.warning(f"[STOP GUARD] {ticker} post-cancel poll failed: {e}")
                    if late_filled > 0:
                        logger.warning(f"[STOP GUARD] {ticker} filled during timeout cancel (qty={late_filled}) — continuing guard")
                    else:
                        await _finish_guard(ticker, "stop_timeout_entry_cancelled")
                        await state.delete_position(ticker)
                        await trade_ledger.record("stop_timeout_entry_cancelled", {
                            "ticker": ticker, "entry_order_id": guard.get("entry_order_id"),
                            "timeout_seconds": STOP_PLACEMENT_TIMEOUT_SECONDS,
                        })
                        await alerts.send(
                            "warning", "stop_timeout_entry_cancelled",
                            f"{ticker}: entry unfilled and no protective stop within "
                            f"{STOP_PLACEMENT_TIMEOUT_SECONDS}s — entry auto-cancelled",
                            dedupe_key=f"stop_timeout:{ticker}",
                        )
                        return

            if status == "EXECUTED" and filled > 0 and guarded >= filled:
                await _finish_guard(ticker, "filled_and_protected")
                return
            if status in _TERMINAL_ORDER_STATUSES and status != "EXECUTED" and filled == 0:
                await _finish_guard(ticker, f"entry_{status.lower()}_unfilled")
                await state.delete_position(ticker)
                return
            if time.time() >= float(guard.get("deadline_ts") or 0):
                if filled == 0:
                    await _cancel_order_safe(guard.get("entry_order_id"))
                    await _finish_guard(ticker, "entry_timeout_cancelled")
                    await state.delete_position(ticker)
                    return
                if guarded >= filled:
                    await _finish_guard(ticker, "partial_fill_protected")
                    return
                # Filled but stop never stuck — keep trying rather than walk away.
                logger.error(f"[STOP GUARD] {ticker} UNPROTECTED at deadline — extending guard")
                await alerts.send(
                    "critical", "position_unprotected",
                    f"{ticker}: filled entry still has NO resting stop at the guard deadline — guard extended, retrying",
                    dedupe_key=f"unprotected:{ticker}",
                )
                await state.update_guard(ticker, deadline_ts=time.time() + ENTRY_FILL_TIMEOUT_MIN * 60)
            # Poll faster while the stop deadline is live and unmet so the 60s
            # rule is enforced with tight granularity, not at the next 10s tick.
            sleep_s = float(STOP_GUARD_POLL_SECONDS)
            if stop_deadline and not guard.get("stop_order_id"):
                sleep_s = max(1.0, min(sleep_s, stop_deadline - time.time()))
            await asyncio.sleep(sleep_s)
    finally:
        await lock.release()


def _spawn_guard(ticker: str) -> None:
    asyncio.create_task(_stop_guard_worker(ticker))


async def _arm_stop_guard(ticker: str, action: str, stop_price: float,
                          entry_order_id: Optional[str], entry_client_id: str,
                          kind: str = "equity", contract: Optional[dict] = None) -> None:
    """Persist guard state to Redis (restart-safe) and spawn the watcher task.
    kind='option' guards rest a SELL_CLOSE STOP on the same OCC contract;
    `stop_price` is then the protective PREMIUM trigger."""
    await state.set_guard(ticker, {
        "ticker": ticker,
        "kind": kind,
        "action": action,
        "stop": round(float(stop_price), 2),
        "contract": contract,
        "entry_order_id": entry_order_id,
        "entry_client_id": entry_client_id,
        "guarded_qty": 0,
        "last_filled": 0,
        "last_status": "OPEN",
        "stop_order_id": None,
        "stop_client_id": None,
        "armed_ts": time.time(),
        "deadline_ts": time.time() + ENTRY_FILL_TIMEOUT_MIN * 60,
        # STOP PLACEMENT TIMEOUT deadline — initially anchored at entry
        # placement; the guard worker re-anchors it to the first detected fill
        # so protection always gets the full window after the fill.
        "stop_deadline_ts": (time.time() + STOP_PLACEMENT_TIMEOUT_SECONDS) if STOP_PLACEMENT_TIMEOUT_SECONDS > 0 else 0,
        "fill_detected_ts": None,
        "done": False,
        "result": None,
    })
    _spawn_guard(ticker)


async def _resume_guards() -> None:
    """Respawn watcher tasks for guards interrupted by a restart. The guard
    lock makes this safe when several workers boot at once — only one wins."""
    pending = await state.pending_guards()
    for t in pending:
        logger.info(f"[STOP GUARD] resuming guard for {t} after restart")
        _spawn_guard(t)


# ==================== POSITION LEDGER (Redis-backed) ====================
async def _record_open(ticker: str, qty: int, entry: Optional[float], stop: Optional[float],
                       target: Optional[float], contract: Optional[dict],
                       action: str = "BUY") -> None:
    await state.set_position(ticker, {
        "qty": int(qty),
        "action": str(action or "BUY").upper(),
        "entry": float(entry) if entry else None,
        "stop": float(stop) if stop else None,
        "target": float(target) if target else None,
        "ts": _utcnow().isoformat(),
        "contract": contract,
    })
    await state.incr_trades_today()
    await trade_ledger.record("position_opened", {
        "ticker": ticker, "qty": int(qty), "action": str(action or "BUY").upper(),
        "entry": float(entry) if entry else None,
        "stop": float(stop) if stop else None,
        "target": float(target) if target else None,
        "contract": contract,
    })


async def _record_close(ticker: str, exit_price: Optional[float], payload: dict) -> None:
    """Pop the position and feed realized pnl (underlying move, signed by
    direction) into the daily loss-limit accounting — plus credit equity
    proceeds back into the tracked balance so wins/losses immediately update
    the funds available for the next entry."""
    pos = await state.delete_position(ticker)
    entry = float((pos or {}).get("entry") or payload.get("entry") or 0)
    exit_px = float(exit_price or payload.get("exit_price") or payload.get("limit_price") or entry or 0)
    direction = 1.0 if str(payload.get("action") or "BUY").upper() == "BUY" else -1.0
    qty = int((pos or {}).get("filled_qty") or (pos or {}).get("qty") or 0)
    is_option = bool((pos or {}).get("contract"))
    pnl_pct: Optional[float] = None
    realized_usd: Optional[float] = None
    if entry > 0 and exit_px > 0:
        pnl_pct = direction * ((exit_px - entry) / entry * 100.0)
        await state.add_realized_pnl(pnl_pct)
        if qty > 0 and not is_option:
            realized_usd = direction * (exit_px - entry) * qty
    # BALANCE TRACKING — equity closes return known proceeds (qty × exit).
    # Option proceeds can't be derived from underlying prices, so we stay
    # conservative: credit nothing and let the next fresh broker fetch true it
    # up (a too-low tracked balance can never cause an insufficient-funds trade).
    if not is_option and exit_px > 0 and qty > 0:
        try:
            await state.adjust_balance(exit_px * qty)
        except Exception as e:
            logger.warning(f"balance credit failed (non-fatal): {e}")
    await trade_ledger.record("position_closed", {
        "ticker": ticker,
        "entry": entry or None,
        "exit": exit_px or None,
        "realized_pnl_pct": round(pnl_pct, 4) if pnl_pct is not None else None,
        "realized_pnl_usd": round(realized_usd, 2) if realized_usd is not None else None,
        "qty": qty or None,
    })


def _positive_float(value: Any) -> Optional[float]:
    """Parse a broker-reported number; None unless strictly positive."""
    try:
        f = float(value)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


async def _fetch_broker_balance() -> Optional[Dict[str, Optional[float]]]:
    """ONE E*TRADE balance call → {'total': totalAccountValue, 'available':
    cash available for new orders}. Returns None on any failure — callers
    decide whether to fail closed or fall back to the tracked snapshot."""
    try:
        tokens = load_tokens()
        if not tokens:
            return None
        accounts = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens["oauth_token"], tokens["oauth_token_secret"],
            dev=is_sandbox,
        )
        lst = await _etrade_call(accounts.list_accounts, resp_format="json", source="equity_accounts")
        acct_list = (((lst or {}).get("AccountListResponse") or {}).get("Accounts") or {}).get("Account") or []
        if isinstance(acct_list, dict):
            acct_list = [acct_list]
        if not acct_list:
            return None
        acct = acct_list[0]
        bal = await _etrade_call(
            accounts.get_account_balance,
            acct["accountIdKey"],
            account_type=acct.get("accountType"),
            institution_type=acct.get("institutionType", "BROKERAGE"),
            resp_format="json",
            source="balance",
        )
        computed = ((bal or {}).get("BalanceResponse", {}) or {}).get("Computed", {}) or {}
        real_time = computed.get("RealTimeValues", {}) or {}
        total = _positive_float(real_time.get("totalAccountValue"))
        # Cash actually spendable on new orders — the number that prevents
        # 8400 insufficient-funds rejections. Order of preference matches
        # E*TRADE's own "purchasing power" semantics for cash accounts.
        available = (
            _positive_float(computed.get("cashBuyingPower"))
            or _positive_float(computed.get("cashAvailableForInvestment"))
            or _positive_float(computed.get("marginBuyingPower"))
            or total
        )
        return {"total": total, "available": available}
    except Exception as e:
        logger.error(f"balance fetch failed ({e})")
        return None


async def _live_equity() -> Optional[float]:
    """Fetch real account equity from E*TRADE. Returns None on any failure —
    live sizing must FAIL CLOSED (reject the trade) rather than silently size
    off a default."""
    bal = await _fetch_broker_balance()
    return (bal or {}).get("total")


async def _available_funds() -> Optional[float]:
    """Best estimate of funds available for a NEW entry. A fresh broker fetch
    is always preferred (and refreshes the tracked snapshot); during broker
    hiccups it falls back to the last snapshot ± the win/loss/cost delta
    accumulated since. None when neither source is usable."""
    bal = await _fetch_broker_balance()
    fresh = (bal or {}).get("available")
    if fresh is not None and fresh > 0:
        try:
            await state.set_balance_snapshot(fresh)
        except Exception as e:
            logger.warning(f"balance snapshot store failed (non-fatal): {e}")
        return fresh
    tracked = await state.tracked_balance()
    if tracked is not None:
        logger.warning(
            f"💰 broker balance fetch unavailable — sizing from tracked balance ≈${tracked:.2f} "
            f"(last snapshot ± wins/losses/open orders)"
        )
        return tracked
    return None


# ==================== EXPIRY HELPER ====================
def _resolve_expiry_string(payload: dict) -> Optional[str]:
    """Return a clean 'YYYY-MM-DD' expiry string for pyetrade.

    pyetrade parses expiryDate with dateutil, so a plain ISO date string is the
    most reliable input. Prefer the explicit hint; otherwise rebuild it from the
    discrete year/month/day fields the app also sends.
    """
    raw = payload.get("expiration_hint") or payload.get("expiry")
    if raw:
        return str(raw)[:10]
    y = payload.get("expiration_year")
    m = payload.get("expiration_month")
    d = payload.get("expiration_day")
    if y and m and d:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return None


# ==================== OPTION PRICE SANITY ====================
def _round_to_option_tick(price: float, direction: str = "nearest") -> float:
    """Round an option premium to E*TRADE's accepted increments.

    E*TRADE Error 2040 rejects limit prices that aren't in valid increments:
    $0.05 ticks below $3.00 and $0.10 ticks at/above $3.00. Rounding to these
    ticks is accepted for ALL contracts (penny-pilot names simply allow finer
    increments too), so it is always safe.
    """
    price = max(0.01, float(price))
    tick = 0.05 if price < 3.0 else 0.10
    ticks = price / tick
    if direction == "up":
        ticks = math.ceil(ticks - 1e-9)
    elif direction == "down":
        ticks = math.floor(ticks + 1e-9)
    else:
        ticks = round(ticks)
    return max(tick, round(ticks * tick, 2))


# ==================== CONTRACT SNAP ====================
def _snap_option_contract(market, symbol: str, expiry: str, strike: float, call_put: str):
    """Snap a requested option expiry/strike to REAL listed contract values.

    Returns (expiry, strike, bid, ask). bid/ask are the REAL quote of the
    snapped contract straight from E*TRADE's chain (0.0 when unavailable) —
    used to sanitize the app's estimated limit price, which is modeled from
    the stock price and can drift far from the true premium (Error 1011) or
    land on an invalid tick (Error 2040).

    E*TRADE rejects orders for contracts that don't exist with
    "Error 2009: The symbol you entered does not appear to be valid" — the
    classic trigger is a 0DTE expiry landing on a market holiday (e.g.
    2026-07-03, July 4th observed) or an unlisted strike. Ask the option chain
    what actually exists and snap to the nearest listed expiry >= today and
    the nearest listed strike. Best effort: any lookup failure returns the
    original values so an order is never blocked by this helper.
    """
    snapped_expiry = str(expiry)[:10]
    snapped_strike = float(strike)
    real_bid = 0.0
    real_ask = 0.0

    try:
        requested = datetime.strptime(snapped_expiry, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Contract snap skipped — unparseable expiry '{expiry}'")
        return snapped_expiry, snapped_strike, real_bid, real_ask
    today = datetime.utcnow().date()

    try:
        resp = _sync_etrade_call(market.get_option_expire_date, symbol, resp_format="json", source="option_expiry")
        raw_dates = (((resp or {}).get("OptionExpireDateResponse") or {}).get("ExpirationDate")) or []
        if isinstance(raw_dates, dict):
            raw_dates = [raw_dates]
        listed = []
        for entry in raw_dates:
            try:
                listed.append(date(int(entry["year"]), int(entry["month"]), int(entry["day"])))
            except (KeyError, TypeError, ValueError):
                continue
        valid = [x for x in listed if x >= today]
        if valid and requested not in valid:
            best = min(valid, key=lambda x: abs((x - requested).days))
            logger.warning(
                f"⚠️ Expiry {requested} is not a listed {symbol} expiration "
                f"(holiday/weekend?) — snapping to {best}"
            )
            requested = best
            snapped_expiry = best.strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"Expiry snap skipped for {symbol}: {e}")
        return snapped_expiry, snapped_strike, real_bid, real_ask

    try:
        chains = _sync_etrade_call(
            market.get_option_chains,
            symbol,
            expiry_date=requested,
            chain_type=("CALL" if str(call_put).upper() == "CALL" else "PUT"),
            strike_price_near=int(round(snapped_strike)),
            no_of_strikes=10,
            resp_format="json",
            source="option_chain",
        )
        pairs = (((chains or {}).get("OptionChainResponse") or {}).get("OptionPair")) or []
        if isinstance(pairs, dict):
            pairs = [pairs]
        strikes = []
        for pair in pairs:
            leg = pair.get("Call") or pair.get("Put") or {}
            sp = leg.get("strikePrice")
            try:
                if sp is not None:
                    strikes.append((float(sp), leg))
            except (TypeError, ValueError):
                continue
        if strikes:
            nearest, best_leg = min(strikes, key=lambda t: abs(t[0] - snapped_strike))
            if abs(nearest - snapped_strike) > 1e-9:
                logger.warning(
                    f"⚠️ Strike {snapped_strike} not listed for {symbol} {snapped_expiry} "
                    f"— snapping to {nearest}"
                )
            snapped_strike = nearest
            try:
                real_bid = max(0.0, float(best_leg.get("bid") or 0))
                real_ask = max(0.0, float(best_leg.get("ask") or 0))
                logger.info(
                    f"📊 Real quote for {symbol} {snapped_expiry} {snapped_strike} {call_put}: "
                    f"bid={real_bid} ask={real_ask}"
                )
            except (TypeError, ValueError):
                real_bid = real_ask = 0.0
    except Exception as e:
        logger.warning(f"Strike snap skipped for {symbol} {snapped_expiry}: {e}")

    return snapped_expiry, snapped_strike, real_bid, real_ask


# ==================== LIVE TRADING ====================
async def execute_live_order(payload: dict):
    mode = payload.get("mode", "paper").lower()
    instrument = payload.get("instrument", "stock").lower()
    ticker = payload.get("ticker", "UNKNOWN")
    action = payload.get("action", "UNKNOWN").upper()

    logger.info(f"📥 Received signal → mode={mode}, instrument={instrument}, ticker={ticker}, action={action}")

    if mode != "live" or not LIVE_TRADING or is_sandbox:
        logger.info(f"⏭️ Skipping trade (mode={mode}, LIVE_TRADING={LIVE_TRADING}, sandbox={is_sandbox})")
        # Verbose reason so the app's dispatch log shows the true gate that
        # blocked the live send (mode vs env config) instead of a bare status.
        return {
            "status": "skipped",
            "reason": f"mode={mode}, LIVE_TRADING={LIVE_TRADING}, sandbox={is_sandbox}",
        }

    await check_risk_limits()

    # A queued job may execute after conditions changed — re-check the hard
    # gates for ENTRIES here too (closes always pass: they reduce risk).
    is_close = _is_close_payload(payload)
    if not is_close:
        if await state.is_killed():
            raise Exception("kill switch active — entry refused")
        if not _is_market_open() and not bool(payload.get("force_execute")):
            raise Exception("market closed — entry refused (exchange-aware calendar)")

    tokens = load_tokens()
    if not tokens:
        logger.error("❌ No E*TRADE tokens available")
        raise Exception("E*TRADE tokens not set")

    # NEVER pass the raw env value straight through — E*TRADE needs the opaque
    # accountIdKey from /accounts/list (Error 102 otherwise).
    account_id_key = await _resolve_account_id_key(tokens)

    client_order_id = str(uuid.uuid4().int)[:18]

    # DISTRIBUTED CRITICAL SECTION — one worker at a time may place/close
    # orders for this ticker (prevents double-placement across instances).
    order_lock = state.lock(f"order:{str(ticker).upper()}", ttl_ms=120_000, wait_timeout=30.0)
    try:
        await order_lock.acquire()
    except LockNotAcquired:
        raise Exception(f"order lock busy for {ticker} — another worker is placing/closing this ticker")

    try:
        if instrument == "option":
            symbol = payload["ticker"]
            strike = payload.get("strike_hint") or payload.get("strike")
            expiry = _resolve_expiry_string(payload)
            call_put = str(payload.get("option_right") or payload.get("call_put") or "CALL").upper()
            call_put = "CALL" if call_put.startswith("C") else "PUT"
            # The app sends `option_contracts`; accept `contracts`/`quantity`
            # aliases so hand-rolled test payloads size correctly too.
            quantity = int(
                payload.get("option_contracts")
                or payload.get("contracts")
                or payload.get("quantity")
                or 1
            )
            order_action = "SELL_CLOSE" if (is_close or action != "BUY") else "BUY_OPEN"
            is_exit = order_action == "SELL_CLOSE"

            if not strike or not expiry:
                raise Exception(f"Missing strike or expiration for option order. Got strike={strike}, expiry={expiry}")

            # Snap to a contract that actually exists on E*TRADE's chain.
            # Without this, a 0DTE expiry on a market holiday (or an unlisted
            # strike) fails with Error 2009 "symbol not valid".
            market = pyetrade.ETradeMarket(
                CONSUMER_KEY, CONSUMER_SECRET,
                tokens["oauth_token"], tokens["oauth_token_secret"],
                dev=is_sandbox,
            )
            expiry, strike, real_bid, real_ask = await asyncio.to_thread(
                _snap_option_contract, market, symbol, expiry, float(strike), call_put
            )

            # A LIMIT with a real premium is the most reliable for options; fall
            # back to MARKET only if no premium was supplied. NOTE: the app's
            # option_limit_price is an ESTIMATE modeled from the stock price —
            # it must be sanitized against the real bid/ask captured above or
            # E*TRADE rejects it (1011: too far from market; 2040: bad tick).
            limit_price = payload.get("option_limit_price") or payload.get("limit_price")

            # FLAT kwargs — pyetrade builds Order/Instrument/Product itself and
            # auto-previews (no manual previewId needed).
            common = dict(
                resp_format="json",
                accountIdKey=account_id_key,
                symbol=symbol,
                orderAction=order_action,
                clientOrderId=client_order_id,
                quantity=quantity,
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
                allOrNone=False,
                callPut=call_put,
                strikePrice=float(strike),
                expiryDate=expiry,  # "YYYY-MM-DD" string
            )

            if is_exit:
                # CLOSE FLOW: cancel the broker-resting protective option stop
                # FIRST (same discipline as equity) so the close can never
                # double-sell against it.
                sym_u = str(symbol).upper()
                pos = await state.get_position(sym_u) or {}
                opt_guard = await state.get_guard(sym_u) or {}
                resting_stop_id = pos.get("stop_order_id") or opt_guard.get("stop_order_id")
                if resting_stop_id and not await _cancel_order_safe(resting_stop_id):
                    try:
                        stop_status, _sf = await _order_state(resting_stop_id, None)
                    except Exception as e:
                        stop_status = "UNKNOWN"
                        logger.warning(f"option stop status check failed for {sym_u}: {e}")
                    if stop_status == "EXECUTED":
                        await _finish_guard(sym_u, "closed_by_stop")
                        await _record_close(sym_u, None, payload)
                        logger.info(f"[LIVE option CLOSE] {sym_u} already closed by resting stop {resting_stop_id}")
                        return {
                            "status": "success",
                            "response": {"note": "resting protective stop already executed at broker", "stop_order_id": resting_stop_id},
                        }
                    if stop_status not in {"CANCELLED", "REJECTED", "EXPIRED", "NOT_FOUND"}:
                        raise Exception(
                            f"could not cancel resting option stop {resting_stop_id} — refusing to double-sell {sym_u}"
                        )
                await _finish_guard(sym_u, "closed_by_app")

                # EXIT (SELL_CLOSE): protective exits must FILL. Default to
                # MARKET. An explicit `exit_limit_price` overrides; a resting
                # broker-side STOP is placed only when the app explicitly asks
                # (`broker_stop: true` + a stop level). Trailing-stop levels
                # are otherwise tracked app-side by the Auto-Exit engine.
                exit_limit = payload.get("exit_limit_price")
                broker_stop_level = payload.get("stop_price") or payload.get("trail_stop") or payload.get("stop")
                if exit_limit and float(exit_limit) > 0:
                    desired = float(exit_limit)
                    # A sell limit far BELOW the market is rejected (1011);
                    # clamp a stale low estimate up to the real bid, and round
                    # DOWN to a valid tick so the order stays marketable.
                    if real_bid > 0 and desired < real_bid:
                        logger.warning(f"Exit limit {desired} below real bid {real_bid} — lifting to bid")
                        desired = real_bid
                    common["priceType"] = "LIMIT"
                    common["limitPrice"] = _round_to_option_tick(desired, "down")
                elif payload.get("broker_stop") and broker_stop_level and float(broker_stop_level) > 0:
                    common["priceType"] = "STOP"
                    common["stopPrice"] = _round_to_option_tick(float(broker_stop_level), "down")
                    logger.info(f"Placing resting broker-side STOP close at {common['stopPrice']}")
                else:
                    common["priceType"] = "MARKET"
            else:
                # ENTRY (BUY_OPEN): NEVER a STOP/TRAILING_STOP price type — that
                # turns the entry into a trigger order instead of a fill (the
                # v2.8.0 design flaw). Stop/trail fields on entries are stored
                # app-side and enforced by the Auto-Exit engine.
                desired = float(limit_price) if limit_price and float(limit_price) > 0 else None
                if real_ask > 0:
                    # Sanitize against the REAL quote. The app's estimate is
                    # frequently far off the true premium:
                    #  - above ask → E*TRADE 1011 (limit too far above market)
                    #    and would overpay anyway → cap at ask
                    #  - below bid → resting order that never fills → lift to ask
                    #  - inside the spread → keep it
                    # Then round UP to a valid $0.05/$0.10 tick (2040 guard);
                    # one tick above ask is a normal marketable limit.
                    if desired is None or desired > real_ask or desired < real_bid:
                        if desired is not None:
                            logger.warning(
                                f"⚠️ App limit {desired} outside real market "
                                f"[{real_bid}, {real_ask}] — repricing to ask"
                            )
                        desired = real_ask
                    common["priceType"] = "LIMIT"
                    common["limitPrice"] = _round_to_option_tick(desired, "up")
                elif desired is not None:
                    # No real quote available — at minimum fix the tick size.
                    common["priceType"] = "LIMIT"
                    common["limitPrice"] = _round_to_option_tick(desired)
                else:
                    common["priceType"] = "MARKET"
                if payload.get("stop") or payload.get("trail_stop") or payload.get("stop_price"):
                    logger.info(
                        f"Entry stop telemetry noted (app-managed): stop={payload.get('stop') or payload.get('stop_price')} "
                        f"trail_stop={payload.get('trail_stop')} trail_amount={payload.get('trail_amount') or payload.get('trailing_stop_amount')}"
                    )

            logger.info(f"📤 Placing OPTION: {json.dumps({k: v for k, v in common.items() if k != 'resp_format'})}")
            if is_exit:
                # Closes must always cover the full tracked position — never clamp.
                final = await _place_order_smart("option", common, tokens)
            else:
                # Per-contract cost = premium × 100 shares. Prefer the sanitized
                # limit price (set from the real ask above); fall back to the
                # raw ask for MARKET orders.
                premium_ref = float(common.get("limitPrice") or 0) or float(real_ask or 0)
                final, quantity = await _place_entry_with_funds_clamp(
                    "option", common, tokens,
                    unit_cost=premium_ref * 100.0 if premium_ref > 0 else None,
                )
            logger.info(f"✅ LIVE OPTION TRADE SUCCESS: {symbol} {call_put} {strike} {expiry}")
            await _record_api_success()
            if is_exit:
                await _record_close(str(symbol).upper(), None, payload)
            else:
                contract = {
                    "occ_symbol": _occ_symbol(symbol, expiry, call_put, float(strike)),
                    "right": call_put,
                    "strike": float(strike),
                    "expiration": expiry,
                }
                await _record_open(
                    str(symbol).upper(), quantity,
                    payload.get("entry"), payload.get("stop") or payload.get("stop_price"),
                    payload.get("target"), contract, action,
                )
                # OPTION STOP PROTECTION — async guard task (state in Redis,
                # survives restarts): poll the fill, then rest a SELL_CLOSE
                # STOP on the same OCC contract.
                entry_order_id = _order_id_from_place(final)
                stop_premium: Optional[float] = None
                explicit = payload.get("option_stop_price")
                try:
                    if explicit is not None and float(explicit) > 0:
                        stop_premium = float(explicit)
                except (TypeError, ValueError):
                    stop_premium = None
                fill_ref = float(common.get("limitPrice") or 0)
                if stop_premium is None and fill_ref > 0 and OPTION_STOP_LOSS_PCT > 0:
                    stop_premium = fill_ref * (1.0 - OPTION_STOP_LOSS_PCT / 100.0)
                if stop_premium and stop_premium > 0:
                    stop_premium = _round_to_option_tick(stop_premium, "down")
                    await _arm_stop_guard(
                        str(symbol).upper(), "BUY", stop_premium, entry_order_id, client_order_id,
                        kind="option", contract=contract,
                    )
                    pos_rec = await state.get_position(str(symbol).upper())
                    if pos_rec is not None:
                        pos_rec["stop_premium"] = stop_premium
                        await state.set_position(str(symbol).upper(), pos_rec)
                    logger.info(
                        f"[STOP GUARD] option guard armed for {symbol} at premium {stop_premium:.2f} "
                        f"(entry order={entry_order_id})"
                    )
                else:
                    logger.warning(f"⚠️ {symbol} option entry has no derivable stop premium — no broker-side stop armed")
            return {"status": "success", "response": final}

        else:
            # EQUITY ORDER
            symbol = str(ticker).upper()
            limit_price = payload.get("limit_price")
            tracked_pos = await state.get_position(symbol)
            is_equity_exit = is_close or action in {"EXIT", "CLOSE"} or (action != "BUY" and tracked_pos is not None)

            if is_equity_exit:
                # LIVE EQUITY CLOSE — cancel the broker-resting protective stop
                # FIRST so the close can never double-sell against it.
                pos = tracked_pos or {}
                guard = await state.get_guard(symbol) or {}
                stop_order_id = pos.get("stop_order_id") or guard.get("stop_order_id")
                already_closed_by_stop = False
                if stop_order_id and not await _cancel_order_safe(stop_order_id):
                    try:
                        stop_status, _stop_filled = await _order_state(stop_order_id, None)
                    except Exception as e:
                        stop_status = "UNKNOWN"
                        logger.warning(f"stop status check failed for {symbol}: {e}")
                    if stop_status == "EXECUTED":
                        already_closed_by_stop = True
                    elif stop_status not in {"CANCELLED", "REJECTED", "EXPIRED", "NOT_FOUND"}:
                        raise Exception(
                            f"could not cancel resting stop {stop_order_id} — refusing to double-sell {symbol}"
                        )
                await _finish_guard(symbol, "closed_by_app")
                if already_closed_by_stop:
                    await _record_close(symbol, None, payload)
                    logger.info(f"[LIVE equity CLOSE] {symbol} already closed by resting stop {stop_order_id}")
                    return {
                        "status": "success",
                        "response": {"note": "resting protective stop already executed at broker", "stop_order_id": stop_order_id},
                    }

                qty = int(pos.get("filled_qty") or pos.get("qty") or payload.get("position_size_shares") or 0)
                if qty < 1:
                    raise Exception(
                        f"close refused — unknown quantity for {symbol} (no tracked position and no position_size_shares)"
                    )
                entry_action = str(pos.get("action") or ("BUY" if action != "BUY" else "SELL")).upper()
                exit_side = "SELL" if entry_action == "BUY" else "BUY_TO_COVER"
                common = dict(
                    resp_format="json",
                    accountIdKey=account_id_key,
                    symbol=symbol,
                    orderAction=exit_side,
                    clientOrderId=client_order_id,
                    quantity=qty,
                    orderTerm="GOOD_FOR_DAY",
                    marketSession="REGULAR",
                    allOrNone=False,
                    priceType="MARKET",  # protective closes prioritize certainty of fill
                )
                logger.info(f"📤 Placing EQUITY CLOSE: {json.dumps({k: v for k, v in common.items() if k != 'resp_format'})}")
                final = await _place_order_smart("equity", common, tokens)
                await _record_close(symbol, None, payload)
                logger.info(f"✅ LIVE EQUITY CLOSE SUCCESS: {exit_side} {qty} {symbol}")
                await _record_api_success()
                return {"status": "success", "response": final}

            # LIVE EQUITY ENTRY — FAIL-CLOSED sizing: never default to 1 share.
            shares_raw = payload.get("position_size_shares")
            try:
                quantity = int(shares_raw) if shares_raw is not None else 0
            except (TypeError, ValueError):
                quantity = 0
            entry_px = float(payload.get("entry") or payload.get("limit_price") or 0)
            stop_px = float(payload.get("stop") or payload.get("stop_price") or 0)
            if quantity < 1:
                dist = abs(entry_px - stop_px)
                equity_val = await _live_equity()
                if equity_val and equity_val > 0 and dist > 0:
                    quantity = int((equity_val * (RISK_PER_TRADE_PCT / 100.0)) // dist)
                    logger.info(f"sized {symbol} from live equity {equity_val:.2f}: qty={quantity}")
                if quantity < 1:
                    raise Exception(
                        "fail-closed sizing: position_size_shares missing/invalid and cannot size "
                        "from live equity — refusing to default to 1 share"
                    )

            order_action = "BUY" if action == "BUY" else "SELL"
            common = dict(
                resp_format="json",
                accountIdKey=account_id_key,
                symbol=symbol,
                orderAction=order_action,
                clientOrderId=client_order_id,
                quantity=quantity,
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
                allOrNone=False,
            )
            if limit_price and float(limit_price) > 0:
                common["priceType"] = "LIMIT"
                common["limitPrice"] = round(float(limit_price), 2)
            else:
                common["priceType"] = "MARKET"

            # ATOMIC OTOCO BEST-EFFORT — one linked Entry + Protective Stop
            # request via the raw Order API. E*TRADE's public API doesn't
            # officially support OCO/OTOCO, so rejection falls back to the
            # proven sequential path (entry + stop guard) and is remembered.
            if (
                ENABLE_RAW_OTOCO and USE_RAW_ORDER_API and stop_px > 0
                and not await state.exists("otoco:unsupported")
            ):
                try:
                    raw = _raw_client(tokens)
                    entry_detail = raw.order_detail(
                        [raw.equity_instrument(symbol, order_action, quantity)],
                        str(common.get("priceType") or "MARKET"),
                        limit_price=common.get("limitPrice"),
                    )
                    otoco_exit_side = "SELL" if order_action == "BUY" else "BUY_TO_COVER"
                    stop_detail = raw.order_detail(
                        [raw.equity_instrument(symbol, otoco_exit_side, quantity)],
                        "STOP", stop_price=round(stop_px, 2),
                    )
                    final = await raw.place_otoco_best_effort(
                        account_id_key, client_order_id, "EQ", entry_detail, stop_detail,
                    )
                    logger.info(f"✅ ATOMIC OTOCO accepted for {symbol} — entry + protective stop in ONE request")
                    await _record_api_success()
                    await _record_open(symbol, quantity, entry_px or None, stop_px or None,
                                       payload.get("target"), None, action)
                    return {"status": "success", "response": final, "otoco": True}
                except OTOCOUnsupported as e:
                    logger.warning(
                        f"OTOCO not accepted by E*TRADE ({e.message}) — falling back to entry + stop guard "
                        f"(skipping OTOCO attempts for {OTOCO_UNSUPPORTED_TTL // 3600}h)"
                    )
                    await state.set("otoco:unsupported", "1", ex=OTOCO_UNSUPPORTED_TTL)
                except ETradeAPIError as e:
                    logger.warning(f"OTOCO attempt failed ({e}) — falling back to entry + stop guard")

            logger.info(f"📤 Placing EQUITY: {json.dumps({k: v for k, v in common.items() if k != 'resp_format'})}")
            # Per-share cost for the pre-flight funds clamp — the limit price
            # when set, otherwise the signal's entry estimate for MARKET orders.
            share_cost = float(common.get("limitPrice") or 0) or (entry_px if entry_px > 0 else 0)
            final, quantity = await _place_entry_with_funds_clamp(
                "equity", common, tokens,
                unit_cost=share_cost if share_cost > 0 else None,
            )
            logger.info(f"✅ LIVE EQUITY TRADE SUCCESS: {action} {quantity} {symbol}")
            await _record_api_success()

            await _record_open(symbol, quantity, entry_px or None, stop_px or None, payload.get("target"), None, action)
            entry_order_id = _order_id_from_place(final)
            if stop_px > 0:
                # Arm the stop guard: poll the entry fill, then rest a real STOP
                # at E*TRADE so the position stays protected even if the app
                # goes offline.
                await _arm_stop_guard(symbol, action, stop_px, entry_order_id, client_order_id)
                logger.info(f"[STOP GUARD] armed for {symbol} at {stop_px:.2f} (entry order={entry_order_id})")
            else:
                logger.warning(f"⚠️ {symbol} live entry has NO stop level — no broker-side protective stop armed")
            return {"status": "success", "response": final}

    except Exception as e:
        await _record_api_failure("order_placement", str(e))
        await trade_ledger.record("order_failed", {
            "ticker": ticker, "action": action, "instrument": instrument,
            "error": str(e)[:300],
        })
        await alerts.send(
            "error", "order_failed",
            f"{ticker} {action} ({instrument}) failed: {e}",
            dedupe_key=f"order_failed:{ticker}",
        )
        logger.error(f"❌ LIVE TRADE FAILED: {e}")
        raise
    finally:
        await order_lock.release()


# ==================== WORKER ====================
# E*TRADE access tokens go INACTIVE after ~2h of no API traffic and can then
# only be reactivated via renew_access_token (they hard-expire at midnight ET
# and require a full relink after that). Renewing every ~50 minutes keeps the
# session alive all trading day without any user action.
TOKEN_KEEPALIVE_SECONDS = 50 * 60


async def token_keepalive_worker():
    while not _worker_stop:
        await asyncio.sleep(TOKEN_KEEPALIVE_SECONDS)
        tokens = load_tokens()
        if not tokens:
            continue
        try:
            auth_manager = pyetrade.ETradeAccessManager(
                CONSUMER_KEY, CONSUMER_SECRET,
                tokens["oauth_token"], tokens["oauth_token_secret"],
            )
            await _etrade_call(auth_manager.renew_access_token, source="keepalive_renew")
            logger.info("🔄 Keepalive: E*TRADE access token renewed")
        except Exception as e:
            # Renewal fails after the midnight-ET hard expiry — that requires a
            # full relink from the app, so just log it (orders will 401 and the
            # app surfaces the relink prompt).
            logger.warning(f"Keepalive renewal failed (relink may be required): {e}")
            await alerts.send(
                "warning", "token_keepalive_failed",
                f"E*TRADE access token renewal failed — a relink may be required: {e}",
                dedupe_key="keepalive",
            )


async def placement_worker():
    while not _worker_stop:
        try:
            if state.is_distributed:
                job = await state.queue_pop(QUEUE_KEY)
                if job:
                    await execute_live_order(json.loads(job)["payload"])
                else:
                    await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(2)


# ==================== RECONCILIATION ENGINE WIRING ====================
# Dependency-injected so reconciliation.py never imports main_bot. The engine
# runs under the distributed `lock:reconcile` — one instance at a time.
def _reconcile_has_tokens() -> bool:
    return load_tokens() is not None


async def _reconcile_fetch_portfolio() -> Dict[str, Any]:
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE not linked")
    acct_key = await _resolve_account_id_key(tokens)
    return await _raw_client(tokens).get_portfolio(acct_key)


async def _reconcile_fetch_orders() -> Dict[str, Any]:
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE not linked")
    acct_key = await _resolve_account_id_key(tokens)
    return await _raw_client(tokens).list_orders(acct_key)


async def _reconcile_cancel(order_id: str) -> bool:
    return await _cancel_order_safe(order_id)


async def _reconcile_rearm_guard(ticker: str, pos: dict) -> None:
    """Re-arm a protective stop for a position the reconciler found
    UNPROTECTED (filled at broker, no live stop, no active guard)."""
    qty = int(pos.get("filled_qty") or pos.get("qty") or 0)
    if qty < 1:
        return
    contract = pos.get("contract")
    if contract and contract.get("right"):
        premium = float(pos.get("stop_premium") or 0)
        if premium <= 0:
            logger.warning(f"[RECONCILE] cannot re-arm option stop for {ticker} — no stop premium recorded")
            return
        stop_info = await _place_option_protective_stop(ticker, dict(contract), qty, premium)
    else:
        stop = float(pos.get("stop") or 0)
        if stop <= 0:
            return
        stop_info = await _place_protective_stop(ticker, str(pos.get("action") or "BUY"), qty, stop)
    pos["stop_order_id"] = stop_info["order_id"]
    pos["filled_qty"] = qty
    await state.set_position(ticker, pos)
    logger.warning(f"[RECONCILE] 🛡️ re-armed protective stop for {ticker} (order={stop_info['order_id']})")


def _start_reconciler() -> None:
    asyncio.create_task(reconciliation.reconciliation_loop(
        state,
        _reconcile_fetch_portfolio,
        _reconcile_fetch_orders,
        _reconcile_cancel,
        _is_market_open,
        _reconcile_has_tokens,
        rearm_guard=_reconcile_rearm_guard,
        interval_seconds=RECONCILE_INTERVAL_SECONDS,
        offhours_interval_seconds=RECONCILE_OFFHOURS_SECONDS,
        auto_heal=RECONCILE_AUTO_HEAL,
        stop_flag=lambda: _worker_stop,
        alert=alerts.send,
    ))


async def _startup_reconcile() -> None:
    """BLOCKING broker reconciliation at boot — sync Redis truth with E*TRADE
    BEFORE the placement worker accepts any job. Closes the biggest cold-start
    gap: acting on stale positions/guards after a restart. Best-effort: a
    broker failure or timeout never blocks boot (the background engine
    retries), but it is alerted."""
    if not load_tokens():
        logger.info("[RECONCILE] startup pass skipped — E*TRADE not linked yet")
        return
    lock = state.lock(reconciliation.RECONCILE_LOCK, ttl_ms=STARTUP_RECONCILE_TIMEOUT_SECONDS * 2000)
    if not await lock.try_acquire():
        logger.info("[RECONCILE] startup pass skipped — another worker is reconciling")
        return
    try:
        logger.info("[RECONCILE] 🚀 startup pass — syncing state with broker before accepting orders")
        report = await asyncio.wait_for(
            reconciliation.reconcile_once(
                state, _reconcile_fetch_portfolio, _reconcile_fetch_orders,
                _reconcile_cancel, rearm_guard=_reconcile_rearm_guard,
                auto_heal=RECONCILE_AUTO_HEAL, alert=alerts.send,
            ),
            timeout=STARTUP_RECONCILE_TIMEOUT_SECONDS,
        )
        summary = {
            "ok": report.get("ok"),
            "tracked_positions": report.get("tracked_positions"),
            "broker_positions": report.get("broker_positions"),
            "live_orders": report.get("live_orders"),
            "healed": report.get("healed", []),
            "warnings": report.get("warnings", []),
        }
        logger.info(
            f"[RECONCILE] ✅ startup pass done — tracked={summary['tracked_positions']} "
            f"broker={summary['broker_positions']} orders={summary['live_orders']} "
            f"healed={len(summary['healed'])} warnings={len(summary['warnings'])}"
        )
        await trade_ledger.record("startup_reconcile", summary)
        if summary["warnings"]:
            await alerts.send(
                "warning", "startup_reconcile_warnings",
                "; ".join(str(w) for w in summary["warnings"][:5]),
                dedupe_key="startup_reconcile",
            )
    except asyncio.TimeoutError:
        logger.error(f"[RECONCILE] startup pass timed out ({STARTUP_RECONCILE_TIMEOUT_SECONDS}s) — background engine will retry")
        await alerts.send(
            "error", "startup_reconcile_timeout",
            f"Startup reconciliation timed out after {STARTUP_RECONCILE_TIMEOUT_SECONDS}s — background engine will retry",
            dedupe_key="startup_reconcile",
        )
    except Exception as e:
        logger.error(f"[RECONCILE] startup pass failed: {e}")
        await alerts.send(
            "error", "startup_reconcile_failed",
            f"Startup reconciliation failed: {e} — background engine will retry",
            dedupe_key="startup_reconcile",
        )
    finally:
        await lock.release()


async def start_worker():
    global _worker_task
    _worker_task = asyncio.create_task(placement_worker())
    asyncio.create_task(token_keepalive_worker())


# ==================== STARTUP ====================
@app.on_event("startup")
async def on_startup():
    global state
    logger.info(f"Starting → {'SANDBOX' if is_sandbox else 'PRODUCTION'} | LIVE={LIVE_TRADING} | VERSION={BOT_VERSION}")
    # Distributed state first — everything else reads through it.
    state = await StateStore.create(REDIS_URL)
    # Alerting + immutable ledger come up right after state — everything
    # downstream (breaker, guards, reconciler) reports through them.
    alerts.init(state, ALERT_WEBHOOK_URL, dedupe_seconds=ALERT_DEDUPE_SECONDS)
    trade_ledger.init(TRADE_LEDGER_FILE)
    await init_db()
    await preload_tokens()
    # One-time migration of the legacy JSON STATE_FILE into Redis (skipped when
    # Redis already holds state; the file is renamed *.migrated afterwards).
    await state.migrate_from_file(STATE_FILE)
    # STARTUP RECONCILIATION — blocking broker→Redis sync BEFORE the placement
    # worker starts, so no order is ever placed against stale cold-start state.
    await _startup_reconcile()
    # Resume stop-guards interrupted by the restart (guard lock makes this
    # multi-worker safe) and start the background reconciliation engine.
    await _resume_guards()
    _start_reconciler()
    await start_worker()
    logger.info(f"✅ Bot ready (state backend: {state.backend_name})")


@app.on_event("shutdown")
async def on_shutdown():
    global _worker_stop
    _worker_stop = True
    await state.close()


# ==================== ENDPOINTS ====================
@app.post("/webhook")
async def webhook(
    request: Request,
    x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret"),
    x_signature: Optional[str] = Header(None, alias="X-Rork-Signature"),
):
    # Raw body first — the HMAC signature (X-Rork-Signature) is computed over
    # the exact bytes the app sent. Shared secret (body field or X-Rork-Secret
    # header) is accepted as before.
    raw = await request.body()
    try:
        data = json.loads(raw or b"{}")
    except json.JSONDecodeError:
        raise HTTPException(400, "invalid json")
    if not isinstance(data, dict):
        raise HTTPException(400, "invalid payload")
    _verify_webhook_auth(raw, data.get("secret"), x_rork_secret, x_signature)
    try:
        payload = WebhookPayload(**data)
    except ValidationError as e:
        raise HTTPException(400, f"invalid payload: {e.errors()[:3]}")

    pd = payload.dict()
    pd.pop("secret", None)  # never persist/queue the shared secret
    sig_key = _signal_key(pd)
    is_close = _is_close_payload(pd)
    mode = str(pd.get("mode") or "paper").lower()
    live_intent = mode == "live" and LIVE_TRADING and not is_sandbox

    # Atomic idempotency in Redis (SET NX + TTL) — a network retry or a second
    # worker can never double-place an order.
    if not await state.set(sig_key, "processing", ex=86400, nx=True):
        return {"status": "duplicate", "existing_status": await state.get(sig_key), "signal_id": sig_key}

    # Server-side gates for LIVE ENTRIES. Closes always pass — a protective
    # exit must never be blocked by entry gating.
    if live_intent and not is_close:
        if not _is_market_open() and not bool(pd.get("force_execute")):
            await state.set(sig_key, "rejected", ex=86400)
            return {"status": "rejected", "reason": "market_closed", "signal_id": sig_key}
        ok, blocked = await _passes_entry_filters(pd)
        if not ok:
            await state.set(sig_key, "rejected", ex=86400)
            await trade_ledger.record("entry_rejected", {
                "ticker": str(pd.get("ticker") or "").upper(),
                "action": pd.get("action"),
                "blocked_by": blocked,
            })
            return {"status": "rejected", "reason": "; ".join(blocked), "signal_id": sig_key}
        cooldown_key = f"cooldown:{str(pd.get('ticker') or '').upper()}"
        # NX write doubles as the existence check — atomic even across workers.
        if not await state.set(cooldown_key, "1", ex=TICKER_COOLDOWN_MINUTES * 60, nx=True):
            await state.set(sig_key, "cooldown", ex=86400)
            return {"status": "cooldown", "reason": "ticker_in_cooldown", "signal_id": sig_key}

    job = {"payload": pd}
    if state.is_distributed:
        try:
            await state.queue_push(QUEUE_KEY, json.dumps(job))
            await state.set(sig_key, "queued", ex=86400)
            return {"status": "queued", "signal_id": sig_key}
        except Exception as e:
            logger.warning(f"Redis push failed, processing directly: {e}")
    try:
        result = await execute_live_order(pd)
        await state.set(sig_key, str(result.get("status") or "processed"), ex=86400)
        return {"status": "processed_directly", "result": result, "signal_id": sig_key}
    except Exception as e:
        await state.set(sig_key, "failed", ex=86400)
        logger.error(f"Direct processing failed: {e}")
        return {"status": "error", "message": str(e), "signal_id": sig_key}


@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens),
        "version": BOT_VERSION,
        "target_account_set": bool(TARGET_ACCOUNT_ID),
        "resolved_account_key": bool(_resolved_account_id_key),
        "circuit_breaker_open": await _breaker_is_open(),
        "killed": await state.is_killed(),
        "state_backend": state.backend_name,
        "alert_webhook_configured": alerts.webhook_configured(),
        "reconcile_interval_seconds": RECONCILE_INTERVAL_SECONDS,
    }


@app.get("/healthz")
async def healthz():
    """Lightweight liveness probe polled by the app's System Monitor."""
    return {"ok": True, "ts": _utcnow().isoformat(), "version": BOT_VERSION}


@app.get("/status")
async def status():
    """Broker-state snapshot polled by the app's Reconciliation engine. The
    shape matches etrade_bot_handler's /status (open_positions keyed by ticker
    with qty/entry/stop/target/ts/contract), now read from reconciled Redis
    state and enriched with the last reconciliation report."""
    daily = await state.get_daily()
    last_reconcile = None
    raw_report = await state.get("reconcile:last")
    if raw_report:
        try:
            last_reconcile = json.loads(raw_report)
        except json.JSONDecodeError:
            last_reconcile = None
    return {
        "killed": await state.is_killed(),
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "version": BOT_VERSION,
        "market_open": _is_market_open(),
        "open_positions": await state.all_positions(),
        "stop_guards": await state.all_guards(),
        "trades_today": daily["trades_today"],
        "realized_pnl_today_pct": daily["realized_pnl_today_pct"],
        "circuit_breaker_open": await _breaker_is_open(),
        "state_backend": state.backend_name,
        "last_reconcile": last_reconcile,
        "reconcile_interval_seconds": RECONCILE_INTERVAL_SECONDS,
        "alert_webhook_configured": alerts.webhook_configured(),
        "filters": {
            "min_score": MIN_SCORE,
            "min_score_trending": MIN_SCORE_TRENDING,
            "min_rvol": MIN_RVOL,
            "min_mtf": MIN_MTF,
            "allowed_setups": sorted(ALLOWED_SETUPS),
        },
    }


@app.get("/alerts")
async def recent_alerts(count: int = Query(50, ge=1, le=200)):
    """Recent real-time alerts (newest first) — kill switch, failed guards,
    unprotected positions, circuit breaker trips, API problems."""
    return {
        "alerts": await alerts.recent(count),
        "webhook_configured": alerts.webhook_configured(),
    }


@app.get("/ledger")
async def ledger_tail(count: int = Query(100, ge=1, le=500), verify: bool = Query(False)):
    """Tail of the immutable trade ledger (newest first). Pass ?verify=true to
    walk the FULL hash chain and prove no record was altered or removed."""
    out: Dict[str, Any] = {"records": trade_ledger.tail(count)}
    if verify:
        ok, checked, err = trade_ledger.verify()
        out["chain_ok"] = ok
        out["chain_records_checked"] = checked
        out["chain_error"] = err
    return out


@app.post("/kill")
async def kill(x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    await state.set_killed(True)
    logger.warning("KILL SWITCH activated (distributed — all workers respect it)")
    open_tickers = await state.position_tickers()
    await trade_ledger.record("kill_switch_engaged", {"open_positions": open_tickers})
    await alerts.send(
        "critical", "kill_switch_engaged",
        f"KILL SWITCH engaged — all entries halted. Open positions: {', '.join(open_tickers) or 'none'}",
        dedupe_key="kill_switch",
    )
    return {"status": "killed", "open_positions": open_tickers}


@app.post("/resume")
async def resume(x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    await state.set_killed(False)
    logger.info("Kill switch released — trading resumed")
    await trade_ledger.record("kill_switch_released", {})
    await alerts.send("info", "kill_switch_released", "Kill switch released — trading resumed",
                      dedupe_key="kill_switch_release")
    return {"status": "resumed"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
