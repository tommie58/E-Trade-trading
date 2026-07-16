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

Run:
    pip install -r requirements.txt
    cp .env.example .env   # fill in keys + WEBHOOK_SECRET
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
import logging
import uuid
import asyncio
import hashlib
import hmac
import time
import threading
from pathlib import Path
from datetime import datetime, date, time as dtime, timezone
from zoneinfo import ZoneInfo
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from requests_oauthlib import OAuth1Session
from urllib.parse import quote
from dotenv import load_dotenv

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
BOT_VERSION = "4.0.0-parity"

# ---- Safety / parity config (mirrors etrade_bot_handler.py) ----
MIN_SCORE = int(os.getenv("MIN_SCORE", "90"))
MIN_SCORE_TRENDING = int(os.getenv("MIN_SCORE_TRENDING", "85"))
MIN_RVOL = float(os.getenv("MIN_RVOL", "1.5"))
MIN_MTF = int(os.getenv("MIN_MTF", "3"))
ALLOWED_SETUPS = {
    s.strip().lower()
    for s in os.getenv(
        "ALLOWED_SETUPS",
        "ema cross + adx,bull flag + adx,bear flag + adx,volume breakout",
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
# Daily state (positions, counters, kill switch, guards) survives restarts here.
STATE_FILE = Path(os.getenv("ETRADE_STATE_FILE", ".etrade_state.json"))


def _utcnow() -> datetime:
    """Timezone-aware UTC now (datetime.utcnow() is deprecated in 3.12+)."""
    return datetime.now(timezone.utc)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Trading Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== GLOBALS ====================
redis = None
engine = None
async_session = None
circuit_breaker_open = False
circuit_breaker_opened_at: Optional[datetime] = None
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5
# Auto-reset the breaker after a cooldown so one bad burst (e.g. stale prices
# before this fix) doesn't silently block trading for the rest of the day.
CIRCUIT_BREAKER_COOLDOWN_SECONDS = 10 * 60
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

# ==================== SAFETY STATE (parity with etrade_bot_handler) ====================
# All of this state is touched only from the event loop (webhook handlers,
# placement worker, stop-guard tasks), so plain dicts are safe; _state_lock
# additionally guards the file write against overlapping saves.
_state_lock = threading.Lock()
_open_positions: Dict[str, dict] = {}
_stop_guards: Dict[str, dict] = {}
_trades_today = 0
_realized_pnl_today_pct = 0.0
_today = _utcnow().date().isoformat()
_killed = False


def _save_state() -> None:
    """Persist daily counters, open positions, kill switch and stop-guard state
    so a mid-day restart cannot forget risk limits or unprotected positions."""
    try:
        with _state_lock:
            STATE_FILE.write_text(json.dumps({
                "date": _today,
                "trades_today": _trades_today,
                "realized_pnl_today_pct": _realized_pnl_today_pct,
                "killed": _killed,
                "open_positions": _open_positions,
                "stop_guards": _stop_guards,
            }, default=str))
    except (OSError, TypeError, ValueError) as e:
        logger.error(f"state persist failed: {e}")


def _load_state() -> None:
    """Restore persisted state on boot. Daily counters only restore when the
    saved date is still today; positions, guards and the kill switch always
    restore (broker reconciliation remains the source of truth)."""
    global _today, _trades_today, _realized_pnl_today_pct, _killed, _open_positions, _stop_guards
    if not STATE_FILE.exists():
        return
    try:
        data = json.loads(STATE_FILE.read_text())
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"state load failed: {e}")
        return
    _killed = bool(data.get("killed", False))
    _open_positions = dict(data.get("open_positions") or {})
    _stop_guards = dict(data.get("stop_guards") or {})
    saved_date = str(data.get("date") or "")
    today = _utcnow().date().isoformat()
    _today = today
    if saved_date == today:
        _trades_today = int(data.get("trades_today") or 0)
        _realized_pnl_today_pct = float(data.get("realized_pnl_today_pct") or 0.0)
    logger.info(
        f"state restored: {len(_open_positions)} open positions, {len(_stop_guards)} stop guards, "
        f"trades_today={_trades_today}, pnl={_realized_pnl_today_pct:.2f}%, killed={_killed}"
    )


def _reset_daily() -> None:
    global _today, _trades_today, _realized_pnl_today_pct
    today = _utcnow().date().isoformat()
    if today != _today:
        _today = today
        _trades_today = 0
        _realized_pnl_today_pct = 0.0
        _save_state()
        logger.info(f"daily counters reset for {today}")


class _TTL:
    """In-memory TTL store for idempotency keys and ticker cooldowns."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Tuple[Any, float]] = {}

    def _purge(self) -> None:
        now = time.time()
        for k in [k for k, (_, exp) in self._data.items() if exp and exp < now]:
            self._data.pop(k, None)

    def set(self, k: str, v: Any, ex: Optional[int] = None, nx: bool = False) -> bool:
        with self._lock:
            self._purge()
            if nx and k in self._data:
                return False
            self._data[k] = (v, (time.time() + ex) if ex else 0.0)
            return True

    def get(self, k: str) -> Optional[Any]:
        with self._lock:
            self._purge()
            return self._data.get(k, (None, 0))[0]

    def exists(self, k: str) -> bool:
        with self._lock:
            self._purge()
            return k in self._data


store = _TTL()


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
def _passes_entry_filters(p: dict) -> Tuple[bool, List[str]]:
    """Server-side re-filter mirroring the Rork app's gating. Quality checks
    apply only when the payload carries the field (score/rvol/mtf/setup);
    risk limits (kill switch, daily loss, trade count, positions, heat,
    duplicate ticker) always apply."""
    blocked: List[str] = []
    if _killed:
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

    _reset_daily()
    if _realized_pnl_today_pct <= -abs(DAILY_LOSS_LIMIT_PCT):
        blocked.append(f"daily loss limit ({_realized_pnl_today_pct:.2f}%)")
    if _trades_today >= DAILY_TRADE_LIMIT:
        blocked.append(f"daily trade limit ({DAILY_TRADE_LIMIT}) reached")
    if len(_open_positions) >= MAX_CONCURRENT_POSITIONS:
        blocked.append(f"max positions ({MAX_CONCURRENT_POSITIONS}) open")
    ticker = str(p.get("ticker") or "").upper()
    if ticker and ticker in _open_positions:
        blocked.append(f"already in {ticker}")

    try:
        account_ref = float(os.getenv("ACCOUNT_SIZE", "50000"))
        open_risk = sum(
            abs(float(pos.get("entry") or 0) - float(pos.get("stop") or 0)) * float(pos.get("qty") or 0)
            for pos in _open_positions.values()
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
    global circuit_breaker_open, circuit_breaker_opened_at, consecutive_failures
    logger.info("=== NEW TOKENS RECEIVED ===")
    _current_tokens = {"oauth_token": token, "oauth_token_secret": token_secret}
    _resolved_account_id_key = None  # re-resolve accountIdKey for the new session
    # A fresh link is an explicit user action — clear any tripped breaker so
    # the relinked session starts clean instead of rejecting with 503.
    if circuit_breaker_open:
        logger.info("🔓 Circuit breaker reset by account relink")
    circuit_breaker_open = False
    circuit_breaker_opened_at = None
    consecutive_failures = 0
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
        fetch_response = etrade_session.fetch_request_token(REQUEST_TOKEN_URL)
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
        resp = await asyncio.to_thread(accounts_api.list_accounts, resp_format="json")
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
    return {"status": "linked", "linked": True, "accounts": accounts_out}


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
        renewed = await asyncio.to_thread(auth_manager.renew_access_token)
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
    for attempt in range(1, 5):
        try:
            return await asyncio.to_thread(market.get_quote, symbol_list, resp_format="json")
        except Exception as e:
            if attempt < 4 and ("401" in str(e) or "Unauthorized" in str(e)):
                await asyncio.sleep(3)
                continue
            raise HTTPException(500, detail=str(e))


# ==================== DATABASE ====================
async def init_db():
    global engine, async_session
    use_postgres = False
    if DATABASE_URL and "postgres" in DATABASE_URL:
        try:
            import asyncpg  # noqa: F401
            use_postgres = True
        except ImportError:
            logger.warning("asyncpg not found — falling back to SQLite")
            use_postgres = False
    target_url = DATABASE_URL if use_postgres else "sqlite+aiosqlite:///etrade_cache.db"
    try:
        engine = create_async_engine(target_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Database connected")
    except Exception as e:
        logger.error(f"Database error: {e}")


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
    resp = await asyncio.to_thread(accounts_api.list_accounts, resp_format="json")
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


# ==================== SAFETY ====================
async def check_risk_limits():
    global circuit_breaker_open, circuit_breaker_opened_at, consecutive_failures
    if circuit_breaker_open:
        elapsed = (datetime.utcnow() - circuit_breaker_opened_at).total_seconds() if circuit_breaker_opened_at else 1e9
        if elapsed >= CIRCUIT_BREAKER_COOLDOWN_SECONDS:
            logger.info("🔓 Circuit breaker cooldown elapsed — resetting and resuming")
            circuit_breaker_open = False
            circuit_breaker_opened_at = None
            consecutive_failures = 0
        else:
            remaining = int(CIRCUIT_BREAKER_COOLDOWN_SECONDS - elapsed)
            raise HTTPException(503, f"Circuit breaker open — auto-resets in {remaining}s (or relink the account)")


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


async def _order_state(order_id: Optional[str], client_id: Optional[str]) -> Tuple[str, int]:
    """Return (status, total filled quantity) for an order, matched by orderId
    or clientOrderId in the account's recent orders. ('NOT_FOUND', 0) when the
    order is not in the list."""
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")
    acct_key = await _resolve_account_id_key(tokens)
    orders = _orders_client(tokens)
    resp = await asyncio.to_thread(orders.list_orders, acct_key, resp_format="json")
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
        await asyncio.to_thread(orders.cancel_order, acct_key, int(order_id), resp_format="json")
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
    orders = _orders_client(tokens)
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
    placed = await asyncio.to_thread(orders.place_equity_order, **common)
    order_id = _order_id_from_place(placed)
    logger.info(
        f"[STOP GUARD] protective stop RESTING at broker: {exit_side} {ticker} "
        f"qty={qty} stop={stop_price:.2f} (order={order_id})"
    )
    return {"order_id": order_id, "client_id": client_id, "qty": int(qty), "stop": round(float(stop_price), 2)}


def _finish_guard(ticker: str, result: str) -> None:
    g = _stop_guards.get(ticker)
    if g:
        g["done"] = True
        g["result"] = result
        _save_state()
    logger.info(f"[STOP GUARD] {ticker} finished: {result}")


async def _stop_guard_worker(ticker: str) -> None:
    """Poll the entry order; once (partially) filled, rest a protective STOP at
    the broker sized to the filled quantity. Cancel entries with zero fill at
    the deadline. Retries stop placement on every poll until protected."""
    logger.info(f"[STOP GUARD] watching {ticker} entry fill")
    while True:
        guard = _stop_guards.get(ticker)
        if not guard or guard.get("done"):
            return

        status = str(guard.get("last_status") or "OPEN")
        filled = int(guard.get("last_filled") or 0)
        try:
            status, filled = await _order_state(guard.get("entry_order_id"), guard.get("entry_client_id"))
        except Exception as e:
            logger.warning(f"[STOP GUARD] {ticker} poll failed: {e}")

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
                    stop_info = await _place_protective_stop(
                        ticker, str(guard.get("action") or "BUY"), filled, float(guard.get("stop") or 0),
                    )
                    g = _stop_guards.get(ticker)
                    if g:
                        g["guarded_qty"] = filled
                        g["stop_order_id"] = stop_info["order_id"]
                        g["stop_client_id"] = stop_info["client_id"]
                    pos = _open_positions.get(ticker)
                    if pos:
                        pos["stop_order_id"] = stop_info["order_id"]
                        pos["filled_qty"] = filled
                    _save_state()
                    guarded = filled
                except Exception as e:
                    logger.error(f"[STOP GUARD] {ticker} stop placement FAILED (will retry): {e}")

        g = _stop_guards.get(ticker)
        if g:
            g["last_filled"] = filled
            g["last_status"] = status
            _save_state()

        if status == "EXECUTED" and filled > 0 and guarded >= filled:
            _finish_guard(ticker, "filled_and_protected")
            return
        if status in _TERMINAL_ORDER_STATUSES and status != "EXECUTED" and filled == 0:
            _finish_guard(ticker, f"entry_{status.lower()}_unfilled")
            _open_positions.pop(ticker, None)
            _save_state()
            return
        if time.time() >= float(guard.get("deadline_ts") or 0):
            if filled == 0:
                await _cancel_order_safe(guard.get("entry_order_id"))
                _finish_guard(ticker, "entry_timeout_cancelled")
                _open_positions.pop(ticker, None)
                _save_state()
                return
            if guarded >= filled:
                _finish_guard(ticker, "partial_fill_protected")
                return
            # Filled but stop never stuck — keep trying rather than walk away.
            logger.error(f"[STOP GUARD] {ticker} UNPROTECTED at deadline — extending guard")
            g = _stop_guards.get(ticker)
            if g:
                g["deadline_ts"] = time.time() + ENTRY_FILL_TIMEOUT_MIN * 60
                _save_state()
        await asyncio.sleep(STOP_GUARD_POLL_SECONDS)


def _spawn_guard(ticker: str) -> None:
    asyncio.create_task(_stop_guard_worker(ticker))


def _arm_stop_guard(ticker: str, action: str, stop_price: float, entry_order_id: Optional[str], entry_client_id: str) -> None:
    _stop_guards[ticker] = {
        "ticker": ticker,
        "action": action,
        "stop": round(float(stop_price), 2),
        "entry_order_id": entry_order_id,
        "entry_client_id": entry_client_id,
        "guarded_qty": 0,
        "last_filled": 0,
        "last_status": "OPEN",
        "stop_order_id": None,
        "stop_client_id": None,
        "deadline_ts": time.time() + ENTRY_FILL_TIMEOUT_MIN * 60,
        "done": False,
        "result": None,
    }
    _save_state()
    _spawn_guard(ticker)


def _resume_guards() -> None:
    """Respawn watcher tasks for guards interrupted by a restart."""
    pending = [t for t, g in _stop_guards.items() if not g.get("done")]
    for t in pending:
        logger.info(f"[STOP GUARD] resuming guard for {t} after restart")
        _spawn_guard(t)


# ==================== POSITION LEDGER ====================
def _record_open(ticker: str, qty: int, entry: Optional[float], stop: Optional[float],
                 target: Optional[float], contract: Optional[dict]) -> None:
    global _trades_today
    _reset_daily()
    _open_positions[ticker] = {
        "qty": int(qty),
        "entry": float(entry) if entry else None,
        "stop": float(stop) if stop else None,
        "target": float(target) if target else None,
        "ts": _utcnow().isoformat(),
        "contract": contract,
    }
    _trades_today += 1
    _save_state()


def _record_close(ticker: str, exit_price: Optional[float], payload: dict) -> None:
    """Pop the position and feed realized pnl (underlying move, signed by
    direction) into the daily loss-limit accounting."""
    global _realized_pnl_today_pct
    pos = _open_positions.pop(ticker, None)
    _reset_daily()
    entry = float((pos or {}).get("entry") or payload.get("entry") or 0)
    exit_px = float(exit_price or payload.get("exit_price") or payload.get("limit_price") or entry or 0)
    direction = 1.0 if str(payload.get("action") or "BUY").upper() == "BUY" else -1.0
    if entry > 0 and exit_px > 0:
        _realized_pnl_today_pct += direction * ((exit_px - entry) / entry * 100.0)
    _save_state()


async def _live_equity() -> Optional[float]:
    """Fetch real account equity from E*TRADE. Returns None on any failure —
    live sizing must FAIL CLOSED (reject the trade) rather than silently size
    off a default."""
    try:
        tokens = load_tokens()
        if not tokens:
            return None
        accounts = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens["oauth_token"], tokens["oauth_token_secret"],
            dev=is_sandbox,
        )
        lst = await asyncio.to_thread(accounts.list_accounts, resp_format="json")
        acct_list = (((lst or {}).get("AccountListResponse") or {}).get("Accounts") or {}).get("Account") or []
        if isinstance(acct_list, dict):
            acct_list = [acct_list]
        if not acct_list:
            return None
        acct = acct_list[0]
        bal = await asyncio.to_thread(
            accounts.get_account_balance,
            acct["accountIdKey"],
            account_type=acct.get("accountType"),
            institution_type=acct.get("institutionType", "BROKERAGE"),
            resp_format="json",
        )
        val = (
            (bal or {}).get("BalanceResponse", {})
            .get("Computed", {})
            .get("RealTimeValues", {})
            .get("totalAccountValue")
        )
        return float(val) if val else None
    except Exception as e:
        logger.error(f"equity fetch failed ({e}) — live sizing will fail closed")
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
        resp = market.get_option_expire_date(symbol, resp_format="json")
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
        chains = market.get_option_chains(
            symbol,
            expiry_date=requested,
            chain_type=("CALL" if str(call_put).upper() == "CALL" else "PUT"),
            strike_price_near=int(round(snapped_strike)),
            no_of_strikes=10,
            resp_format="json",
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
    global consecutive_failures, circuit_breaker_open

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
        if _killed:
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

    orders = pyetrade.ETradeOrder(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens["oauth_token"], tokens["oauth_token_secret"],
        dev=is_sandbox,
    )

    client_order_id = str(uuid.uuid4().int)[:18]

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

            logger.info(f"📤 Placing OPTION (flat kwargs): {json.dumps(common)}")
            final = await asyncio.to_thread(orders.place_option_order, **common)
            logger.info(f"✅ LIVE OPTION TRADE SUCCESS: {symbol} {call_put} {strike} {expiry}")
            consecutive_failures = 0
            if is_exit:
                _record_close(str(symbol).upper(), None, payload)
            else:
                _record_open(
                    str(symbol).upper(), quantity,
                    payload.get("entry"), payload.get("stop") or payload.get("stop_price"),
                    payload.get("target"),
                    {
                        "occ_symbol": _occ_symbol(symbol, expiry, call_put, float(strike)),
                        "right": call_put,
                        "strike": float(strike),
                        "expiration": expiry,
                    },
                    action,
                )
            return {"status": "success", "response": final}

        else:
            # EQUITY ORDER
            symbol = str(ticker).upper()
            limit_price = payload.get("limit_price")
            is_equity_exit = is_close or action in {"EXIT", "CLOSE"} or (action != "BUY" and symbol in _open_positions)

            if is_equity_exit:
                # LIVE EQUITY CLOSE — cancel the broker-resting protective stop
                # FIRST so the close can never double-sell against it.
                pos = _open_positions.get(symbol) or {}
                guard = _stop_guards.get(symbol) or {}
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
                _finish_guard(symbol, "closed_by_app")
                if already_closed_by_stop:
                    _record_close(symbol, None, payload)
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
                logger.info(f"📤 Placing EQUITY CLOSE (flat kwargs): {json.dumps(common)}")
                final = await asyncio.to_thread(orders.place_equity_order, **common)
                _record_close(symbol, None, payload)
                logger.info(f"✅ LIVE EQUITY CLOSE SUCCESS: {exit_side} {qty} {symbol}")
                consecutive_failures = 0
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

            logger.info(f"📤 Placing EQUITY (flat kwargs): {json.dumps(common)}")
            final = await asyncio.to_thread(orders.place_equity_order, **common)
            logger.info(f"✅ LIVE EQUITY TRADE SUCCESS: {action} {quantity} {symbol}")
            consecutive_failures = 0

            _record_open(symbol, quantity, entry_px or None, stop_px or None, payload.get("target"), None, action)
            entry_order_id = _order_id_from_place(final)
            if stop_px > 0:
                # Arm the stop guard: poll the entry fill, then rest a real STOP
                # at E*TRADE so the position stays protected even if the app
                # goes offline.
                _arm_stop_guard(symbol, action, stop_px, entry_order_id, client_order_id)
                logger.info(f"[STOP GUARD] armed for {symbol} at {stop_px:.2f} (entry order={entry_order_id})")
            else:
                logger.warning(f"⚠️ {symbol} live entry has NO stop level — no broker-side protective stop armed")
            return {"status": "success", "response": final}

    except Exception as e:
        global circuit_breaker_opened_at
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES and not circuit_breaker_open:
            circuit_breaker_open = True
            circuit_breaker_opened_at = datetime.utcnow()
            logger.error(
                f"⛔ Circuit breaker OPEN after {consecutive_failures} consecutive failures "
                f"— auto-resets in {CIRCUIT_BREAKER_COOLDOWN_SECONDS // 60} minutes"
            )
        logger.error(f"❌ LIVE TRADE FAILED: {e}")
        raise


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
            await asyncio.to_thread(auth_manager.renew_access_token)
            logger.info("🔄 Keepalive: E*TRADE access token renewed")
        except Exception as e:
            # Renewal fails after the midnight-ET hard expiry — that requires a
            # full relink from the app, so just log it (orders will 401 and the
            # app surfaces the relink prompt).
            logger.warning(f"Keepalive renewal failed (relink may be required): {e}")


async def placement_worker():
    while not _worker_stop:
        try:
            if redis:
                job = await redis.lpop(QUEUE_KEY)
                if job:
                    await execute_live_order(json.loads(job)["payload"])
                else:
                    await asyncio.sleep(0.5)
            else:
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(2)


async def start_worker():
    global _worker_task
    _worker_task = asyncio.create_task(placement_worker())
    asyncio.create_task(token_keepalive_worker())


# ==================== STARTUP ====================
@app.on_event("startup")
async def on_startup():
    global redis
    logger.info(f"Starting → {'SANDBOX' if is_sandbox else 'PRODUCTION'} | LIVE={LIVE_TRADING} | VERSION={BOT_VERSION}")
    if REDIS_URL:
        try:
            redis = await redis_from_url(REDIS_URL, decode_responses=True)
            logger.info("✅ Redis connected")
        except Exception as e:
            logger.warning(f"Redis not available (running without queue): {e}")
            redis = None
    else:
        logger.warning("No REDIS_URL set — running without Redis queue")
        redis = None
    await init_db()
    await preload_tokens()
    # Restore persisted safety state (positions, guards, counters, kill switch)
    # and resume any stop-guards interrupted by the restart.
    _load_state()
    _resume_guards()
    await start_worker()
    logger.info("✅ Bot ready")


@app.on_event("shutdown")
async def on_shutdown():
    global _worker_stop
    _worker_stop = True
    if redis:
        await redis.close()


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

    # Atomic idempotency — a network retry can never double-place an order.
    if not store.set(sig_key, "processing", ex=86400, nx=True):
        return {"status": "duplicate", "existing_status": store.get(sig_key), "signal_id": sig_key}

    # Server-side gates for LIVE ENTRIES. Closes always pass — a protective
    # exit must never be blocked by entry gating.
    if live_intent and not is_close:
        if not _is_market_open() and not bool(pd.get("force_execute")):
            store.set(sig_key, "rejected", ex=86400)
            return {"status": "rejected", "reason": "market_closed", "signal_id": sig_key}
        ok, blocked = _passes_entry_filters(pd)
        if not ok:
            store.set(sig_key, "rejected", ex=86400)
            return {"status": "rejected", "reason": "; ".join(blocked), "signal_id": sig_key}
        cooldown_key = f"cooldown:{str(pd.get('ticker') or '').upper()}"
        if store.exists(cooldown_key):
            store.set(sig_key, "cooldown", ex=86400)
            return {"status": "cooldown", "reason": "ticker_in_cooldown", "signal_id": sig_key}
        store.set(cooldown_key, "1", ex=TICKER_COOLDOWN_MINUTES * 60)

    job = {"payload": pd}
    if redis:
        try:
            await redis.rpush(QUEUE_KEY, json.dumps(job))
            store.set(sig_key, "queued", ex=86400)
            return {"status": "queued", "signal_id": sig_key}
        except Exception as e:
            logger.warning(f"Redis push failed, processing directly: {e}")
    try:
        result = await execute_live_order(pd)
        store.set(sig_key, str(result.get("status") or "processed"), ex=86400)
        return {"status": "processed_directly", "result": result, "signal_id": sig_key}
    except Exception as e:
        store.set(sig_key, "failed", ex=86400)
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
        "circuit_breaker_open": circuit_breaker_open,
        "killed": _killed,
    }


@app.get("/healthz")
async def healthz():
    """Lightweight liveness probe polled by the app's System Monitor."""
    return {"ok": True, "ts": _utcnow().isoformat(), "version": BOT_VERSION}


@app.get("/status")
async def status():
    """Broker-state snapshot polled by the app's Reconciliation engine. The
    shape matches etrade_bot_handler's /status (open_positions keyed by ticker
    with qty/entry/stop/target/ts/contract)."""
    _reset_daily()
    return {
        "killed": _killed,
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "version": BOT_VERSION,
        "market_open": _is_market_open(),
        "open_positions": _open_positions,
        "stop_guards": _stop_guards,
        "trades_today": _trades_today,
        "realized_pnl_today_pct": _realized_pnl_today_pct,
        "circuit_breaker_open": circuit_breaker_open,
        "state_file": str(STATE_FILE),
        "filters": {
            "min_score": MIN_SCORE,
            "min_score_trending": MIN_SCORE_TRENDING,
            "min_rvol": MIN_RVOL,
            "min_mtf": MIN_MTF,
            "allowed_setups": sorted(ALLOWED_SETUPS),
        },
    }


@app.post("/kill")
async def kill(x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    global _killed
    _killed = True
    _save_state()
    logger.warning("KILL SWITCH activated")
    return {"status": "killed", "open_positions": list(_open_positions.keys())}


@app.post("/resume")
async def resume(x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    global _killed
    _killed = False
    _save_state()
    logger.info("Kill switch released — trading resumed")
    return {"status": "resumed"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
