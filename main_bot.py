"""
E*TRADE Bot — drop-in handler
=============================

Extends the minimal pyetrade stub into a production-shaped webhook that:
  • verifies HMAC / shared secret
  • enforces idempotency (no duplicate fills on retry)
  • re-applies hard filters (score, rvol, MTF, setup, market hours)
  • sizes the position from live account equity
  • places a real BRACKET order on E*TRADE (entry + stop + target)
  • emits a decision-trace so the Rork app's Auto-Trade log shows WHY each
    signal was executed or rejected from the broker's side.

Run:
    pip install -r requirements.txt
    cp .env.example .env   # fill in keys + WEBHOOK_SECRET
    uvicorn etrade_bot_handler:app --host 0.0.0.0 --port 8000

The Rork app's Auto-Trade screen posts to:
    POST /webhook
    headers:
        X-Rork-Secret: <WEBHOOK_SECRET>
        X-Rork-Signature: sha256=<hex hmac of raw body>   (either is accepted)
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from datetime import datetime, time as dtime, timezone
from zoneinfo import ZoneInfo


def _utcnow() -> datetime:
    """Timezone-aware UTC now (datetime.utcnow() is deprecated in 3.12+)."""
    return datetime.now(timezone.utc)
from pathlib import Path
from typing import Any, Literal, Optional

import pyetrade
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY", "")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET", "")
ETRADE_ENV: Literal["sandbox", "live"] = os.getenv("ETRADE_ENV", "sandbox")  # type: ignore
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
TOKENS_FILE = Path(os.getenv("ETRADE_TOKEN_FILE", ".etrade_tokens.json"))

# Gate parity with the Rork app (app defaults: minScore 85 / trending 80).
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
}

ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "50000"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "2.0"))
DAILY_TRADE_LIMIT = int(os.getenv("DAILY_TRADE_LIMIT", "20"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))
PORTFOLIO_HEAT_PCT = float(os.getenv("PORTFOLIO_HEAT_PCT", "6.0"))
TICKER_COOLDOWN_MINUTES = int(os.getenv("TICKER_COOLDOWN_MINUTES", "15"))
PAPER_MODE_DEFAULT = os.getenv("PAPER_MODE", "true").lower() == "true"

# Stop-guard: how long an entry may sit unfilled before it is cancelled, and
# how often the guard polls the broker for fill state.
ENTRY_FILL_TIMEOUT_MIN = int(os.getenv("ENTRY_FILL_TIMEOUT_MIN", "20"))
STOP_GUARD_POLL_SECONDS = int(os.getenv("STOP_GUARD_POLL_SECONDS", "10"))
# Daily state (positions, counters, kill switch, guards) survives restarts here.
STATE_FILE = Path(os.getenv("ETRADE_STATE_FILE", ".etrade_state.json"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("etrade-bot")

# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="E*TRADE Bot")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# pyetrade OAuth + persisted tokens (so /webhook works after restart)
# ---------------------------------------------------------------------------
_oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
_pending_request_token: dict[str, str] = {}
_tokens: dict[str, Any] = {}

# Persist the short-lived OAuth *request* token (not the access token) so the
# 5-minute window between /auth/start and /auth/complete survives a Railway
# cold-start or a second worker picking up the /complete call.
PENDING_FILE = Path(os.getenv("ETRADE_PENDING_FILE", ".etrade_pending.json"))


def _save_pending(token: str, secret: str) -> None:
    global _pending_request_token
    _pending_request_token = {"oauth_token": token, "oauth_token_secret": secret}
    try:
        PENDING_FILE.write_text(json.dumps(_pending_request_token))
    except OSError as e:
        log.error("pending token persist failed: %s", e)


def _load_pending() -> dict[str, str]:
    if _pending_request_token:
        return _pending_request_token
    if not PENDING_FILE.exists():
        return {}
    try:
        return json.loads(PENDING_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _capture_request_token() -> dict[str, str]:
    """Pull the request token + secret off the live _oauth session so we can
    round-trip them to the app and rebuild the session later if this process
    dies before the user pastes their verification code."""
    token = getattr(_oauth, "resource_owner_key", None)
    secret = None
    session = getattr(_oauth, "session", None)
    if session is not None:
        client = getattr(getattr(session, "_client", None), "client", None)
        token = token or getattr(client, "resource_owner_key", None)
        secret = getattr(client, "resource_owner_secret", None)
    if token and secret:
        _save_pending(str(token), str(secret))
    return {"oauth_token": str(token or ""), "oauth_token_secret": str(secret or "")}


def _restore_oauth_session(token: str, secret: str) -> None:
    """Rebuild _oauth.session against a previously-issued request token so
    get_access_token() can complete even after a restart wiped the in-memory
    OAuth1Session created by /auth/start."""
    from requests_oauthlib import OAuth1Session

    _oauth.session = OAuth1Session(
        CONSUMER_KEY,
        CONSUMER_SECRET,
        resource_owner_key=token,
        resource_owner_secret=secret,
        callback_uri="oob",
        signature_type="AUTH_HEADER",
    )
    _oauth.resource_owner_key = token

_accounts_client: Optional[pyetrade.ETradeAccounts] = None
_orders_client: Optional[pyetrade.ETradeOrder] = None
_market_client: Optional["pyetrade.ETradeMarket"] = None


def _save_tokens(tokens: dict) -> None:
    try:
        TOKENS_FILE.write_text(json.dumps(tokens))
    except OSError as e:
        log.error("token persist failed: %s", e)


def _load_tokens() -> dict:
    if not TOKENS_FILE.exists():
        return {}
    try:
        return json.loads(TOKENS_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _ensure_clients() -> tuple[pyetrade.ETradeAccounts, pyetrade.ETradeOrder]:
    global _accounts_client, _orders_client, _market_client, _tokens
    if _accounts_client and _orders_client:
        return _accounts_client, _orders_client
    if not _tokens:
        _tokens = _load_tokens()
    token = _tokens.get("oauth_token")
    secret = _tokens.get("oauth_token_secret")
    if not token or not secret:
        raise HTTPException(412, "E*TRADE not linked — call /etrade/auth/start first")
    dev = ETRADE_ENV == "sandbox"
    _accounts_client = pyetrade.ETradeAccounts(CONSUMER_KEY, CONSUMER_SECRET, token, secret, dev=dev)
    _orders_client = pyetrade.ETradeOrder(CONSUMER_KEY, CONSUMER_SECRET, token, secret, dev=dev)
    _market_client = pyetrade.ETradeMarket(CONSUMER_KEY, CONSUMER_SECRET, token, secret, dev=dev)
    return _accounts_client, _orders_client


def _ensure_market_client() -> "pyetrade.ETradeMarket":
    if _market_client is None:
        _ensure_clients()
    assert _market_client is not None
    return _market_client


# ---------------------------------------------------------------------------
# In-memory state + TTL store (swap for Redis in production)
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_open_positions: dict[str, dict] = {}
_stop_guards: dict[str, dict] = {}
_trades_today = 0
_realized_pnl_today_pct = 0.0
_today = _utcnow().date().isoformat()
_killed = False
_circuit_tripped = False


def _save_state() -> None:
    """Persist daily counters, open positions, kill switch and stop-guard state
    so a mid-day restart cannot forget risk limits or unprotected positions.
    Callers must hold _lock (or be single-threaded startup code)."""
    try:
        STATE_FILE.write_text(json.dumps({
            "date": _today,
            "trades_today": _trades_today,
            "realized_pnl_today_pct": _realized_pnl_today_pct,
            "killed": _killed,
            "circuit_tripped": _circuit_tripped,
            "open_positions": _open_positions,
            "stop_guards": _stop_guards,
        }, default=str))
    except (OSError, TypeError, ValueError) as e:
        log.error("state persist failed: %s", e)


def _load_state() -> None:
    """Restore persisted state on boot. Daily counters only restore when the
    saved date is still today; positions, guards and the kill switch always
    restore (broker reconciliation remains the source of truth)."""
    global _today, _trades_today, _realized_pnl_today_pct
    global _killed, _circuit_tripped, _open_positions, _stop_guards
    if not STATE_FILE.exists():
        return
    try:
        data = json.loads(STATE_FILE.read_text())
    except (OSError, json.JSONDecodeError) as e:
        log.error("state load failed: %s", e)
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
        _circuit_tripped = bool(data.get("circuit_tripped", False))
    log.info(
        "state restored: %d open positions, %d stop guards, trades_today=%d, pnl=%.2f%%, killed=%s",
        len(_open_positions), len(_stop_guards), _trades_today, _realized_pnl_today_pct, _killed,
    )


class _TTL:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, tuple[Any, float]] = {}

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


def _reset_daily() -> None:
    global _today, _trades_today, _realized_pnl_today_pct, _circuit_tripped
    today = _utcnow().date().isoformat()
    if today != _today:
        _today = today
        _trades_today = 0
        _realized_pnl_today_pct = 0.0
        _circuit_tripped = False
        _save_state()
        log.info("daily counters reset for %s", today)


# ---------------------------------------------------------------------------
# Webhook payload (matches AutoTradeProvider.buildPayload in the Rork app)
# ---------------------------------------------------------------------------
class TradePayload(BaseModel):
    ticker: str
    action: Literal["BUY", "SELL"]
    entry: float
    stop: float
    target: float
    score: int
    rvol: float
    mtf_alignment: str = Field(..., description="e.g. '4/5'")
    setup: str
    atr: float | None = None
    risk_reward: str | None = None
    confidence: int | None = None
    position_size_shares: int | None = None
    risk_dollars: float | None = None
    trail_stop: float | None = None
    trail_amount: float | None = None
    regime: str = ""
    session: str | None = None
    timestamp: str | None = None
    source: str | None = None
    broker: str | None = None
    mode: Literal["off", "paper", "live"] = "paper"
    force_execute: bool = False
    # Option-chain alignment (set by the Rork app when routing a day/weekly
    # signal as an option order instead of equity).
    instrument: Literal["equity", "option"] = "equity"
    option_right: Optional[Literal["CALL", "PUT"]] = None
    option_style: Optional[Literal["day", "weekly"]] = None
    strike_hint: Optional[float] = None
    expiration_hint: Optional[str] = None
    days_to_expiry_hint: Optional[int] = None
    option_contracts: Optional[int] = None
    # Exit routing (set by the Rork app's Auto-Exit engine). intent == "close"
    # (or order_action == "SELL_CLOSE") closes the previously opened contract
    # for this ticker instead of opening a new position. Closes bypass entry
    # filters, cooldowns and sizing — a protective exit must never be blocked
    # by entry gating.
    intent: Literal["open", "close"] = "open"
    order_action: Optional[str] = None
    close_reason: Optional[str] = None
    exit_price: Optional[float] = None


# ---------------------------------------------------------------------------
# Security / idempotency helpers
# ---------------------------------------------------------------------------
def _verify_secret(raw: bytes, secret_header: Optional[str], sig_header: Optional[str]) -> None:
    if not WEBHOOK_SECRET:
        return
    if sig_header:
        expected = hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
        provided = sig_header.split("=", 1)[-1].strip()
        if not hmac.compare_digest(expected, provided):
            raise HTTPException(401, "invalid signature")
        return
    if secret_header != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")


def _signal_key(p: TradePayload) -> str:
    # intent is part of the key so a SELL_CLOSE is never deduped against the
    # BUY_OPEN that opened the position.
    base = f"{p.ticker}|{p.action}|{p.entry}|{p.stop}|{p.target}|{p.timestamp}|{p.intent}"
    return "sig:" + hashlib.sha1(base.encode()).hexdigest()


_ET_ZONE = ZoneInfo("America/New_York")

# NYSE/Nasdaq full-closure holidays (observed dates).
_MARKET_HOLIDAYS: set[str] = {
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
_MARKET_HALF_DAYS: set[str] = {
    "2025-07-03", "2025-11-28", "2025-12-24",
    "2026-11-27", "2026-12-24",
    "2027-11-26",
}


def _is_market_open() -> bool:
    """Exchange-aware regular-session check: 9:30–16:00 US/Eastern (13:00 close
    on half days), weekdays only, NYSE holidays excluded. DST-safe via zoneinfo
    — no more fixed-UTC drift in winter."""
    now_et = _utcnow().astimezone(_ET_ZONE)
    if now_et.weekday() >= 5:
        return False
    day = now_et.date().isoformat()
    if day in _MARKET_HOLIDAYS:
        return False
    close = dtime(13, 0) if day in _MARKET_HALF_DAYS else dtime(16, 0)
    return dtime(9, 30) <= now_et.time() <= close


# ---------------------------------------------------------------------------
# Filters / risk / sizing
# ---------------------------------------------------------------------------
def _passes_filters(p: TradePayload) -> tuple[bool, list[str]]:
    blocked: list[str] = []
    if _killed:
        blocked.append("kill switch active")
    if p.mode == "off":
        blocked.append("client mode is OFF")

    required = MIN_SCORE_TRENDING if "trending" in p.regime.lower() else MIN_SCORE
    if p.score < required:
        blocked.append(f"score {p.score} < {required}")
    if p.rvol < MIN_RVOL:
        blocked.append(f"rvol {p.rvol} < {MIN_RVOL}")

    try:
        mtf = int(p.mtf_alignment.split("/")[0])
        if mtf < MIN_MTF:
            blocked.append(f"mtf {p.mtf_alignment} < {MIN_MTF}/5")
    except (ValueError, IndexError):
        blocked.append("mtf_alignment unparseable")

    if p.setup.lower().strip() not in ALLOWED_SETUPS:
        blocked.append(f"setup '{p.setup}' not allowlisted")

    with _lock:
        _reset_daily()
        if _circuit_tripped:
            blocked.append("circuit breaker tripped")
        if _realized_pnl_today_pct <= -abs(DAILY_LOSS_LIMIT_PCT):
            blocked.append(f"daily loss limit ({_realized_pnl_today_pct:.2f}%)")
        if _trades_today >= DAILY_TRADE_LIMIT:
            blocked.append(f"daily trade limit ({DAILY_TRADE_LIMIT}) reached")
        if len(_open_positions) >= MAX_CONCURRENT_POSITIONS:
            blocked.append(f"max positions ({MAX_CONCURRENT_POSITIONS}) open")
        if p.ticker in _open_positions:
            blocked.append(f"already in {p.ticker}")
        open_risk = sum(abs(pos["entry"] - pos["stop"]) * pos["qty"] for pos in _open_positions.values())
        new_risk = ACCOUNT_SIZE * (RISK_PER_TRADE_PCT / 100.0)
        heat = (open_risk + new_risk) / max(ACCOUNT_SIZE, 1) * 100.0
        if heat > PORTFOLIO_HEAT_PCT:
            blocked.append(f"portfolio heat {heat:.2f}% > {PORTFOLIO_HEAT_PCT}%")

    return len(blocked) == 0, blocked


def _live_equity() -> Optional[float]:
    """Fetch real account equity from E*TRADE. Returns None on any failure —
    live sizing must FAIL CLOSED (reject the trade) rather than silently size
    off the ACCOUNT_SIZE default, which oversizes smaller real accounts."""
    try:
        accounts, _ = _ensure_clients()
        lst = accounts.list_accounts(resp_format="json")
        acct = lst["AccountListResponse"]["Accounts"]["Account"][0]
        bal = accounts.get_account_balance(
            acct["accountIdKey"],
            account_type=acct.get("accountType"),
            institution_type=acct.get("institutionType", "BROKERAGE"),
            resp_format="json",
        )
        val = (
            bal.get("BalanceResponse", {})
            .get("Computed", {})
            .get("RealTimeValues", {})
            .get("totalAccountValue")
        )
        return float(val) if val else None
    except Exception as e:  # noqa: BLE001
        log.error("equity fetch failed (%s) — live sizing will fail closed", e)
        return None


def _size_position(p: TradePayload, equity: float) -> int:
    dist = abs(p.entry - p.stop)
    if dist <= 0:
        return 0
    risk = equity * (RISK_PER_TRADE_PCT / 100.0)
    qty = int(risk // dist)
    if p.score >= 90:
        return qty
    if p.score >= 85:
        return qty // 2
    return 0


def _account_id_key(accounts: pyetrade.ETradeAccounts) -> str:
    lst = accounts.list_accounts(resp_format="json")
    return lst["AccountListResponse"]["Accounts"]["Account"][0]["accountIdKey"]


# ---------------------------------------------------------------------------
# Stop guard — makes the protective stop REST AT THE BROKER
#
# The entry LIMIT goes in first; a daemon watcher polls the broker until the
# entry fills, then places a real STOP order at E*TRADE for the filled
# quantity (replacing it as partial fills grow). If the entry has zero fill by
# ENTRY_FILL_TIMEOUT_MIN it is cancelled. The position stays protected even
# when the Rork app is backgrounded, offline, or the phone is locked.
# Guard state persists to STATE_FILE and resumes after a restart.
# ---------------------------------------------------------------------------
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


def _order_state(order_id: Optional[str], client_id: Optional[str]) -> tuple[str, int]:
    """Return (status, total filled quantity) for an order, matched by orderId
    or clientOrderId in the account's recent orders. ('NOT_FOUND', 0) when the
    order is not in the list."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    resp = orders.list_orders(acct_key, resp_format="json")
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


def _cancel_order_safe(order_id: Optional[str]) -> bool:
    """Best-effort broker order cancel. Returns True only when the cancel call
    succeeded — callers must treat False as 'the order may still be live'."""
    if not order_id:
        return False
    try:
        accounts, orders = _ensure_clients()
        acct_key = _account_id_key(accounts)
        orders.cancel_order(acct_key, int(order_id), resp_format="json")
        log.info("[STOP GUARD] cancelled order %s", order_id)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("order cancel failed (%s): %s", order_id, e)
        return False


def _place_protective_stop(ticker: str, action: str, qty: int, stop_price: float) -> dict:
    """Rest a protective STOP order at E*TRADE for a filled entry."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    exit_side = "SELL" if action == "BUY" else "BUY_TO_COVER"
    client_id = f"rork-stop-{ticker}-{int(_utcnow().timestamp())}"
    common = dict(
        accountIdKey=acct_key,
        symbol=ticker,
        orderAction=exit_side,
        clientOrderId=client_id,
        priceType="STOP",
        stopPrice=round(stop_price, 2),
        quantity=qty,
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        resp_format="json",
    )
    preview = orders.preview_equity_order(**common)
    preview_id = preview["PreviewOrderResponse"]["PreviewIds"][0]["previewId"]
    placed = orders.place_equity_order(previewId=preview_id, **common)
    order_id = _order_id_from_place(placed)
    log.info(
        "[STOP GUARD] protective stop RESTING at broker: %s %s qty=%d stop=%.2f (order=%s)",
        exit_side, ticker, qty, stop_price, order_id,
    )
    return {"order_id": order_id, "client_id": client_id, "qty": qty, "stop": round(stop_price, 2)}


def _finish_guard(ticker: str, result: str) -> None:
    with _lock:
        g = _stop_guards.get(ticker)
        if g:
            g["done"] = True
            g["result"] = result
            _save_state()
    log.info("[STOP GUARD] %s finished: %s", ticker, result)


def _stop_guard_worker(ticker: str) -> None:
    """Poll the entry order; once (partially) filled, rest a protective STOP at
    the broker sized to the filled quantity. Cancel entries with zero fill at
    the deadline. Retries stop placement on every poll until protected."""
    log.info("[STOP GUARD] watching %s entry fill", ticker)
    while True:
        with _lock:
            guard = dict(_stop_guards.get(ticker) or {})
        if not guard or guard.get("done"):
            return

        status = str(guard.get("last_status") or "OPEN")
        filled = int(guard.get("last_filled") or 0)
        try:
            status, filled = _order_state(guard.get("entry_order_id"), guard.get("entry_client_id"))
        except Exception as e:  # noqa: BLE001
            log.warning("[STOP GUARD] %s poll failed: %s", ticker, e)

        guarded = int(guard.get("guarded_qty") or 0)
        if filled > guarded:
            # (Re)place the protective stop for the total filled quantity.
            can_place = True
            if guard.get("stop_order_id"):
                # Replace flow: only place a new stop if the old one is truly
                # cancelled — never risk two live stops double-selling.
                can_place = _cancel_order_safe(guard.get("stop_order_id"))
            if can_place:
                try:
                    stop_info = _place_protective_stop(
                        ticker, str(guard.get("action") or "BUY"), filled, float(guard.get("stop") or 0),
                    )
                    with _lock:
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
                except Exception as e:  # noqa: BLE001
                    log.error("[STOP GUARD] %s stop placement FAILED (will retry): %s", ticker, e)

        with _lock:
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
            with _lock:
                _open_positions.pop(ticker, None)
                _save_state()
            return
        if time.time() >= float(guard.get("deadline_ts") or 0):
            if filled == 0:
                _cancel_order_safe(guard.get("entry_order_id"))
                _finish_guard(ticker, "entry_timeout_cancelled")
                with _lock:
                    _open_positions.pop(ticker, None)
                    _save_state()
            elif guarded >= filled:
                _finish_guard(ticker, "partial_fill_protected")
            else:
                # Filled but stop never stuck — keep trying rather than walk away.
                log.error("[STOP GUARD] %s UNPROTECTED at deadline — extending guard", ticker)
                with _lock:
                    g = _stop_guards.get(ticker)
                    if g:
                        g["deadline_ts"] = time.time() + ENTRY_FILL_TIMEOUT_MIN * 60
                        _save_state()
                time.sleep(STOP_GUARD_POLL_SECONDS)
                continue
            return
        time.sleep(STOP_GUARD_POLL_SECONDS)


def _spawn_guard(ticker: str) -> None:
    threading.Thread(
        target=_stop_guard_worker, args=(ticker,), daemon=True, name=f"stop-guard-{ticker}",
    ).start()


def _resume_guards() -> None:
    """Respawn watcher threads for guards interrupted by a restart."""
    with _lock:
        pending = [t for t, g in _stop_guards.items() if not g.get("done")]
    for t in pending:
        log.info("[STOP GUARD] resuming guard for %s after restart", t)
        _spawn_guard(t)


def _place_bracket(p: TradePayload, qty: int) -> dict:
    """Entry LIMIT order + broker-resting protective STOP (via the stop guard).

    The take-profit side stays managed by the Rork app's trailing logic; the
    hard stop lives at E*TRADE so the position is protected even if the app
    goes offline."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    side = "BUY" if p.action == "BUY" else "SELL_SHORT"
    client_id = f"rork-{p.ticker}-{int(_utcnow().timestamp())}"

    common = dict(
        accountIdKey=acct_key,
        symbol=p.ticker,
        orderAction=side,
        clientOrderId=client_id,
        priceType="LIMIT",
        limitPrice=round(p.entry, 2),
        quantity=qty,
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        resp_format="json",
    )
    preview = orders.preview_equity_order(**common)
    preview_id = preview["PreviewOrderResponse"]["PreviewIds"][0]["previewId"]
    placed = orders.place_equity_order(previewId=preview_id, **common)
    order_id = _order_id_from_place(placed)

    with _lock:
        _stop_guards[p.ticker] = {
            "ticker": p.ticker,
            "action": p.action,
            "stop": round(p.stop, 2),
            "entry_order_id": order_id,
            "entry_client_id": client_id,
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
    _spawn_guard(p.ticker)

    log.info(
        "[LIVE] %s %s qty=%d entry=%.2f stop=%.2f target=%.2f (preview=%s order=%s) — stop guard armed",
        p.action, p.ticker, qty, p.entry, p.stop, p.target, preview_id, order_id,
    )
    return {"preview_id": preview_id, "placed": placed, "client_id": client_id, "order_id": order_id}


# ---------------------------------------------------------------------------
# Option chain resolution
# ---------------------------------------------------------------------------
def _parse_expiry(raw: Any) -> Optional[datetime]:
    """Coerce an E*TRADE expiry node ({year,month,day}) or an ISO date into a
    timezone-aware UTC datetime so we can compare candidates uniformly."""
    if not raw:
        return None
    try:
        if isinstance(raw, dict):
            y = int(raw.get("year") or raw.get("Year") or 0)
            m = int(raw.get("month") or raw.get("Month") or 0)
            d = int(raw.get("day") or raw.get("Day") or 0)
            if not (y and m and d):
                return None
            return datetime(y, m, d, tzinfo=timezone.utc)
        s = str(raw)
        # ISO 'YYYY-MM-DD' (possibly with time)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _list_expirations(ticker: str) -> list[dict]:
    """Return E*TRADE option expiration dates for `ticker` as a list of
    {date: datetime, raw: dict, weekly: bool, days: int}."""
    market = _ensure_market_client()
    try:
        resp = market.get_option_expire_date(ticker, resp_format="json")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"option expirations fetch failed: {e}") from e
    body = resp.get("OptionExpireDateResponse", resp) if isinstance(resp, dict) else {}
    raw_dates = body.get("ExpirationDate") or body.get("expirationDate") or []
    if isinstance(raw_dates, dict):
        raw_dates = [raw_dates]
    today = _utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    out: list[dict] = []
    for entry in raw_dates:
        dt = _parse_expiry(entry)
        if not dt:
            continue
        days = max(0, (dt - today).days)
        kind = str(entry.get("expiryType") or entry.get("ExpiryType") or "").upper()
        out.append({
            "date": dt,
            "raw": entry,
            "weekly": kind == "WEEKLY" or dt.weekday() == 4,  # Friday or tagged WEEKLY
            "days": days,
        })
    out.sort(key=lambda x: x["date"])
    return out


def _pick_expiration(ticker: str, style: str, hint: Optional[str]) -> dict:
    """Pick the best expiration for the requested style.

    - `day` -> nearest available expiration (0DTE if listed, else next session).
    - `weekly` -> nearest expiration tagged WEEKLY (or the next Friday).
    `hint` (ISO date from the Rork alert) is preferred when it matches a real
    listed expiry.
    """
    expirations = _list_expirations(ticker)
    if not expirations:
        raise HTTPException(422, f"no option expirations listed for {ticker}")

    hint_dt = _parse_expiry(hint) if hint else None
    if hint_dt:
        for e in expirations:
            if e["date"].date() == hint_dt.date():
                return e

    if style == "day":
        return expirations[0]
    # weekly: prefer tagged weekly, else first Friday on or after today.
    weeklies = [e for e in expirations if e["weekly"]]
    return (weeklies or expirations)[0]


def _pick_strike(chain_pairs: list[dict], right: str, target_strike: float) -> dict:
    """Return the option-pair node whose call/put strike is closest to
    `target_strike` (ATM). Strikes come through pyetrade as either a flat field
    on the pair or under `Product.strikePrice` depending on env."""
    if not chain_pairs:
        raise HTTPException(422, "option chain returned no strikes")
    side_key = "Call" if right == "CALL" else "Put"

    def _strike_of(pair: dict) -> Optional[float]:
        for candidate in (pair.get(side_key), pair):
            if not isinstance(candidate, dict):
                continue
            for key in ("strikePrice", "StrikePrice", "strike"):
                if key in candidate:
                    try:
                        return float(candidate[key])
                    except (TypeError, ValueError):
                        pass
            prod = candidate.get("Product") or candidate.get("product")
            if isinstance(prod, dict):
                for key in ("strikePrice", "StrikePrice"):
                    if key in prod:
                        try:
                            return float(prod[key])
                        except (TypeError, ValueError):
                            pass
        return None

    scored: list[tuple[float, dict, float]] = []
    for pair in chain_pairs:
        strike = _strike_of(pair)
        if strike is None:
            continue
        scored.append((abs(strike - target_strike), pair, strike))
    if not scored:
        raise HTTPException(422, "could not parse strikes from option chain")
    scored.sort(key=lambda x: x[0])
    _, pair, strike = scored[0]
    pair["_resolved_strike"] = strike
    return pair


def _occ_symbol(ticker: str, expiry: datetime, right: str, strike: float) -> str:
    """Build an OCC 21-char option symbol: ROOT(6) + YYMMDD + C/P + STRIKE*1000(8)."""
    root = ticker.upper().ljust(6)
    date = expiry.strftime("%y%m%d")
    cp = "C" if right == "CALL" else "P"
    strike_int = int(round(strike * 1000))
    return f"{root}{date}{cp}{strike_int:08d}"


def _resolve_option_contract(p: TradePayload) -> dict:
    """Resolve a TradePayload (instrument='option') into a concrete OCC
    contract by querying E*TRADE's optionchains endpoint. Returns a dict the
    order placer + decision trace can consume."""
    right = (p.option_right or ("CALL" if p.action == "BUY" else "PUT")).upper()
    style = (p.option_style or "weekly").lower()
    expiration = _pick_expiration(p.ticker, style, p.expiration_hint)
    target_strike = float(p.strike_hint or p.entry)

    market = _ensure_market_client()
    exp_date = expiration["date"]
    try:
        chain = market.get_option_chains(
            p.ticker,
            expiry_date=exp_date,
            chain_type="CALLPUT",
            skip_adjusted="TRUE",
            no_of_strikes=20,
            strike_price_near=target_strike,
            resp_format="json",
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"option chain fetch failed: {e}") from e

    body = chain.get("OptionChainResponse", chain) if isinstance(chain, dict) else {}
    pairs = body.get("OptionPair") or body.get("optionPair") or []
    if isinstance(pairs, dict):
        pairs = [pairs]
    pair = _pick_strike(pairs, right, target_strike)
    strike = float(pair.get("_resolved_strike") or target_strike)
    leg = pair.get("Call" if right == "CALL" else "Put") or {}
    bid = float(leg.get("bid") or leg.get("Bid") or 0.0)
    ask = float(leg.get("ask") or leg.get("Ask") or 0.0)
    mid = round((bid + ask) / 2.0, 2) if (bid and ask) else (ask or bid or 0.0)

    return {
        "ticker": p.ticker,
        "right": right,
        "style": style,
        "expiration": exp_date.date().isoformat(),
        "days_to_expiry": expiration["days"],
        "is_weekly": bool(expiration["weekly"]),
        "strike": strike,
        "occ_symbol": _occ_symbol(p.ticker, exp_date, right, strike),
        "bid": bid,
        "ask": ask,
        "mid": mid,
    }


def _place_option_order(p: TradePayload, contract: dict, qty: int) -> dict:
    """Place a single-leg option order at the mid price. Quantity is in
    contracts. Falls back to limit = ask for BUY / bid for SELL when mid is 0."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    # For option spreads the action is always BUY_OPEN / SELL_CLOSE etc.; the
    # Rork signal action just tells us call-vs-put (already in contract.right).
    side = "BUY_OPEN"
    client_id = f"rork-opt-{p.ticker}-{int(_utcnow().timestamp())}"
    mid = contract.get("mid") or contract.get("ask") or 0.01
    limit_price = round(max(0.01, float(mid)), 2)

    common = dict(
        accountIdKey=acct_key,
        symbol=p.ticker,
        orderAction=side,
        clientOrderId=client_id,
        priceType="LIMIT",
        limitPrice=limit_price,
        quantity=qty,
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        callPut=contract["right"],
        strikePrice=contract["strike"],
        expiryDate=contract["expiration"],
        resp_format="json",
    )

    # pyetrade exposes option order placement through the same ETradeOrder
    # client; method names differ slightly across versions, so we probe.
    preview_fn = getattr(orders, "preview_option_order", None) or getattr(orders, "preview_equity_order")
    place_fn = getattr(orders, "place_option_order", None) or getattr(orders, "place_equity_order")
    preview = preview_fn(**common)
    preview_id = preview["PreviewOrderResponse"]["PreviewIds"][0]["previewId"]
    placed = place_fn(previewId=preview_id, **common)
    log.info(
        "[LIVE option] %s %s %s %s qty=%d limit=%.2f (preview=%s)",
        p.action, p.ticker, contract["right"], contract["occ_symbol"], qty, limit_price, preview_id,
    )
    return {"preview_id": preview_id, "placed": placed, "client_id": client_id, "contract": contract, "limit_price": limit_price}


def _close_option_order(p: TradePayload, contract: dict, qty: int) -> dict:
    """Close a single-leg option position with a SELL_CLOSE LIMIT at the mid
    price (falls back to the bid, then a floor of 0.01)."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    client_id = f"rork-optclose-{p.ticker}-{int(_utcnow().timestamp())}"
    mid = contract.get("mid") or contract.get("bid") or 0.01
    limit_price = round(max(0.01, float(mid)), 2)

    common = dict(
        accountIdKey=acct_key,
        symbol=p.ticker,
        orderAction="SELL_CLOSE",
        clientOrderId=client_id,
        priceType="LIMIT",
        limitPrice=limit_price,
        quantity=qty,
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        callPut=contract["right"],
        strikePrice=contract["strike"],
        expiryDate=contract["expiration"],
        resp_format="json",
    )

    preview_fn = getattr(orders, "preview_option_order", None) or getattr(orders, "preview_equity_order")
    place_fn = getattr(orders, "place_option_order", None) or getattr(orders, "place_equity_order")
    preview = preview_fn(**common)
    preview_id = preview["PreviewOrderResponse"]["PreviewIds"][0]["previewId"]
    placed = place_fn(previewId=preview_id, **common)
    log.info(
        "[LIVE option CLOSE] %s %s %s qty=%d limit=%.2f reason=%s (preview=%s)",
        p.ticker, contract["right"], contract.get("occ_symbol", "?"), qty, limit_price,
        p.close_reason or "unspecified", preview_id,
    )
    return {"preview_id": preview_id, "placed": placed, "client_id": client_id, "contract": contract, "limit_price": limit_price}


def _close_equity_order(p: TradePayload, qty: int) -> dict:
    """Close a live equity position with a MARKET order (protective closes
    prioritize certainty of fill over price)."""
    accounts, orders = _ensure_clients()
    acct_key = _account_id_key(accounts)
    exit_side = "SELL" if p.action == "BUY" else "BUY_TO_COVER"
    client_id = f"rork-eqclose-{p.ticker}-{int(_utcnow().timestamp())}"
    common = dict(
        accountIdKey=acct_key,
        symbol=p.ticker,
        orderAction=exit_side,
        clientOrderId=client_id,
        priceType="MARKET",
        quantity=qty,
        orderTerm="GOOD_FOR_DAY",
        marketSession="REGULAR",
        resp_format="json",
    )
    preview = orders.preview_equity_order(**common)
    preview_id = preview["PreviewOrderResponse"]["PreviewIds"][0]["previewId"]
    placed = orders.place_equity_order(previewId=preview_id, **common)
    order_id = _order_id_from_place(placed)
    log.info(
        "[LIVE equity CLOSE] %s %s qty=%d reason=%s (order=%s)",
        exit_side, p.ticker, qty, p.close_reason or "unspecified", order_id,
    )
    return {"preview_id": preview_id, "placed": placed, "client_id": client_id, "order_id": order_id}


def _handle_close(payload: TradePayload, sig_key: str) -> dict:
    """Execute a protective close for an open position. Deliberately bypasses
    the entry filters, ticker cooldown, sizing and the kill switch — closing an
    open position always REDUCES risk, so it must never be gated like an entry.
    """
    with _lock:
        pos = _open_positions.get(payload.ticker)
        guard = _stop_guards.get(payload.ticker)

    contract: Optional[dict] = pos.get("contract") if pos else None
    qty = int(pos["qty"]) if pos and pos.get("qty") else max(1, payload.option_contracts or 1)
    entry = float(pos["entry"]) if pos and pos.get("entry") else payload.entry
    exit_price = payload.exit_price if payload.exit_price else payload.entry

    # Realized pnl on the underlying move (signed by direction). Feeds the
    # daily loss-limit accounting the same way an entry-side fill would.
    direction = 1.0 if payload.action == "BUY" else -1.0
    pnl_pct = direction * ((exit_price - entry) / entry * 100.0) if entry else 0.0

    is_paper = payload.mode == "paper" or (payload.mode != "live" and PAPER_MODE_DEFAULT)

    if not is_paper:
        is_option_close = payload.instrument == "option" or contract is not None
        if is_option_close:
            if contract is None:
                # Position state was lost (restart) — re-resolve the contract
                # from the payload hints so the close can still go out.
                try:
                    contract = _resolve_option_contract(payload)
                except HTTPException as opt_exc:
                    store.set(sig_key, "failed", ex=86400)
                    return {
                        "status": "failed",
                        "reason": f"close_option_chain: {opt_exc.detail}",
                        "trace": _build_trace(payload, {"close": False, "option_chain": False}, [str(opt_exc.detail)]),
                    }
            try:
                etrade_resp = _close_option_order(payload, contract, qty)
            except Exception as e:  # noqa: BLE001
                store.set(sig_key, "failed", ex=86400)
                log.error("E*TRADE close error: %s", e, exc_info=True)
                return {
                    "status": "failed",
                    "reason": f"etrade_close_error: {e}",
                    "trace": _build_trace(payload, {"close": False, "broker": False}, [str(e)]),
                }
        else:
            # Live EQUITY close: first cancel the broker-resting protective
            # stop so the close can never double-sell against it.
            stop_order_id = (pos or {}).get("stop_order_id") or (guard or {}).get("stop_order_id")
            already_closed_by_stop = False
            if stop_order_id and not _cancel_order_safe(stop_order_id):
                try:
                    stop_status, _stop_filled = _order_state(stop_order_id, None)
                except Exception as e:  # noqa: BLE001
                    stop_status = "UNKNOWN"
                    log.warning("stop status check failed for %s: %s", payload.ticker, e)
                if stop_status == "EXECUTED":
                    already_closed_by_stop = True
                elif stop_status not in {"CANCELLED", "REJECTED", "EXPIRED", "NOT_FOUND"}:
                    store.set(sig_key, "failed", ex=86400)
                    reason = f"could not cancel resting stop {stop_order_id} — refusing to double-sell"
                    return {
                        "status": "failed",
                        "reason": reason,
                        "trace": _build_trace(payload, {"close": False, "broker": False}, [reason]),
                    }
            if already_closed_by_stop:
                etrade_resp = {"note": "resting protective stop already executed at broker", "stop_order_id": stop_order_id}
                log.info("[LIVE equity CLOSE] %s already closed by resting stop %s", payload.ticker, stop_order_id)
            else:
                fill_qty = int((pos or {}).get("filled_qty") or 0) or qty
                try:
                    etrade_resp = _close_equity_order(payload, fill_qty)
                except Exception as e:  # noqa: BLE001
                    store.set(sig_key, "failed", ex=86400)
                    log.error("E*TRADE equity close error: %s", e, exc_info=True)
                    return {
                        "status": "failed",
                        "reason": f"etrade_close_error: {e}",
                        "trace": _build_trace(payload, {"close": False, "broker": False}, [str(e)]),
                    }
        _finish_guard(payload.ticker, "closed_by_app")
    else:
        etrade_resp = None
        log.info(
            "[PAPER CLOSE] %s qty=%d exit=%.2f reason=%s pnl=%.2f%%",
            payload.ticker, qty, exit_price, payload.close_reason or "unspecified", pnl_pct,
        )

    with _lock:
        _open_positions.pop(payload.ticker, None)
        globals()["_realized_pnl_today_pct"] = _realized_pnl_today_pct + pnl_pct
        _save_state()

    store.set(sig_key, "executed", ex=86400)
    close_info = {
        "reason": payload.close_reason or "unspecified",
        "exit_price": exit_price,
        "entry": entry,
        "qty": qty,
        "pnl_pct": round(pnl_pct, 2),
    }
    order: dict[str, Any] = {
        "mode": "paper" if is_paper else "live",
        "filled": True,
        "close": close_info,
        "instrument": "option" if (contract or payload.instrument == "option") else "equity",
        "contract": contract,
    }
    if etrade_resp is not None:
        order["etrade"] = etrade_resp
    return {
        "status": "executed",
        "signal_id": sig_key,
        "mode": "paper" if is_paper else "live",
        "instrument": order["instrument"],
        "contract": contract,
        "close": close_info,
        "order": order,
        "trace": _build_trace(
            payload,
            checks={"market_open": True, "close": True, "broker": not is_paper},
            reasons=[],
        ),
    }


def _build_trace(p: TradePayload, checks: dict[str, bool], reasons: list[str]) -> dict:
    return {
        "ticker": p.ticker,
        "action": p.action,
        "score": p.score,
        "setup": p.setup,
        "regime": p.regime,
        "checks": checks,
        "rejection_reasons": reasons,
        "ts": _utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok", "env": ETRADE_ENV}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": _utcnow().isoformat()}


@app.post("/etrade/auth/start")
async def auth_start():
    try:
        url = _oauth.get_request_token(params={"oauth_callback": "oob", "format": "json"})
        # pyetrade returns a string URL OR (token, secret) depending on version.
        # Capture + persist the request token so /complete can rebuild the
        # session even if this process restarts before the user pastes the code.
        pending = _capture_request_token()
        return {
            "authorize_url": url if isinstance(url, str) else url[0],
            "request_token": pending.get("oauth_token") or None,
            "env": ETRADE_ENV,
        }
    except Exception as e:  # noqa: BLE001
        raise HTTPException(500, f"Start failed: {e}")


@app.post("/etrade/auth/complete")
async def auth_complete(request: Request):
    global _tokens, _accounts_client, _orders_client, _pending_request_token
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or "").strip()
        if not verifier:
            raise HTTPException(400, "Missing verification code")
        # If the in-memory session is gone (restart / different worker), rebuild
        # it from the request token the app round-tripped, or from disk.
        if getattr(_oauth, "session", None) is None:
            req_token = str(data.get("request_token") or data.get("oauth_token") or "").strip()
            pending = _load_pending()
            tok = req_token or pending.get("oauth_token", "")
            sec = pending.get("oauth_token_secret", "")
            if tok and sec:
                _restore_oauth_session(tok, sec)
            else:
                raise HTTPException(
                    409,
                    "Authorization session expired — tap Link again to restart E*TRADE OAuth",
                )
        tokens = _oauth.get_access_token(verifier)
        _tokens = {
            "oauth_token": tokens.get("oauth_token"),
            "oauth_token_secret": tokens.get("oauth_token_secret"),
            "linked_at": _utcnow().isoformat(),
            "env": ETRADE_ENV,
        }
        _save_tokens(_tokens)
        _accounts_client = None
        _orders_client = None
        # Request token is consumed — clear the pending state on both sides.
        _pending_request_token = {}
        if PENDING_FILE.exists():
            try:
                PENDING_FILE.unlink()
            except OSError:
                pass
        # Echo the freshly-minted tokens back so the Rork app can persist them
        # locally and survive a Railway/bot restart without re-OAuth.
        return {
            "status": "linked",
            "linked": True,
            "env": ETRADE_ENV,
            "linked_at": _tokens["linked_at"],
            "tokens": {
                "oauth_token": _tokens["oauth_token"],
                "oauth_token_secret": _tokens["oauth_token_secret"],
                "env": ETRADE_ENV,
                "linked_at": _tokens["linked_at"],
            },
        }
    except Exception as e:  # noqa: BLE001
        raise HTTPException(500, f"Complete failed: {e}")


@app.get("/etrade/account")
async def etrade_account():
    saved = _load_tokens()
    if not saved:
        return {"linked": False, "env": ETRADE_ENV}
    try:
        accounts, _ = _ensure_clients()
        lst = accounts.list_accounts(resp_format="json")
        acct = lst["AccountListResponse"]["Accounts"]["Account"][0]
        return {
            "linked": True,
            "env": ETRADE_ENV,
            "linked_at": saved.get("linked_at"),
            "account_id": acct.get("accountId"),
            "account_id_key": acct.get("accountIdKey"),
            "account_type": acct.get("accountType"),
            "equity": _live_equity(),
        }
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"account fetch failed: {e}")


@app.post("/etrade/auth/restore")
async def auth_restore(request: Request):
    """Re-hydrate the bot's E*TRADE session from tokens the Rork app cached
    locally. Lets a cold-started Railway instance resume without forcing the
    user back through OAuth."""
    global _tokens, _accounts_client, _orders_client
    try:
        data = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(400, "invalid json")

    token = str(data.get("oauth_token") or "").strip()
    secret = str(data.get("oauth_token_secret") or "").strip()
    if not token or not secret:
        raise HTTPException(400, "oauth_token and oauth_token_secret required")

    _tokens = {
        "oauth_token": token,
        "oauth_token_secret": secret,
        "env": str(data.get("env") or ETRADE_ENV),
        "linked_at": str(data.get("linked_at") or _utcnow().isoformat()),
    }
    _save_tokens(_tokens)
    _accounts_client = None
    _orders_client = None
    return {"status": "restored", "linked": True, "env": _tokens["env"], "linked_at": _tokens["linked_at"]}


@app.post("/etrade/auth/renew")
async def auth_renew(request: Request):
    """Keep the E*TRADE access token "locked in". E*TRADE access tokens go
    idle after 2h of inactivity and expire at midnight US Eastern. Calling
    renew_access_token reactivates an idle token so the linked session never
    silently dies. Optionally accepts {oauth_token, oauth_token_secret} to
    rehydrate first (so a cold-started bot can renew from app-cached tokens)."""
    global _tokens, _accounts_client, _orders_client
    try:
        data = await request.json()
    except (json.JSONDecodeError, Exception):  # noqa: BLE001
        data = {}

    # Allow the app to push its locally-cached tokens so we can renew even
    # after a Railway cold start wiped in-memory/file state.
    token = str((data or {}).get("oauth_token") or "").strip()
    secret = str((data or {}).get("oauth_token_secret") or "").strip()
    if token and secret:
        _tokens = {
            "oauth_token": token,
            "oauth_token_secret": secret,
            "env": str((data or {}).get("env") or ETRADE_ENV),
            "linked_at": str((data or {}).get("linked_at") or _utcnow().isoformat()),
        }
        _save_tokens(_tokens)
        _accounts_client = None
        _orders_client = None

    snapshot = _tokens or _load_tokens()
    tok = str((snapshot or {}).get("oauth_token") or "").strip()
    sec = str((snapshot or {}).get("oauth_token_secret") or "").strip()
    if not tok or not sec:
        raise HTTPException(412, "E*TRADE not linked — nothing to renew")
    try:
        manager = pyetrade.ETradeAccessManager(
            CONSUMER_KEY, CONSUMER_SECRET, tok, sec
        )
        manager.renew_access_token()
        _tokens = {**(snapshot or {}), "renewed_at": _utcnow().isoformat()}
        _save_tokens(_tokens)
        # Drop cached clients so the next call rebuilds against the live session.
        _accounts_client = None
        _orders_client = None
        log.info("E*TRADE access token renewed (locked in)")
        return {
            "status": "renewed",
            "linked": True,
            "env": _tokens.get("env", ETRADE_ENV),
            "linked_at": _tokens.get("linked_at"),
            "renewed_at": _tokens["renewed_at"],
            "tokens": {
                "oauth_token": tok,
                "oauth_token_secret": sec,
                "env": _tokens.get("env", ETRADE_ENV),
                "linked_at": _tokens.get("linked_at"),
            },
        }
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"renew failed: {e}")


def _revoke_remote_tokens(reason: str) -> dict[str, Any]:
    """Best-effort: call E*TRADE's revoke_access_token so the upstream session
    is truly killed. Returns a small status dict for the response body so the
    app can show whether the remote revoke succeeded."""
    snapshot = _tokens or _load_tokens()
    token = str((snapshot or {}).get("oauth_token") or "").strip()
    secret = str((snapshot or {}).get("oauth_token_secret") or "").strip()
    if not token or not secret:
        return {"revoked": False, "reason": "no_tokens"}
    try:
        manager = pyetrade.ETradeAccessManager(
            CONSUMER_KEY, CONSUMER_SECRET, token, secret
        )
        manager.revoke_access_token()
        log.info("E*TRADE access token revoked upstream (%s)", reason)
        return {"revoked": True}
    except Exception as e:  # noqa: BLE001
        log.warning("E*TRADE revoke failed (%s): %s", reason, e)
        return {"revoked": False, "reason": str(e)}


@app.post("/etrade/disconnect")
async def etrade_disconnect():
    """Forget every cached/persisted token AND revoke them upstream so the next
    link starts clean on both sides."""
    global _tokens, _accounts_client, _orders_client, _pending_request_token
    revoke = _revoke_remote_tokens("disconnect")
    _tokens = {}
    _accounts_client = None
    _orders_client = None
    _pending_request_token = {}
    if TOKENS_FILE.exists():
        try:
            TOKENS_FILE.unlink()
        except OSError as e:
            log.warning("token file unlink failed: %s", e)
    log.info("E*TRADE session disconnected (tokens cleared, remote revoke=%s)", revoke.get("revoked"))
    return {
        "status": "disconnected",
        "linked": False,
        "env": ETRADE_ENV,
        "remote_revoke": revoke,
    }


@app.post("/etrade/relink")
async def etrade_relink():
    """Convenience: disconnect existing session and immediately mint a fresh
    request-token URL. The Rork app can call this single endpoint when the
    user taps “Relink”."""
    global _tokens, _accounts_client, _orders_client, _pending_request_token
    revoke = _revoke_remote_tokens("relink")
    _tokens = {}
    _accounts_client = None
    _orders_client = None
    _pending_request_token = {}
    if TOKENS_FILE.exists():
        try:
            TOKENS_FILE.unlink()
        except OSError:
            pass
    _ = revoke
    if PENDING_FILE.exists():
        try:
            PENDING_FILE.unlink()
        except OSError:
            pass
    try:
        url = _oauth.get_request_token(params={"oauth_callback": "oob", "format": "json"})
        pending = _capture_request_token()
        return {
            "status": "relink",
            "linked": False,
            "env": ETRADE_ENV,
            "authorize_url": url if isinstance(url, str) else url[0],
            "request_token": pending.get("oauth_token") or None,
            "remote_revoke": revoke,
        }
    except Exception as e:  # noqa: BLE001
        raise HTTPException(500, f"Relink failed: {e}")


@app.post("/webhook")
async def webhook(
    request: Request,
    x_rork_secret: Optional[str] = Header(default=None, alias="X-Rork-Secret"),
    x_signature: Optional[str] = Header(default=None, alias="X-Rork-Signature"),
):
    raw = await request.body()
    _verify_secret(raw, x_rork_secret, x_signature)

    try:
        payload = TradePayload(**json.loads(raw or b"{}"))
    except json.JSONDecodeError:
        raise HTTPException(400, "invalid json")

    log.info(
        "signal: %s %s @ %.2f score=%d setup=%s mode=%s",
        payload.action, payload.ticker, payload.entry, payload.score, payload.setup, payload.mode,
    )

    sig_key = _signal_key(payload)

    # 1) Market filter (cheapest first)
    if not _is_market_open() and not payload.force_execute:
        return {
            "status": "rejected",
            "reason": "market_closed",
            "trace": _build_trace(payload, {"market_open": False}, ["market_closed"]),
        }

    # 2) Atomic idempotency — prevents the duplicate-dispatch race
    if not store.set(sig_key, "processing", ex=86400, nx=True):
        existing = store.get(sig_key)
        return {"status": "duplicate", "existing_status": existing, "signal_id": sig_key}

    try:
        # 2b) Protective close — bypasses entry filters/cooldown/sizing entirely.
        if payload.intent == "close" or (payload.order_action or "").upper() == "SELL_CLOSE":
            return _handle_close(payload, sig_key)

        # 3) Defensive filter (mirrors Rork-side gating)
        ok, blocked = _passes_filters(payload)
        if not ok:
            store.set(sig_key, "rejected", ex=86400)
            return {
                "status": "rejected",
                "reason": "; ".join(blocked),
                "trace": _build_trace(payload, {"market_open": True, "quality": False}, blocked),
            }

        # 4) Ticker cooldown
        cooldown_key = f"cooldown:{payload.ticker}"
        if store.exists(cooldown_key):
            store.set(sig_key, "cooldown", ex=86400)
            return {
                "status": "cooldown",
                "reason": "ticker_in_cooldown",
                "trace": _build_trace(payload, {"market_open": True, "cooldown": True}, ["cooldown"]),
            }
        store.set(cooldown_key, "1", ex=TICKER_COOLDOWN_MINUTES * 60)

        # 5) Risk-based sizing — live mode FAILS CLOSED when equity is
        # unavailable (never falls back to the ACCOUNT_SIZE default).
        if payload.mode == "live":
            live_equity = _live_equity()
            if live_equity is None or live_equity <= 0:
                store.set(sig_key, "rejected", ex=86400)
                reason = "live equity unavailable — refusing to size from defaults (fail-closed)"
                return {
                    "status": "rejected",
                    "reason": reason,
                    "trace": _build_trace(payload, {"sizing": False}, [reason]),
                }
            equity = live_equity
        else:
            equity = ACCOUNT_SIZE
        qty = _size_position(payload, equity)
        if qty < 1:
            store.set(sig_key, "rejected", ex=86400)
            reason = "invalid position size"
            return {
                "status": "rejected",
                "reason": reason,
                "trace": _build_trace(payload, {"sizing": False}, [reason]),
            }

        # 6) Option-chain alignment (day/weekly contract resolution)
        is_option = payload.instrument == "option"
        resolved_contract: Optional[dict] = None
        if is_option:
            try:
                resolved_contract = _resolve_option_contract(payload)
            except HTTPException as opt_exc:
                store.set(sig_key, "failed", ex=86400)
                return {
                    "status": "failed",
                    "reason": f"option_chain: {opt_exc.detail}",
                    "trace": _build_trace(payload, {"option_chain": False}, [str(opt_exc.detail)]),
                }
            # Override quantity with contract count (each contract = 100 shares).
            option_qty = max(1, payload.option_contracts or (qty // 100))
        else:
            option_qty = qty

        # 7) Execution
        is_paper = payload.mode == "paper" or (payload.mode != "live" and PAPER_MODE_DEFAULT)
        if is_paper:
            with _lock:
                _open_positions[payload.ticker] = {
                    "entry": payload.entry, "stop": payload.stop,
                    "target": payload.target, "qty": option_qty if is_option else qty,
                    "ts": _utcnow().isoformat(),
                    "contract": resolved_contract,
                }
                globals()["_trades_today"] = _trades_today + 1
                _save_state()
            order = {
                "mode": "paper",
                "filled": True,
                "entry": payload.entry,
                "shares": qty,
                "instrument": "option" if is_option else "equity",
                "contract": resolved_contract,
                "contracts": option_qty if is_option else None,
            }
            log.info(
                "[PAPER] %s %s %s qty=%d",
                payload.action, payload.ticker,
                resolved_contract["occ_symbol"] if resolved_contract else "equity",
                option_qty if is_option else qty,
            )
        else:
            try:
                if is_option and resolved_contract is not None:
                    etrade_resp = _place_option_order(payload, resolved_contract, option_qty)
                else:
                    etrade_resp = _place_bracket(payload, qty)
            except HTTPException:
                raise
            except Exception as e:  # noqa: BLE001
                store.set(sig_key, "failed", ex=86400)
                log.error("E*TRADE error: %s", e, exc_info=True)
                return {
                    "status": "failed",
                    "reason": f"etrade_error: {e}",
                    "trace": _build_trace(payload, {"broker": False}, [str(e)]),
                }
            with _lock:
                _open_positions[payload.ticker] = {
                    "entry": payload.entry, "stop": payload.stop,
                    "target": payload.target, "qty": option_qty if is_option else qty,
                    "ts": _utcnow().isoformat(),
                    "etrade": etrade_resp,
                    "contract": resolved_contract,
                }
                globals()["_trades_today"] = _trades_today + 1
                _save_state()
            order = {
                "mode": "live",
                "filled": True,
                "shares": qty,
                "etrade": etrade_resp,
                "instrument": "option" if is_option else "equity",
                "contract": resolved_contract,
                "contracts": option_qty if is_option else None,
            }

        # 8) Success path
        store.set(sig_key, "executed", ex=86400)
        return {
            "status": "executed",
            "signal_id": sig_key,
            "position_size": option_qty if is_option else qty,
            "mode": "paper" if is_paper else "live",
            "instrument": "option" if is_option else "equity",
            "contract": resolved_contract,
            "order": order,
            "trace": _build_trace(
                payload,
                checks={
                    "market_open": True, "quality": True, "risk": True,
                    "cooldown": False, "option_chain": is_option, "broker": True,
                },
                reasons=[],
            ),
        }

    except HTTPException as http_exc:
        store.set(sig_key, "rejected", ex=86400)
        return {
            "status": "rejected",
            "reason": http_exc.detail,
            "trace": _build_trace(payload, {"quality": False}, [str(http_exc.detail)]),
        }
    except Exception as e:  # noqa: BLE001
        store.set(sig_key, "failed", ex=86400)
        log.error("unexpected error: %s", e, exc_info=True)
        raise HTTPException(500, "internal execution error")


@app.post("/kill")
async def kill(x_rork_secret: Optional[str] = Header(default=None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    global _killed
    with _lock:
        _killed = True
        _save_state()
    log.warning("KILL SWITCH activated")
    return {"status": "killed", "open_positions": list(_open_positions.keys())}


@app.post("/resume")
async def resume(x_rork_secret: Optional[str] = Header(default=None, alias="X-Rork-Secret")):
    if WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(401, "invalid secret")
    global _killed
    with _lock:
        _killed = False
        _save_state()
    return {"status": "resumed"}


@app.get("/etrade/quote")
async def etrade_quote(symbols: str = ""):
    """Return real-time E*TRADE quotes for the requested symbols so the Rork
    app can render the watchlist, sparklines and chart overlays using the same
    feed it will actually trade against. Comma-separated list, max 25 symbols
    per E*TRADE's limit.

    Response shape (per symbol):
        {
          "symbol": "AAPL",
          "price": 247.18,            # All.lastTrade
          "previous_close": 245.10,
          "change": 2.08,
          "change_percent": 0.85,
          "open": 245.50,
          "high": 247.90,
          "low": 245.00,
          "volume": 41230112,
          "bid": 247.17,
          "ask": 247.19,
          "market_cap": 3720000000000,
          "name": "APPLE INC COM",
          "quote_status": "REALTIME"
        }
    """
    raw = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not raw:
        raise HTTPException(400, "symbols query param required")
    if len(raw) > 25:
        raw = raw[:25]

    try:
        market = _ensure_market_client()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"market client unavailable: {e}")

    try:
        resp = market.get_quote(raw, resp_format="json", detail_flag="ALL")
    except Exception as e:  # noqa: BLE001
        log.warning("etrade quote fetch failed: %s", e)
        raise HTTPException(502, f"quote fetch failed: {e}")

    quote_root = (resp or {}).get("QuoteResponse", {}) if isinstance(resp, dict) else {}
    raw_data = quote_root.get("QuoteData") or []
    if isinstance(raw_data, dict):
        raw_data = [raw_data]

    out: list[dict] = []
    for q in raw_data:
        if not isinstance(q, dict):
            continue
        product = q.get("Product") or {}
        all_data = q.get("All") or {}
        sym = str(product.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            price = float(all_data.get("lastTrade") or 0)
            prev_close = float(all_data.get("previousClose") or 0)
            change = float(all_data.get("changeClose") or (price - prev_close if price and prev_close else 0))
            change_pct = float(all_data.get("changeClosePercentage") or ((change / prev_close * 100) if prev_close else 0))
            open_p = float(all_data.get("open") or 0)
            high_p = float(all_data.get("high") or 0)
            low_p = float(all_data.get("low") or 0)
            volume = int(float(all_data.get("totalVolume") or 0))
            bid = float(all_data.get("bid") or 0)
            ask = float(all_data.get("ask") or 0)
            mcap = float(all_data.get("marketCap") or 0)
        except (TypeError, ValueError):
            continue
        out.append({
            "symbol": sym,
            "price": price,
            "previous_close": prev_close,
            "change": change,
            "change_percent": change_pct,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "volume": volume,
            "bid": bid,
            "ask": ask,
            "market_cap": mcap,
            "name": str(all_data.get("companyName") or product.get("symbol") or sym),
            "quote_status": str(q.get("quoteStatus") or all_data.get("quoteStatus") or ""),
        })

    return {"env": ETRADE_ENV, "count": len(out), "quotes": out}


@app.get("/status")
async def status():
    with _lock:
        _reset_daily()
    return {
        "killed": _killed,
        "env": ETRADE_ENV,
        "market_open": _is_market_open(),
        "open_positions": _open_positions,
        "stop_guards": _stop_guards,
        "trades_today": _trades_today,
        "realized_pnl_today_pct": _realized_pnl_today_pct,
        "state_file": str(STATE_FILE),
        "filters": {
            "min_score": MIN_SCORE,
            "min_score_trending": MIN_SCORE_TRENDING,
            "min_rvol": MIN_RVOL,
            "min_mtf": MIN_MTF,
            "allowed_setups": sorted(ALLOWED_SETUPS),
        },
    }


# ---------------------------------------------------------------------------
# Boot: restore persisted state and resume any interrupted stop-guards
# ---------------------------------------------------------------------------
_load_state()
_resume_guards()
