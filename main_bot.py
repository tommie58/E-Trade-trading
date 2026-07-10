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
from fastapi import FastAPI, HTTPException, Body, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional, Dict
import pyetrade
import os
import json
import math
import logging
import uuid
import asyncio
from datetime import datetime, date
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
BOT_VERSION = "3.0.0-price-sanitizer"

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
          order_action = "BUY_OPEN" if action == "BUY" else "SELL_CLOSE"
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
          return {"status": "success", "response": final}

      else:
          # EQUITY ORDER
          quantity = int(payload.get("position_size_shares") or 1)
          limit_price = payload.get("limit_price")
          order_action = "BUY" if action == "BUY" else "SELL"

          common = dict(
              resp_format="json",
              accountIdKey=account_id_key,
              symbol=ticker,
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
          logger.info(f"✅ LIVE EQUITY TRADE SUCCESS: {action} {quantity} {ticker}")
          consecutive_failures = 0
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
  payload: WebhookPayload = Body(...),
  x_rork_secret: Optional[str] = Header(None, alias="X-Rork-Secret"),
):
  # The app sends the shared secret both in the body (`secret`) and in the
  # X-Rork-Secret header — accept either.
  if WEBHOOK_SECRET and payload.secret != WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
      raise HTTPException(403, "Unauthorized")
  job = {"payload": payload.dict()}
  if redis:
      try:
          await redis.rpush(QUEUE_KEY, json.dumps(job))
          return {"status": "queued"}
      except Exception as e:
          logger.warning(f"Redis push failed, processing directly: {e}")
  try:
      result = await execute_live_order(payload.dict())
      return {"status": "processed_directly", "result": result}
  except Exception as e:
      logger.error(f"Direct processing failed: {e}")
      return {"status": "error", "message": str(e)}

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
  }

if __name__ == "__main__":
  import uvicorn
  port = int(os.getenv("PORT", 8000))
  uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
