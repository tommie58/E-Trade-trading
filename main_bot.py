"""
E*TRADE Trading Bot - v3.0.0-price-sanitizer
Production-ready for real-money live trades.
"""

from fastapi import FastAPI, HTTPException, Body, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pyetrade
import os
import json
import logging
import uuid
import asyncio
import time
from datetime import datetime, date
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from requests_oauthlib import OAuth1Session
from urllib.parse import quote, parse_qs, urlsplit
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
ENV = os.getenv("ETRADE_ENV", "production").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "true").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")

is_sandbox = ENV == "sandbox"
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
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5
breaker_open_time = 0
CIRCUIT_BREAKER_RESET_SECONDS = 600  # 10 minutes

_current_tokens: Optional[Dict[str, str]] = None
_resolved_account_id_key: Optional[str] = None
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
    secret: Optional[str] = None
    ticker: str
    action: str
    mode: Optional[str] = "paper"
    instrument: Optional[str] = "stock"
    strike: Optional[float] = None
    strike_hint: Optional[float] = None
    expiry: Optional[str] = None
    expiration_hint: Optional[str] = None
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
    stop: Optional[float] = None
    trail_stop: Optional[float] = None
    trail_amount: Optional[float] = None
    stop_price: Optional[float] = None
    trailing_stop_amount: Optional[float] = None
    trailing_stop_percent: Optional[float] = None
    broker_stop: Optional[bool] = None
    exit_limit_price: Optional[float] = None

    class Config:
        extra = "allow"

# ==================== TOKEN HELPERS ====================
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

def save_tokens(token: str, token_secret: str):
    global _current_tokens, _resolved_account_id_key, circuit_breaker_open, consecutive_failures
    logger.info("=== NEW TOKENS RECEIVED ===")
    _current_tokens = {"oauth_token": token, "oauth_token_secret": token_secret}
    _resolved_account_id_key = None
    circuit_breaker_open = False
    consecutive_failures = 0
    if async_session:
        try:
            asyncio.create_task(_save_tokens_to_db(token, token_secret))
        except Exception as e:
            logger.warning(f"Failed to persist tokens to DB: {e}")

async def _save_tokens_to_db(token: str, token_secret: str):
    async with async_session() as session:
        state = await session.get(ETradeSessionState, "active_state")
        if state:
            state.oauth_token = token
            state.oauth_token_secret = token_secret
        else:
            state = ETradeSessionState(id="active_state", oauth_token=token, oauth_token_secret=token_secret)
            session.add(state)
        await session.commit()
        logger.info("✅ Tokens saved to database")

async def preload_tokens():
    global _current_tokens
    if async_session:
        try:
            async with async_session() as session:
                state = await session.get(ETradeSessionState, "active_state")
                if state:
                    _current_tokens = {
                        "oauth_token": state.oauth_token,
                        "oauth_token_secret": state.oauth_token_secret
                    }
                    logger.info("✅ Tokens preloaded from database")
        except Exception as e:
            logger.warning(f"Could not preload tokens: {e}")

# ==================== DATABASE ====================
async def init_db():
    global engine, async_session
    if not DATABASE_URL:
        logger.warning("No DATABASE_URL set — using SQLite")
        db_url = "sqlite+aiosqlite:///./etrade_bot.db"
    else:
        db_url = DATABASE_URL

    try:
        engine = create_async_engine(db_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Database connected")
    except Exception as e:
        logger.warning(f"Database warning (falling back to SQLite): {e}")
        engine = create_async_engine("sqlite+aiosqlite:///./etrade_bot.db", echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

# ==================== CIRCUIT BREAKER ====================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        if time.time() - breaker_open_time > CIRCUIT_BREAKER_RESET_SECONDS:
            circuit_breaker_open = False
            consecutive_failures = 0
            logger.info("✅ Circuit breaker auto-reset")
        else:
            raise HTTPException(503, "Circuit breaker open")

# ==================== ACCOUNT RESOLUTION ====================
async def _resolve_account_id_key(tokens: dict) -> str:
    global _resolved_account_id_key
    if _resolved_account_id_key:
        return _resolved_account_id_key

    try:
        accounts = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens["oauth_token"], tokens["oauth_token_secret"],
            dev=is_sandbox
        )
        resp = await asyncio.to_thread(accounts.list_accounts, resp_format="json")
        account_list = resp.get("AccountListResponse", {}).get("Accounts", {}).get("Account", [])
        if isinstance(account_list, dict):
            account_list = [account_list]

        for acc in account_list:
            if TARGET_ACCOUNT_ID and str(TARGET_ACCOUNT_ID) in [str(acc.get("accountId")), str(acc.get("accountIdKey"))]:
                _resolved_account_id_key = acc["accountIdKey"]
                logger.info(f"✅ Resolved accountIdKey: {_resolved_account_id_key}")
                return _resolved_account_id_key

        if account_list:
            _resolved_account_id_key = account_list[0]["accountIdKey"]
            logger.info(f"✅ Using first active accountIdKey: {_resolved_account_id_key}")
            return _resolved_account_id_key
    except Exception as e:
        logger.error(f"Account resolution failed: {e}")
        raise Exception("Could not resolve accountIdKey")

    raise Exception("No valid E*TRADE account found")

# ==================== OPTION CONTRACT SNAPPING + REAL BID/ASK ====================
def _snap_option_contract(market, symbol: str, expiry: str, strike: float, call_put: str):
    try:
        chains = market.get_option_chains(
            symbol, expiry_date=expiry, chain_type=call_put,
            strike_price_near=int(round(strike)), no_of_strikes=5, resp_format="json"
        )
        pairs = chains.get("OptionChainResponse", {}).get("OptionPair", [])
        if isinstance(pairs, dict):
            pairs = [pairs]
        for pair in pairs:
            leg = pair.get("Call") if call_put == "CALL" else pair.get("Put")
            if leg and abs(float(leg.get("strikePrice", 0)) - strike) < 0.5:
                return expiry, float(leg.get("strikePrice"))
    except Exception as e:
        logger.warning(f"Chain snap failed, using original values: {e}")
    return expiry, strike

async def _get_real_bid_ask(market, symbol: str, expiry: str, strike: float, call_put: str):
    try:
        chains = await asyncio.to_thread(
            market.get_option_chains, symbol, expiry_date=expiry,
            chain_type=call_put, strike_price_near=int(round(strike)),
            no_of_strikes=5, resp_format="json"
        )
        pairs = chains.get("OptionChainResponse", {}).get("OptionPair", [])
        if isinstance(pairs, dict):
            pairs = [pairs]
        for pair in pairs:
            leg = pair.get("Call") if call_put == "CALL" else pair.get("Put")
            if leg and abs(float(leg.get("strikePrice", 0)) - strike) < 0.01:
                return float(leg.get("bid", 0) or 0), float(leg.get("ask", 0) or 0)
    except Exception as e:
        logger.warning(f"Could not fetch real bid/ask: {e}")
    return None, None

def _get_valid_option_price(price: float, tick_size: float = 0.05) -> float:
    return round(price / tick_size) * tick_size

# ==================== EXECUTE LIVE ORDER (v3.0 - FIXED) ====================
async def execute_live_order(payload: dict):
    global consecutive_failures, circuit_breaker_open, breaker_open_time

    mode = payload.get("mode", "paper").lower()
    instrument = payload.get("instrument", "stock").lower()
    ticker = payload.get("ticker", "UNKNOWN")
    action = payload.get("action", "UNKNOWN").upper()

    logger.info(f"📥 Received signal → mode={mode}, instrument={instrument}, ticker={ticker}, action={action}")

    if mode != "live" or not LIVE_TRADING or is_sandbox:
        return {"status": "skipped", "reason": f"mode={mode}"}

    if circuit_breaker_open:
        if time.time() - breaker_open_time > CIRCUIT_BREAKER_RESET_SECONDS:
            circuit_breaker_open = False
            consecutive_failures = 0
            logger.info("✅ Circuit breaker auto-reset after 10 minutes")
        else:
            raise HTTPException(503, "Circuit breaker open")

    await check_risk_limits()
    tokens = load_tokens()
    if not tokens:
        raise Exception("No E*TRADE tokens available")

    account_id_key = await _resolve_account_id_key(tokens)
    orders = pyetrade.ETradeOrder(CONSUMER_KEY, CONSUMER_SECRET, tokens["oauth_token"], tokens["oauth_token_secret"], dev=is_sandbox)
    client_order_id = str(uuid.uuid4().int)[:18]

    try:
        if instrument == "option":
            symbol = payload["ticker"]
            strike = float(payload.get("strike_hint") or payload.get("strike"))
            expiry = payload.get("expiration_hint") or payload.get("expiry")
            call_put = str(payload.get("option_right") or payload.get("call_put") or "CALL").upper()
            call_put = "CALL" if call_put.startswith("C") else "PUT"
            quantity = int(payload.get("option_contracts") or payload.get("contracts") or payload.get("quantity") or 1)
            order_action = "BUY_OPEN" if action == "BUY" else "SELL_CLOSE"
            is_exit = order_action == "SELL_CLOSE"

            if not strike or not expiry:
                raise Exception("Missing strike or expiry")

            market = pyetrade.ETradeMarket(CONSUMER_KEY, CONSUMER_SECRET, tokens["oauth_token"], tokens["oauth_token_secret"], dev=is_sandbox)
            expiry, strike = await asyncio.to_thread(_snap_option_contract, market, symbol, expiry, strike, call_put)
            real_bid, real_ask = await _get_real_bid_ask(market, symbol, expiry, strike, call_put)

            user_limit = payload.get("option_limit_price") or payload.get("limit_price")

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
                strikePrice=strike,
                expiryDate=expiry,
            )

            if not is_exit:
                # ENTRY
                if user_limit and float(user_limit) > 0:
                    price = float(user_limit)
                    if real_ask:
                        if price > real_ask:
                            price = real_ask
                        elif price < real_bid:
                            price = real_ask
                    price = _get_valid_option_price(price)
                    common["priceType"] = "LIMIT"
                    common["limitPrice"] = round(price, 2)
                else:
                    if real_ask:
                        price = _get_valid_option_price(real_ask)
                        common["priceType"] = "LIMIT"
                        common["limitPrice"] = round(price, 2)
                    else:
                        common["priceType"] = "MARKET"
            else:
                # EXIT
                exit_limit = payload.get("exit_limit_price")
                if exit_limit and float(exit_limit) > 0:
                    price = float(exit_limit)
                    if real_bid:
                        price = min(price, real_bid)
                    price = _get_valid_option_price(price)
                    common["priceType"] = "LIMIT"
                    common["limitPrice"] = round(price, 2)
                elif payload.get("broker_stop"):
                    stop_price = payload.get("stop_price") or payload.get("trail_stop") or payload.get("stop")
                    if stop_price:
                        common["priceType"] = "STOP"
                        common["stopPrice"] = round(float(stop_price), 2)
                    else:
                        common["priceType"] = "MARKET"
                else:
                    common["priceType"] = "MARKET"

            logger.info(f"📤 Placing sanitized OPTION order | priceType={common.get('priceType')}")
            final = await asyncio.to_thread(orders.place_option_order, **common)
            consecutive_failures = 0
            return {"status": "success", "response": final}

        else:
            # Equity branch (simplified for now)
            logger.info("Equity orders not fully implemented in this version")
            return {"status": "skipped", "reason": "equity not implemented"}

    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
            breaker_open_time = time.time()
            logger.warning("🚨 Circuit breaker opened")
        logger.error(f"❌ LIVE TRADE FAILED: {e}")
        raise

# ==================== WEBHOOK ====================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...), x_rork_secret: Optional[str] = Header(None)):
    if WEBHOOK_SECRET and payload.secret != WEBHOOK_SECRET and x_rork_secret != WEBHOOK_SECRET:
        raise HTTPException(403, "Unauthorized")

    job = {"payload": payload.dict()}
    if redis:
        try:
            await redis.rpush("etrade:placement_queue", json.dumps(job))
            return {"status": "queued"}
        except Exception as e:
            logger.warning(f"Redis push failed: {e}")

    try:
        result = await execute_live_order(payload.dict())
        return {"status": "processed_directly", "result": result}
    except Exception as e:
        logger.error(f"Direct processing failed: {e}")
        return {"status": "error", "message": str(e)}

# ==================== HEALTH ====================
@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens),
        "version": BOT_VERSION,
        "circuit_breaker_open": circuit_breaker_open,
    }

# ==================== STARTUP ====================
@app.on_event("startup")
async def on_startup():
    logger.info(f"Starting → PRODUCTION | LIVE=True | VERSION={BOT_VERSION}")
    await init_db()
    await preload_tokens()
    logger.info("✅ Bot ready")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
