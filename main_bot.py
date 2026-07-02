from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pyetrade
import os
import json
import logging
import uuid
import asyncio
from datetime import datetime
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
ENV = os.getenv("ETRADE_ENV", "production").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "true").lower() == "true"
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

is_sandbox = ENV == "sandbox"

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
QUEUE_KEY = "etrade:placement_queue"
_worker_task = None
_worker_stop = False
_current_tokens: Dict[str, str] = {}   # In-memory token cache

Base = declarative_base()

class ETradeSessionState(Base):
    __tablename__ = "etrade_session_state"
    id = Column(String, primary_key=True, default="current")
    access_token = Column(Text)
    access_token_secret = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow)

# ==================== HELPERS ====================
def _parse_expiry_to_str(expiry) -> str:
    """Convert expiry to YYYY-MM-DD string for E*TRADE"""
    if isinstance(expiry, str):
        return expiry
    if isinstance(expiry, dict):
        return f"{expiry.get('year', 2026)}-{str(expiry.get('month', 7)).zfill(2)}-{str(expiry.get('day', 3)).zfill(2)}"
    return str(expiry)

def load_tokens() -> Optional[Dict[str, str]]:
    if _current_tokens:
        return _current_tokens
    # Try environment variables first
    token = os.getenv("ETRADE_ACCESS_TOKEN")
    secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
    if token and secret:
        return {"oauth_token": token, "oauth_token_secret": secret}
    return None

def save_tokens(tokens: Dict[str, str]):
    global _current_tokens
    _current_tokens = tokens.copy()
    logger.info("✅ Tokens saved to memory cache")

# ==================== DATABASE ====================
async def init_db():
    global engine, async_session
    try:
        if not DATABASE_URL:
            logger.warning("No DATABASE_URL set — using SQLite")
            db_url = "sqlite+aiosqlite:///./etrade_tokens.db"
        else:
            db_url = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")

        engine = create_async_engine(db_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("✅ Database connected")
    except Exception as e:
        logger.error(f"Database error: {e}")

# ==================== ORDER EXECUTION ====================
async def execute_live_order(payload: dict):
    global consecutive_failures

    tokens = load_tokens()
    if not tokens:
        raise Exception("No E*TRADE tokens available")

    if not TARGET_ACCOUNT_ID:
        raise Exception("TARGET_ACCOUNT_ID is not set in environment variables")

    ticker = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    instrument = payload.get("instrument", "stock").lower()
    mode = payload.get("mode", "paper").lower()
    quantity = int(payload.get("quantity", 1))

    client_order_id = str(uuid.uuid4())[:20]

    logger.info(f"📥 Received signal → mode={mode}, instrument={instrument}, ticker={ticker}, action={action}")

    if mode != "live":
        logger.info("Paper mode — skipping real order")
        return {"status": "paper", "message": "Paper mode"}

    try:
        if instrument == "option":
            strike = payload.get("strike") or payload.get("strike_hint")
            expiry = payload.get("expiry") or payload.get("expiration_hint") or payload.get("expiry_date")

            if not strike or not expiry:
                raise Exception(f"Missing strike or expiry. Got strike={strike}, expiry={expiry}")

            call_put = "CALL" if payload.get("call_put", "call").lower() == "call" else "PUT"
            order_action = "BUY_OPEN" if action == "BUY" else "SELL_CLOSE"
            expiry_str = _parse_expiry_to_str(expiry)
            strike_price = int(float(strike))   # E*TRADE prefers int for whole numbers

            logger.info(f"🚀 LIVE OPTION ORDER: {order_action} {quantity} {ticker} {call_put} {strike_price} {expiry_str}")

            orders = pyetrade.ETradeOrder(
                consumer_key=CONSUMER_KEY,
                consumer_secret=CONSUMER_SECRET,
                resource_token=tokens['oauth_token'],
                resource_token_secret=tokens['oauth_token_secret'],
                dev=False
            )

            final = await asyncio.to_thread(
                orders.place_option_order,
                resp_format="json",
                accountIdKey=TARGET_ACCOUNT_ID,
                symbol=ticker,
                callPut=call_put,
                expiryDate=expiry_str,
                strikePrice=strike_price,
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType="MARKET",
                allOrNone=False,
                quantity=quantity,
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )
            logger.info(f"✅ LIVE OPTION ORDER PLACED: {final}")
            return {"status": "success", "result": final}

        else:
            # Equity order
            logger.info(f"🚀 LIVE EQUITY ORDER: {action} {quantity} {ticker}")

            orders = pyetrade.ETradeOrder(
                consumer_key=CONSUMER_KEY,
                consumer_secret=CONSUMER_SECRET,
                resource_token=tokens['oauth_token'],
                resource_token_secret=tokens['oauth_token_secret'],
                dev=False
            )

            final = await asyncio.to_thread(
                orders.place_equity_order,
                resp_format="json",
                accountIdKey=TARGET_ACCOUNT_ID,
                symbol=ticker,
                orderAction=action,
                clientOrderId=client_order_id,
                priceType="MARKET",
                quantity=quantity,
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )
            logger.info(f"✅ LIVE EQUITY ORDER PLACED: {final}")
            return {"status": "success", "result": final}

    except Exception as e:
        consecutive_failures += 1
        logger.error(f"❌ LIVE TRADE FAILED: {e}")
        raise

# ==================== WEBHOOK ====================
class WebhookPayload(BaseModel):
    secret: str
    ticker: str
    action: str
    mode: Optional[str] = "paper"
    instrument: Optional[str] = "stock"
    quantity: Optional[int] = 1
    strike: Optional[float] = None
    expiry: Optional[str] = None
    call_put: Optional[str] = None

@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(403, "Unauthorized")

    try:
        result = await execute_live_order(payload.dict())
        return {"status": "processed", "result": result}
    except Exception as e:
        return {"status": "failed", "message": str(e)}

# ==================== AUTH ENDPOINTS ====================
@app.post("/etrade/auth/start")
async def start_linking():
    try:
        oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
        authorize_url = oauth.get_authorized_url()
        logger.info("✅ E*TRADE auth URL generated successfully")
        return {"authorize_url": authorize_url}
    except Exception as e:
        logger.error(f"Start linking failed: {e}")
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_linking(verifier: str = Body(..., embed=True)):
    try:
        oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
        tokens = oauth.get_access_token(verifier)
        save_tokens(tokens)
        logger.info("=== NEW TOKENS RECEIVED ===")
        logger.info(f"ETRADE_ACCESS_TOKEN={tokens['oauth_token']}")
        logger.info(f"ETRADE_ACCESS_TOKEN_SECRET={tokens['oauth_token_secret']}")
        return {"status": "success", "message": "Tokens received. Add them to Railway Variables."}
    except Exception as e:
        logger.error(f"Complete link failed: {e}")
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    tokens = load_tokens()
    if not tokens:
        return {
            "status": "not_linked",
            "linked": False,
            "message": "No tokens found. Please link your E*TRADE account first."
        }

    try:
        accounts_client = pyetrade.ETradeAccounts(
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            resource_token=tokens['oauth_token'],
            resource_token_secret=tokens['oauth_token_secret'],
            dev=False
        )

        raw_response = accounts_client.list_accounts()

        account_list = []
        if raw_response and 'AccountListResponse' in raw_response:
            accounts_data = raw_response['AccountListResponse'].get('Accounts', {}).get('Account', [])
            if not isinstance(accounts_data, list):
                accounts_data = [accounts_data]
            for acc in accounts_data:
                account_list.append({
                    "accountIdKey": acc.get("accountIdKey"),
                    "accountId": acc.get("accountId"),
                    "accountType": acc.get("accountType"),
                    "accountDesc": acc.get("accountDesc", "")
                })

        return {
            "status": "linked",
            "linked": True,
            "accounts": account_list,
            "message": "Copy the accountIdKey you want to use and set it as ETRADE_ACCOUNT_ID in Railway"
        }

    except Exception as e:
        logger.error(f"Failed to fetch accounts: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/etrade/auth/renew")
async def renew_tokens():
    tokens = load_tokens()
    if not tokens:
        raise HTTPException(400, "No tokens to renew")

    try:
        auth_manager = pyetrade.authorization.ETradeAccessManager(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret']
        )
        auth_manager.renew_access_token()
        logger.info("✅ Token renewed successfully")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Renew failed: {e}")
        raise HTTPException(500, str(e))

@app.post("/etrade/disconnect")
async def disconnect():
    global _current_tokens
    _current_tokens = {}
    logger.info("User requested account disconnect")
    return {"status": "disconnected"}

# ==================== HEALTH & QUOTE ====================
@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens),
        "target_account_set": bool(TARGET_ACCOUNT_ID)
    }

@app.get("/etrade/quote")
async def get_quote(symbols: str = Query(...)):
    tokens = load_tokens()
    if not tokens:
        raise HTTPException(401, "Not linked")

    try:
        market = pyetrade.ETradeMarket(
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            oauth_token=tokens['oauth_token'],
            oauth_token_secret=tokens['oauth_token_secret'],
            dev=False
        )
        quotes = market.get_quote(symbols.split(","), resp_format="json")
        return quotes
    except Exception as e:
        logger.error(f"Quote failed: {e}")
        raise HTTPException(500, str(e))

# ==================== STARTUP ====================
@app.on_event("startup")
async def on_startup():
    logger.info(f"Starting → {'SANDBOX' if is_sandbox else 'PRODUCTION'} | LIVE={LIVE_TRADING}")

    if not CONSUMER_KEY or not CONSUMER_SECRET:
        logger.warning("⚠️ ETRADE_CONSUMER_KEY or SECRET not set")

    if not TARGET_ACCOUNT_ID:
        logger.warning("⚠️ TARGET_ACCOUNT_ID is not set — live orders will likely fail")

    if REDIS_URL:
        try:
            global redis
            redis = await redis_from_url(REDIS_URL, decode_responses=True)
            logger.info("✅ Redis connected")
        except Exception as e:
            logger.warning(f"No REDIS_URL set — running without Redis queue")

    await init_db()
    logger.info("✅ Bot ready")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
