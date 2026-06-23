from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional
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
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
ENV = os.getenv("ETRADE_ENV", "sandbox").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")

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

Base = declarative_base()

class ETradeSessionState(Base):
    __tablename__ = "etrade_session_state"
    id = Column(String(50), primary_key=True, default="active_state")
    oauth_token = Column(Text, nullable=False)
    oauth_token_secret = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# ==================== MODELS ====================
class WebhookPayload(BaseModel):
    secret: str
    ticker: str
    action: str
    mode: Optional[str] = "paper"
    instrument: Optional[str] = "stock"
    strike: Optional[float] = None
    strike_hint: Optional[float] = None
    expiry: Optional[str] = None
    expiration_hint: Optional[str] = None
    option_right: Optional[str] = None
    contracts: Optional[int] = None
    option_contracts: Optional[int] = None
    limit_price: Optional[float] = None
    position_size_shares: Optional[int] = None

    @validator("action")
    def validate_action(cls, v):
        if str(v).upper() not in {"BUY", "SELL", "EXIT", "CLOSE"}:
            raise ValueError("Invalid action")
        return str(v).upper()

# ==================== TOKEN HELPERS ====================
def load_tokens():
    token = os.getenv("ETRADE_ACCESS_TOKEN")
    secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
    if token and secret:
        return {"oauth_token": token, "oauth_token_secret": secret}
    return None

def save_tokens(token: str, token_secret: str):
    logger.info("=== NEW TOKENS RECEIVED ===")
    logger.info(f"ETRADE_ACCESS_TOKEN={token}")
    logger.info(f"ETRADE_ACCESS_TOKEN_SECRET={token_secret}")

# ==================== OAUTH ====================
REQUEST_TOKEN_URL = "https://api.etrade.com/oauth/request_token"
AUTHORIZE_URL = "https://us.etrade.com/e/t/etws/authorize"
ACCESS_TOKEN_URL = "https://api.etrade.com/oauth/access_token"

@app.api_route("/etrade/auth/start", methods=["GET", "POST"])
@app.api_route("/link", methods=["GET", "POST"])
async def etrade_auth_start():
    try:
        etrade_session = OAuth1Session(client_key=CONSUMER_KEY, client_secret=CONSUMER_SECRET, callback_uri="oob")
        fetch_response = etrade_session.fetch_request_token(REQUEST_TOKEN_URL)
        token_val = fetch_response.get("oauth_token")
        secret_val = fetch_response.get("oauth_token_secret")

        if not token_val or not secret_val:
            raise Exception("Failed to get request token from E*TRADE")

        auth_url = f"{AUTHORIZE_URL}?key={CONSUMER_KEY}&token={token_val}"

        if async_session:
            async with async_session() as session:
                async with session.begin():
                    state = ETradeSessionState(id="active_state", oauth_token=str(token_val), oauth_token_secret=str(secret_val))
                    await session.merge(state)

        return {"status": "success", "auth_url": auth_url, "authorize_url": auth_url}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

@app.post("/etrade/auth/complete")
@app.post("/complete-link")
async def etrade_auth_complete(data: dict = Body(...)):
    try:
        verifier = data.get("oauth_verifier") or data.get("verifier") or data.get("code")
        if not verifier:
            raise HTTPException(400, "Missing verification code")

        token_val, secret_val = None, None
        if async_session:
            async with async_session() as session:
                cached = await session.get(ETradeSessionState, "active_state")
                if cached:
                    token_val = cached.oauth_token
                    secret_val = cached.oauth_token_secret

        if not token_val or not secret_val:
            raise HTTPException(400, "No active request token found. Please start linking again.")

        etrade_session = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, resource_owner_key=token_val, resource_owner_secret=secret_val, verifier=verifier)
        access_tokens = etrade_session.fetch_access_token(ACCESS_TOKEN_URL)

        final_token = access_tokens.get("oauth_token")
        final_secret = access_tokens.get("oauth_token_secret")

        save_tokens(final_token, final_secret)
        return {"status": "success", "message": "E*TRADE account linked successfully"}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

# ==================== NEW: Disconnect Endpoint ====================
@app.post("/etrade/disconnect")
async def etrade_disconnect():
    logger.info("User requested account disconnect")
    # You can extend this later to clear tokens if needed
    return {"status": "success", "message": "Disconnect request received"}

# ==================== IMPROVED: Renew Endpoint ====================
@app.post("/etrade/auth/renew")
async def etrade_auth_renew(data: dict = Body(...)):
    try:
        access_token = data.get("access_token") or os.getenv("ETRADE_ACCESS_TOKEN")
        access_token_secret = data.get("access_token_secret") or os.getenv("ETRADE_ACCESS_TOKEN_SECRET")

        if not access_token or not access_token_secret:
            raise HTTPException(400, "Missing access tokens")

        # Try to validate current tokens first
        try:
            accounts = pyetrade.ETradeAccounts(CONSUMER_KEY, CONSUMER_SECRET, access_token, access_token_secret, dev=is_sandbox)
            await asyncio.to_thread(accounts.list_accounts, resp_format="json")
            return {"status": "success", "message": "Tokens are still valid", "renewed": False}
        except Exception:
            pass  # Tokens are invalid, try to renew

        auth_manager = pyetrade.ETradeAccessManager(CONSUMER_KEY, CONSUMER_SECRET, access_token, access_token_secret)
        renewed = await asyncio.to_thread(auth_manager.renew_access_token)

        if renewed:
            new_token = auth_manager.oauth_token
            new_secret = auth_manager.oauth_token_secret
            save_tokens(new_token, new_secret)
            return {"status": "success", "message": "Tokens renewed successfully", "renewed": True}
        else:
            raise HTTPException(400, "Token renewal failed")

    except Exception as e:
        raise HTTPException(500, detail=str(e))

# ==================== QUOTE ENDPOINT ====================
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

# ==================== ACCOUNT STATUS ====================
@app.get("/etrade/account")
async def get_etrade_account():
    tokens = load_tokens()
    if not tokens:
        return {"status": "not_linked", "linked": False}
    return {"status": "linked", "linked": True}

# ==================== DATABASE ====================
async def init_db():
    global engine, async_session

    use_postgres = False
    if DATABASE_URL and "postgres" in DATABASE_URL:
        try:
            import asyncpg
            use_postgres = True
        except ImportError:
            logger.warning("asyncpg not found — falling back to SQLite")
            use_postgres = False

    if use_postgres:
        target_url = DATABASE_URL
    else:
        target_url = "sqlite+aiosqlite:///etrade_cache.db"

    try:
        engine = create_async_engine(target_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("✅ Database connected")
    except Exception as e:
        logger.error(f"Database error: {e}")

# ==================== SAFETY ====================
async def check_risk_limits():
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open")

# ==================== LIVE TRADING (Respects mode) ====================
async def execute_live_order(payload: dict):
    mode = payload.get("mode", "paper").lower()

    if mode != "live" or not LIVE_TRADING or is_sandbox:
        return {"status": "skipped", "reason": f"mode={mode}"}

    await check_risk_limits()
    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")

    orders = pyetrade.ETradeOrder(CONSUMER_KEY, CONSUMER_SECRET, tokens["oauth_token"], tokens["oauth_token_secret"], dev=is_sandbox)

    ticker = payload["ticker"]
    action = payload["action"]
    client_order_id = str(uuid.uuid4())[:20]
    quantity = payload.get("position_size_shares", 1)
    price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
    limit_price = payload.get("limit_price")
    order_action = "BUY" if action == "BUY" else "SELL"

    order_payload = {
        "Order": [{
            "allOrNone": False,
            "priceType": price_type,
            "orderTerm": "GOOD_FOR_DAY",
            "marketSession": "REGULAR",
            "Instrument": [{
                "Product": {"securityType": "EQ", "symbol": ticker},
                "orderAction": order_action,
                "quantityType": "QUANTITY",
                "quantity": quantity
            }]
        }]
    }
    if limit_price:
        order_payload["Order"][0]["limitPrice"] = limit_price

    preview = await asyncio.to_thread(orders.preview_equity_order, resp_format="json", accountIdKey=TARGET_ACCOUNT_ID, order=order_payload, clientOrderId=client_order_id)
    preview_id = preview['PreviewOrderResponse']['PreviewIds']['PreviewId'][0]['previewId']

    final = await asyncio.to_thread(orders.place_equity_order, resp_format="json", accountIdKey=TARGET_ACCOUNT_ID, order=order_payload, clientOrderId=client_order_id, previewId=preview_id)
    logger.info(f"✅ LIVE TRADE EXECUTED: {ticker}")
    return {"status": "success", "response": final}

# ==================== WORKER ====================
async def placement_worker():
    while not _worker_stop:
        try:
            job = await redis.lpop(QUEUE_KEY) if redis else None
            if job:
                await execute_live_order(json.loads(job)["payload"])
            else:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(2)

async def start_worker():
    global _worker_task
    _worker_task = asyncio.create_task(placement_worker())

# ==================== STARTUP / SHUTDOWN ====================
@app.on_event("startup")
async def on_startup():
    global redis
    logger.info(f"Starting → {'SANDBOX' if is_sandbox else 'PRODUCTION'} | LIVE={LIVE_TRADING}")

    if REDIS_URL:
        try:
            redis = await redis_from_url(REDIS_URL, decode_responses=True)
        except Exception as e:
            logger.error(f"Redis failed: {e}")

    await init_db()
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
async def webhook(payload: WebhookPayload = Body(...)):
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(403, "Unauthorized")
    if not redis:
        return {"status": "error", "message": "Redis unavailable"}
    await redis.rpush(QUEUE_KEY, json.dumps({"payload": payload.dict()}))
    return {"status": "queued"}

@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens)
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
