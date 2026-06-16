from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional, Dict
import pyetrade
import os
import json
import logging
import uuid
import time
import asyncio
from datetime import datetime
import aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
ENV = os.getenv("ETRADE_ENV", "sandbox").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
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
circuit_breaker_open = False
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5
QUEUE_KEY = "etrade:placement_queue"
_worker_task = None
_worker_stop = False

Base = declarative_base()

# ==================== MODELS ====================
class WebhookPayload(BaseModel):
    secret: str
    ticker: str
    action: str
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
        if v.upper() not in {"BUY", "SELL", "EXIT", "CLOSE"}:
            raise ValueError("Invalid action")
        return v.upper()

# ==================== TOKEN LOADER ====================
def load_tokens():
    token = os.getenv("ETRADE_ACCESS_TOKEN")
    token_secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
    if token and token_secret:
        return {"oauth_token": token, "oauth_token_secret": token_secret}
    logger.error("Missing ETRADE_ACCESS_TOKEN or ETRADE_ACCESS_TOKEN_SECRET")
    return None

# ==================== DB ====================
async def init_db():
    global engine, async_session
    if not DATABASE_URL:
        return
    engine = create_async_engine(DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ==================== SAFETY ====================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open")

async def alert_admin(subject: str, body: str):
    if ALERT_WEBHOOK_URL:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                await s.post(ALERT_WEBHOOK_URL, json={"text": f"**{subject}**\n{body}"})
        except:
            pass

# ==================== LIVE ORDER EXECUTION ====================
async def execute_live_order(payload: dict):
    # Only allow real trades when BOTH conditions are true
    if not LIVE_TRADING or is_sandbox:
        logger.warning("Skipping real trade (LIVE_TRADING or sandbox mode)")
        return {"status": "skipped"}

    await check_risk_limits()

    tokens = load_tokens()
    if not tokens:
        raise AuthInvalidError("Tokens not found in environment variables")

    orders = pyetrade.ETradeOrder(
        os.getenv("ETRADE_CONSUMER_KEY"),
        os.getenv("ETRADE_CONSUMER_SECRET"),
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=is_sandbox          # False = production when ENV=production
    )

    instrument = payload.get("instrument", "stock").lower()
    action = payload["action"]
    ticker = payload["ticker"]
    account_id = TARGET_ACCOUNT_ID
    client_order_id = str(uuid.uuid4())[:20]

    try:
        if instrument == "option":
            # ... (keep your option logic here)
            pass
        else:
            # STOCK
            quantity = payload.get("position_size_shares", 1)
            price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
            limit_price = payload.get("limit_price")

            order_action = "BUY" if action == "BUY" else "SELL"

            await asyncio.to_thread(
                orders.place_equity_order,
                resp_format="json",
                accountId=account_id,
                symbol=ticker,
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType=price_type,
                limitPrice=limit_price,
                quantity=quantity,
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )

        logger.info(f"✅ LIVE TRADE EXECUTED: {ticker}")
        return {"status": "success"}

    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
        logger.error(f"Trade failed: {e}")
        raise

# ==================== WORKER ====================
async def placement_worker():
    while not _worker_stop:
        try:
            job = await redis.lpop(QUEUE_KEY)
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

# ==================== STARTUP ====================
@app.on_event("startup")
async def startup():
    global redis
    logger.info(f"Mode: {ENV} | Live Trading: {LIVE_TRADING}")

    if REDIS_URL:
        redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    await init_db()
    await start_worker()

@app.on_event("shutdown")
async def shutdown():
    global _worker_stop
    _worker_stop = True

# ==================== ENDPOINTS ====================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(403, "Unauthorized")

    await check_risk_limits()

    job = {"payload": payload.dict()}
    await redis.rpush(QUEUE_KEY, json.dumps(job))
    return {"status": "queued"}

@app.get("/health")
async def health():
    return {"status": "ok", "live_trading": LIVE_TRADING, "env": ENV}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
