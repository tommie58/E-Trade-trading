from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional, Dict, Any
import pyetrade
import os
import json
import logging
import uuid
import time
import traceback
import asyncio
from datetime import datetime
from pathlib import Path
import aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# CONFIG
# =========================================================
ENV = os.getenv("ETRADE_ENV", "sandbox").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "2"))
MAX_POSITION_VALUE = float(os.getenv("MAX_POSITION_VALUE", "10000"))
DAILY_LOSS_LIMIT_DOLLARS = float(os.getenv("DAILY_LOSS_LIMIT_DOLLARS", "-500"))
ENABLE_MARKET_HOURS_CHECK = os.getenv("ENABLE_MARKET_HOURS_CHECK", "false").lower() == "true"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

is_sandbox = ENV == "sandbox"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Auto-Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# =========================================================
# GLOBALS
# =========================================================
broker_down_until = 0
redis = None
engine = None
async_session = None
_worker_task = None
_worker_stop = False
QUEUE_KEY = "etrade:placement_queue"

circuit_breaker_open = False
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5

Base = declarative_base()

# =========================================================
# OAUTH
# =========================================================
oauth = pyetrade.ETradeOAuth(
    os.getenv("ETRADE_CONSUMER_KEY"),
    os.getenv("ETRADE_CONSUMER_SECRET")
)

# =========================================================
# PYDANTIC MODEL
# =========================================================
class WebhookPayload(BaseModel):
    secret: str
    ticker: str
    action: str
    instrument: Optional[str] = "stock"
    mode: Optional[str] = "paper"
    strike: Optional[float] = None
    strike_hint: Optional[float] = None
    expiration_hint: Optional[str] = None
    expiry: Optional[str] = None
    option_contracts: Optional[int] = None
    contracts: Optional[int] = None
    option_right: Optional[str] = None
    limit_price: Optional[float] = None
    entry: Optional[float] = None
    position_size_shares: Optional[int] = None

    @validator("action", pre=True, always=True)
    def action_must_be_valid(cls, v):
        if v is None:
            raise ValueError("action is required")
        allowed = {"BUY", "SELL", "EXIT", "CLOSE"}
        val = str(v).upper()
        if val not in allowed:
            raise ValueError("Invalid action")
        return val

    @validator("instrument", pre=True, always=True)
    def instrument_lower(cls, v):
        return (v or "stock").lower()

    @validator("mode", pre=True, always=True)
    def mode_lower(cls, v):
        return (v or "paper").lower()

# =========================================================
# EXCEPTIONS
# =========================================================
class TransientBrokerError(Exception):
    pass

class AuthInvalidError(Exception):
    pass

# =========================================================
# TOKEN LOADER (Simplified - No KMS)
# =========================================================
def load_tokens() -> Optional[Dict[str, str]]:
    """Load tokens from environment variables (recommended for Railway/Docker)"""
    access_token = os.getenv("ETRADE_ACCESS_TOKEN")
    access_token_secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")

    if access_token and access_token_secret:
        return {
            "oauth_token": access_token,
            "oauth_token_secret": access_token_secret
        }
    logger.error("ETRADE_ACCESS_TOKEN or ETRADE_ACCESS_TOKEN_SECRET not set")
    return None

# =========================================================
# DB INIT
# =========================================================
async def init_db():
    global engine, async_session
    if not DATABASE_URL:
        logger.warning("No DATABASE_URL set - running without DB")
        return
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    logger.info("✅ Database initialized")

# =========================================================
# SAFETY
# =========================================================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open - trading paused")

    today = datetime.utcnow().date().isoformat()
    current_loss = float(await redis.get(f"daily_loss:{today}") or 0)
    if current_loss <= DAILY_LOSS_LIMIT_DOLLARS:
        circuit_breaker_open = True
        await alert_admin("🚨 DAILY LOSS LIMIT BREACHED", f"Loss: ${current_loss}")
        raise HTTPException(503, "Daily loss limit reached")

async def record_trade_pnl(pnl: float):
    today = datetime.utcnow().date().isoformat()
    await redis.incrbyfloat(f"daily_loss:{today}", pnl)
    await redis.expire(f"daily_loss:{today}", 86400)

async def alert_admin(subject: str, body: str):
    if not ALERT_WEBHOOK_URL:
        logger.warning(f"ALERT: {subject} - {body}")
        return
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            await session.post(ALERT_WEBHOOK_URL, json={"text": f"**{subject}**\n{body}"})
    except Exception:
        logger.exception("Alert failed")

# =========================================================
# LIVE TRADING EXECUTION
# =========================================================
async def execute_live_order(payload: dict) -> dict:
    if not LIVE_TRADING or is_sandbox:
        return {"status": "skipped", "reason": "not_live_mode"}

    await check_risk_limits()

    instrument = payload.get("instrument", "stock").lower()
    action = payload["action"].upper()
    ticker = payload["ticker"]
    client_order_id = payload.get("client_order_id") or str(uuid.uuid4())[:20]
    account_id = TARGET_ACCOUNT_ID

    if not account_id:
        raise ValueError("ETRADE_ACCOUNT_ID is required")

    tokens = load_tokens()
    if not tokens:
        raise AuthInvalidError("Failed to load tokens from environment variables")

    orders_client = pyetrade.ETradeOrder(
        os.getenv("ETRADE_CONSUMER_KEY"),
        os.getenv("ETRADE_CONSUMER_SECRET"),
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=is_sandbox
    )

    try:
        if instrument == "option":
            strike = payload.get("strike") or payload.get("strike_hint")
            expiry = payload.get("expiry") or payload.get("expiration_hint")
            right = (payload.get("option_right") or "C").upper()[0]
            quantity = payload.get("contracts") or payload.get("option_contracts") or 1

            order_action = "BUY_OPEN" if action in ("BUY", "BUY_OPEN") else "SELL_CLOSE"
            price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
            limit_price = payload.get("limit_price") or payload.get("entry")

            await asyncio.to_thread(
                orders_client.preview_option_order,
                resp_format="json",
                accountId=account_id,
                symbol=ticker,
                callPut=right,
                expiryDate=expiry,
                strikePrice=float(strike),
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType=price_type,
                limitPrice=float(limit_price) if limit_price else None,
                quantity=int(quantity),
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )

            resp = await asyncio.to_thread(
                orders_client.place_option_order,
                resp_format="json",
                accountId=account_id,
                symbol=ticker,
                callPut=right,
                expiryDate=expiry,
                strikePrice=float(strike),
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType=price_type,
                limitPrice=float(limit_price) if limit_price else None,
                quantity=int(quantity),
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )

        else:
            # STOCK
            quantity = payload.get("position_size_shares") or 1
            price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
            limit_price = payload.get("limit_price") or payload.get("entry")
            order_action = "BUY" if action == "BUY" else "SELL"

            await asyncio.to_thread(
                orders_client.preview_equity_order,
                resp_format="json",
                accountId=account_id,
                symbol=ticker,
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType=price_type,
                limitPrice=float(limit_price) if limit_price else None,
                quantity=int(quantity),
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )

            resp = await asyncio.to_thread(
                orders_client.place_equity_order,
                resp_format="json",
                accountId=account_id,
                symbol=ticker,
                orderAction=order_action,
                clientOrderId=client_order_id,
                priceType=price_type,
                limitPrice=float(limit_price) if limit_price else None,
                quantity=int(quantity),
                orderTerm="GOOD_FOR_DAY",
                marketSession="REGULAR",
            )

        logger.info(f"✅ LIVE ORDER PLACED: {resp}")
        return {"status": "success", "broker_response": resp, "client_order_id": client_order_id}

    except Exception as e:
        global consecutive_failures, circuit_breaker_open
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
            await alert_admin("CIRCUIT BREAKER TRIGGERED", str(e))
        logger.error(f"Live order failed: {e}")
        raise

# =========================================================
# BACKGROUND WORKER
# =========================================================
async def placement_worker():
    global _worker_stop
    logger.info("🚀 Background placement worker started")
    while not _worker_stop:
        try:
            job_data = await redis.lpop(QUEUE_KEY)
            if job_data:
                job = json.loads(job_data)
                await execute_live_order(job["payload"])
            else:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(2)

async def start_worker():
    global _worker_task
    if not _worker_task or _worker_task.done():
        _worker_task = asyncio.create_task(placement_worker())

async def stop_worker():
    global _worker_stop
    _worker_stop = True

# =========================================================
# STARTUP / SHUTDOWN
# =========================================================
@app.on_event("startup")
async def on_startup():
    global redis
    logger.info(f"Starting in {'SANDBOX' if is_sandbox else 'PRODUCTION'} mode | LIVE_TRADING={LIVE_TRADING}")

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    await init_db()
    await start_worker()
    logger.info("✅ Worker started")

@app.on_event("shutdown")
async def on_shutdown():
    await stop_worker()
    if redis:
        await redis.close()

# =========================================================
# HEALTH & METRICS
# =========================================================
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "circuit_breaker": "OPEN" if circuit_breaker_open else "CLOSED",
    }

@app.get("/metrics")
async def metrics():
    today = datetime.utcnow().date().isoformat()
    loss = float(await redis.get(f"daily_loss:{today}") or 0) if redis else 0
    return {
        "consecutive_failures": consecutive_failures,
        "circuit_breaker_open": circuit_breaker_open,
        "daily_loss": loss,
    }

# =========================================================
# WEBHOOK
# =========================================================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    global consecutive_failures
    try:
        await check_risk_limits()
        if time.time() < broker_down_until:
            return {"status": "cooldown"}

        if payload.secret != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        client_order_id = str(uuid.uuid4())[:20]
        job = {
            "client_order_id": client_order_id,
            "payload": payload.dict(),
            "queued_at": datetime.utcnow().isoformat()
        }
        await redis.rpush(QUEUE_KEY, json.dumps(job))
        return {"status": "queued", "client_order_id": client_order_id}

    except HTTPException as he:
        raise he
    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
        logger.error(f"Webhook error: {e}")
        return {"status": "failed", "message": str(e)}

# =========================================================
# RAILWAY RUN
# =========================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
