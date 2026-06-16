from fastapi import FastAPI, HTTPException, Request, Body
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
import base64
import boto3
from datetime import datetime, timedelta
import pytz
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken
import aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Text, func, select
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

# =========================================================
# CONFIG + SAFETY LIMITS
# =========================================================
TOKENS_FILE = ".etrade_tokens.json.enc"
ENV = os.getenv("ETRADE_ENV", "sandbox").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "2"))
MAX_POSITION_VALUE = float(os.getenv("MAX_POSITION_VALUE", "10000"))
DAILY_LOSS_LIMIT_DOLLARS = float(os.getenv("DAILY_LOSS_LIMIT_DOLLARS", "-500"))
ENABLE_MARKET_HOURS_CHECK = os.getenv("ENABLE_MARKET_HOURS_CHECK", "false").lower() == "true"
BROKER_TIMEOUT_SECONDS = int(os.getenv("BROKER_TIMEOUT_SECONDS", "25"))
VERIFY_POSITIONS_ON_CLOSE = os.getenv("VERIFY_POSITIONS_ON_CLOSE", "false").lower() == "true"
REJECT_0_DTE = os.getenv("REJECT_0_DTE", "false").lower() == "true"
ZERO_DTE_DELAY_SECONDS = int(os.getenv("ZERO_DTE_DELAY_SECONDS", "180"))
RECENT_TTL = int(os.getenv("RECENT_TTL", "30"))
KMS_ENCRYPTED_KEY = os.getenv("KMS_ENCRYPTED_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

is_sandbox = ENV == "sandbox"          # True = sandbox, False = production
dev_mode_for_pyetrade = is_sandbox     # pyetrade: dev=True → sandbox, dev=False → production

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Bot - Live Trading Enabled")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# =========================================================
# GLOBALS + CIRCUIT BREAKER
# =========================================================
broker_down_until = 0
TOKENS_PATH = Path(TOKENS_FILE)
fernet = None
redis = None
engine = None
async_session = None
_worker_task = None
_worker_stop = False
QUEUE_KEY = "etrade:placement_queue"

circuit_breaker_open = False
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5
last_failure_time = None
daily_loss_tracker = {}

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
# EXCEPTIONS & RETRY
# =========================================================
class TransientBrokerError(Exception):
    pass

class AuthInvalidError(Exception):
    pass

def classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if any(k in m for k in ["429", "rate limit", "too many requests"]):
        return "rate_limit"
    if any(k in m for k in ["temporarily unavailable", "gateway timeout", "service unavailable", "timeout"]):
        return "broker_unavailable"
    if any(kw in m for kw in ["oauth", "token", "unauthorized", "401"]):
        return "auth_error"
    return "other_error"

# =========================================================
# KMS + TOKEN HELPERS (minimal working versions)
# =========================================================
def get_encryption_key_from_kms() -> bytes:
    """Replace with your real KMS logic if needed"""
    if KMS_ENCRYPTED_KEY:
        # Example: decrypt using boto3 KMS
        kms = boto3.client('kms')
        response = kms.decrypt(CiphertextBlob=base64.b64decode(KMS_ENCRYPTED_KEY))
        return response['Plaintext']
    # Fallback for local dev
    return os.getenv("FERNET_KEY", "your-fernet-key-here-32-bytes-long!!").encode()

def load_tokens() -> Optional[Dict[str, str]]:
    global fernet
    if not TOKENS_PATH.exists() or not fernet:
        logger.error("No tokens file or Fernet not initialized")
        return None
    try:
        encrypted = TOKENS_PATH.read_bytes()
        decrypted = fernet.decrypt(encrypted)
        return json.loads(decrypted)
    except (InvalidToken, Exception) as e:
        logger.error(f"Failed to load tokens: {e}")
        return None

# =========================================================
# REDIS + DB (minimal)
# =========================================================
async def init_db():
    global engine, async_session
    if not DATABASE_URL:
        logger.warning("DATABASE_URL not set - DB disabled")
        return
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    logger.info("✅ Database initialized")

# =========================================================
# SAFETY: CIRCUIT BREAKER + RISK CHECKS
# =========================================================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open - trading paused")

    today = datetime.utcnow().date().isoformat()
    current_loss = float(await redis.get(f"daily_loss:{today}") or 0)
    if current_loss <= DAILY_LOSS_LIMIT_DOLLARS:
        circuit_breaker_open = True
        await alert_admin("🚨 DAILY LOSS LIMIT BREACHED", f"Loss today: ${current_loss}")
        raise HTTPException(503, "Daily loss limit reached - trading paused")

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
        async with aiohttp.ClientSession() as s:
            await s.post(ALERT_WEBHOOK_URL, json={"text": f"**{subject}**\n{body}"})
    except Exception:
        logger.exception("Failed to send alert")

# =========================================================
# LIVE TRADING EXECUTION FUNCTION
# =========================================================
async def execute_live_order(payload: dict) -> dict:
    """Real order placement for production (with preview first)"""
    if not LIVE_TRADING or is_sandbox:
        logger.warning("Skipping real trade - not in LIVE_TRADING + production mode")
        return {"status": "skipped", "reason": "sandbox_or_not_live"}

    await check_risk_limits()

    instrument = payload.get("instrument", "stock").lower()
    action = payload["action"].upper()
    ticker = payload["ticker"]
    client_order_id = payload.get("client_order_id") or str(uuid.uuid4())[:20]
    account_id = TARGET_ACCOUNT_ID

    if not account_id:
        raise ValueError("ETRADE_ACCOUNT_ID is required for live trading")

    tokens = load_tokens()
    if not tokens:
        raise AuthInvalidError("Failed to load tokens")

    orders_client = pyetrade.ETradeOrder(
        os.getenv("ETRADE_CONSUMER_KEY"),
        os.getenv("ETRADE_CONSUMER_SECRET"),
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=is_sandbox   # False = production
    )

    try:
        if instrument == "option":
            strike = payload.get("strike") or payload.get("strike_hint")
            expiry = payload.get("expiry") or payload.get("expiration_hint")
            right = (payload.get("option_right") or "C").upper()[0]
            quantity = payload.get("contracts") or payload.get("option_contracts") or 1

            if action in ("BUY", "BUY_OPEN"):
                order_action = "BUY_OPEN"
            else:
                order_action = "SELL_CLOSE"

            price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
            limit_price = payload.get("limit_price") or payload.get("entry")

            # Preview first (recommended for live)
            preview = await asyncio.to_thread(
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
            logger.info(f"Option preview OK: {preview}")

            # Place real order
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

            preview = await asyncio.to_thread(
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
            logger.info(f"Equity preview OK")

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

        logger.info(f"✅ LIVE ORDER PLACED SUCCESSFULLY: {resp}")
        return {"status": "success", "broker_response": resp, "client_order_id": client_order_id}

    except Exception as e:
        logger.error(f"❌ LIVE ORDER FAILED: {e}")
        global consecutive_failures, circuit_breaker_open
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
            await alert_admin("🚨 CIRCUIT BREAKER TRIGGERED", str(e))
        raise

# =========================================================
# BACKGROUND WORKER
# =========================================================
async def placement_worker():
    global _worker_stop
    logger.info("🚀 Placement worker started")
    while not _worker_stop:
        try:
            job_data = await redis.lpop(QUEUE_KEY)
            if job_data:
                job = json.loads(job_data)
                logger.info(f"Processing job: {job.get('client_order_id')}")
                try:
                    result = await execute_live_order(job["payload"])
                    logger.info(f"Job completed: {result}")
                except Exception as e:
                    logger.error(f"Job failed: {e}")
            else:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(2)

async def start_worker():
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(placement_worker())

async def stop_worker():
    global _worker_stop, _worker_task
    _worker_stop = True
    if _worker_task:
        await _worker_task

# =========================================================
# STARTUP / SHUTDOWN
# =========================================================
@app.on_event("startup")
async def on_startup():
    global redis, fernet
    try:
        redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis failed: {e}")

    await init_db()

    try:
        plaintext = get_encryption_key_from_kms()
        key = plaintext if len(plaintext) == 44 else base64.b64encode(plaintext)
        fernet = Fernet(key)
        logger.info("✅ Fernet initialized")
    except Exception as e:
        logger.error(f"❌ KMS/Fernet init failed: {e}")

    await start_worker()
    logger.info("✅ Worker started")

@app.on_event("shutdown")
async def on_shutdown():
    await stop_worker()
    if redis:
        await redis.close()

# =========================================================
# HEALTH + METRICS
# =========================================================
@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "circuit_breaker": "OPEN" if circuit_breaker_open else "CLOSED",
        "redis": "connected" if redis else "disconnected",
    }

@app.get("/metrics")
async def metrics():
    today = datetime.utcnow().date().isoformat()
    daily_loss = float(await redis.get(f"daily_loss:{today}") or 0) if redis else 0
    return {
        "consecutive_failures": consecutive_failures,
        "circuit_breaker_open": circuit_breaker_open,
        "daily_loss": daily_loss,
        "env": ENV,
        "live_trading": LIVE_TRADING,
    }

# =========================================================
# WEBHOOK (fully functional)
# =========================================================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    global consecutive_failures, broker_down_until

    try:
        await check_risk_limits()

        if time.time() < broker_down_until:
            return {"status": "cooldown", "message": "Broker temporarily unavailable"}

        if payload.secret != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        logger.info(f"📥 WEBHOOK RECEIVED: {payload.dict()}")

        # Queue for background worker
        client_order_id = str(uuid.uuid4())[:20]
        job = {
            "client_order_id": client_order_id,
            "payload": payload.dict(),
            "queued_at": datetime.utcnow().isoformat()
        }

        await redis.rpush(QUEUE_KEY, json.dumps(job))
        logger.info(f"✅ Job queued: {client_order_id}")

        return {"status": "queued", "client_order_id": client_order_id}

    except HTTPException as he:
        raise he
    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
            await alert_admin("🚨 CIRCUIT BREAKER TRIGGERED", str(e))
        logger.error(f"❌ WEBHOOK ERROR: {e}")
        traceback.print_exc()
        return {"status": "failed", "message": str(e)}

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
