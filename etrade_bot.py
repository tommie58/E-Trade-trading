from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional

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

# SQLAlchemy async
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime, JSON, Text, func, select

# tenacity
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

# =========================================================
# CONFIG + SAFETY LIMITS
# =========================================================
TOKENS_FILE = ".etrade_tokens.json.enc"
ENV = os.getenv("ETRADE_ENV", "sandbox")
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "5"))
MAX_POSITION_VALUE = float(os.getenv("MAX_POSITION_VALUE", "50000"))  # new
DAILY_LOSS_LIMIT_DOLLARS = float(os.getenv("DAILY_LOSS_LIMIT_DOLLARS", "-1000"))  # new - negative = loss
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

dev_mode = ENV == "sandbox"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Bot")
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

# Circuit breaker
circuit_breaker_open = False
consecutive_failures = 0
MAX_CONSECUTIVE_FAILURES = 5
last_failure_time = None
daily_loss_tracker = {}  # date -> loss in dollars

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
# KMS, TOKENS, REDIS, DB, HELPERS (unchanged)
# =========================================================
# (All previous KMS, token, Redis, DB, market, option contract, load_session, etc. functions are unchanged and included in the full file you already have. They remain exactly as in the last version.)

# =========================================================
# SAFETY: CIRCUIT BREAKER + RISK CHECKS
# =========================================================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open - trading paused")

    # Daily loss check (simple Redis-based)
    today = datetime.utcnow().date().isoformat()
    current_loss = float(await redis.get(f"daily_loss:{today}") or 0)
    if current_loss <= DAILY_LOSS_LIMIT_DOLLARS:
        circuit_breaker_open = True
        await alert_admin("🚨 DAILY LOSS LIMIT BREACHED", f"Loss today: ${current_loss}")
        raise HTTPException(503, "Daily loss limit reached - trading paused")

async def record_trade_pnl(pnl: float):
    today = datetime.utcnow().date().isoformat()
    await redis.incrbyfloat(f"daily_loss:{today}", pnl)
    await redis.expire(f"daily_loss:{today}", 86400)  # 24h

async def alert_admin(subject: str, body: str):
    if not ALERT_WEBHOOK_URL:
        logger.warning(f"ALERT: {subject} - {body}")
        return
    # Simple webhook alert (extend to SNS/email if needed)
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(ALERT_WEBHOOK_URL, json={"text": f"**{subject}**\n{body}"})
        except Exception:
            logger.exception("Failed to send alert")

# =========================================================
# STARTUP / SHUTDOWN
# =========================================================
@app.on_event("startup")
async def on_startup():
    global redis, fernet
    # Redis
    try:
        redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")

    # DB
    await init_db()

    # KMS -> Fernet
    try:
        plaintext = get_encryption_key_from_kms()
        key = plaintext if len(plaintext) == 44 else base64.b64encode(plaintext)
        fernet = Fernet(key)
        logger.info("✅ Fernet initialized from KMS")
    except Exception as e:
        logger.error(f"❌ KMS/Fernet init failed: {e}")

    # Start background worker
    await start_worker()
    logger.info("✅ Placement worker started")

@app.on_event("shutdown")
async def on_shutdown():
    global redis
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
        "circuit_breaker": "OPEN" if circuit_breaker_open else "CLOSED",
        "live_trading": LIVE_TRADING,
        "redis": "connected" if redis else "disconnected",
        "db": "connected" if async_session else "disabled"
    }

@app.get("/metrics")
async def metrics():
    return {
        "consecutive_failures": consecutive_failures,
        "circuit_breaker_open": circuit_breaker_open,
        "daily_loss": float(await redis.get(f"daily_loss:{datetime.utcnow().date().isoformat()}") or 0)
    }

# =========================================================
# WEBHOOK (updated with safety checks)
# =========================================================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    global broker_down_until, consecutive_failures
    try:
        await check_risk_limits()  # ← safety check

        if time.time() < broker_down_until:
            return {"status": "cooldown", "message": "E*TRADE temporarily unavailable"}

        logger.info(f"📥 PAYLOAD:\n{json.dumps(payload.dict(), indent=2)}")
        if payload.secret != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        # ... (rest of your latest webhook logic - ticker, action, instrument, mode, duplicate check, load_session, etc.)

        # After successful trade placement in process_placement_job or worker, call:
        # await record_trade_pnl(pnl_dollars)  # you can extend this later

        return {"status": "queued", "client_order_id": client_order_id}

    except HTTPException as he:
        raise he
    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            circuit_breaker_open = True
            await alert_admin("🚨 CIRCUIT BREAKER TRIGGERED", f"{consecutive_failures} consecutive failures")
        logger.error("❌ WEBHOOK FAILURE")
        traceback.print_exc()
        return {"status": "failed", "message": str(e)}

# (The rest of the file - worker, process_placement_job, DB helpers, etc. - remains exactly as in the previous version you have.)
