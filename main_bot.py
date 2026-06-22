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
import time
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
    mode: Optional[str] = "paper"          # ← NEW: respects "live" or "paper"
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
    logger.info("Add these to Railway Variables and redeploy!")

# ==================== OAUTH + RENEW + QUOTE (same as before) ====================
# ... (keeping the rest of the file the same for brevity - the important changes are below)

REQUEST_TOKEN_URL = "https://api.etrade.com/oauth/request_token"
AUTHORIZE_URL = "https://us.etrade.com/e/t/etws/authorize"
ACCESS_TOKEN_URL = "https://api.etrade.com/oauth/access_token"

# (All previous endpoints for /link, /complete, /renew, /quote remain unchanged)

# ==================== UPDATED: LIVE TRADING WITH MODE SUPPORT ====================
async def execute_live_order(payload: dict):
    mode = payload.get("mode", "paper").lower()

    # Only proceed with real trade if mode is "live" AND server allows it
    if mode != "live" or not LIVE_TRADING or is_sandbox:
        logger.info(f"Signal mode={mode} → Skipping real trade (paper/filtered)")
        return {"status": "skipped", "reason": f"mode={mode}"}

    await check_risk_limits()

    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")

    orders = pyetrade.ETradeOrder(
        CONSUMER_KEY,
        CONSUMER_SECRET,
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=is_sandbox
    )

    ticker = payload["ticker"]
    action = payload["action"]
    client_order_id = str(uuid.uuid4())[:20]
    quantity = payload.get("position_size_shares", 1)
    price_type = "LIMIT" if payload.get("limit_price") else "MARKET"
    limit_price = payload.get("limit_price")
    order_action = "BUY" if action == "BUY" else "SELL"

    try:
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

        # Preview first
        preview_resp = await asyncio.to_thread(
            orders.preview_equity_order,
            resp_format="json",
            accountIdKey=TARGET_ACCOUNT_ID,
            order=order_payload,
            clientOrderId=client_order_id
        )
        preview_id = preview_resp['PreviewOrderResponse']['PreviewIds']['PreviewId'][0]['previewId']

        # Place order
        final_resp = await asyncio.to_thread(
            orders.place_equity_order,
            resp_format="json",
            accountIdKey=TARGET_ACCOUNT_ID,
            order=order_payload,
            clientOrderId=client_order_id,
            previewId=preview_id
        )

        logger.info(f"✅ LIVE TRADE EXECUTED: {ticker} | mode=live")
        return {"status": "success", "response": final_resp}

    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            global circuit_breaker_open
            circuit_breaker_open = True
        logger.error(f"Trade failed: {e}")
        raise

# ==================== WORKER (unchanged) ====================
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

# ==================== STARTUP / SHUTDOWN + OTHER ENDPOINTS ====================
# (All previous code for startup, shutdown, webhook, health, etc. remains the same)

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
        "linked": bool(tokens),
        "ready_for_live_trading": bool(tokens and TARGET_ACCOUNT_ID and LIVE_TRADING and not is_sandbox)
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
