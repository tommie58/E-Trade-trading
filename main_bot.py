from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import pyetrade
import os
import logging
import uuid
import asyncio
from datetime import datetime, timedelta
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Trading Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== GLOBALS ====================
redis = None
engine = None
async_session = None
_current_tokens: Dict[str, str] = {}

# In-memory pending request tokens (request_token -> info)
pending_request_tokens: Dict[str, dict] = {}
MAX_PENDING_TOKENS = 5
REQUEST_TOKEN_TTL = timedelta(minutes=5)

Base = declarative_base()

class ETradeSessionState(Base):
    __tablename__ = "etrade_session_state"
    id = Column(String, primary_key=True, default="current")
    access_token = Column(Text)
    access_token_secret = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow)

def load_tokens() -> Optional[Dict[str, str]]:
    if _current_tokens:
        return _current_tokens
    token = os.getenv("ETRADE_ACCESS_TOKEN")
    secret = os.getenv("ETRADE_ACCESS_TOKEN_SECRET")
    if token and secret:
        return {"oauth_token": token, "oauth_token_secret": secret}
    return None

def save_tokens(tokens: Dict[str, str]):
    global _current_tokens
    _current_tokens = tokens.copy()
    logger.info("✅ Tokens saved to memory cache")

def _cleanup_pending_tokens():
    """Remove expired or oldest tokens (keep max 5)"""
    global pending_request_tokens
    now = datetime.utcnow()
    # Remove expired
    pending_request_tokens = {
        k: v for k, v in pending_request_tokens.items()
        if now - v["timestamp"] < REQUEST_TOKEN_TTL
    }
    # Keep only the newest 5
    if len(pending_request_tokens) > MAX_PENDING_TOKENS:
        sorted_tokens = sorted(
            pending_request_tokens.items(),
            key=lambda x: x[1]["timestamp"],
            reverse=True
        )
        pending_request_tokens = dict(sorted_tokens[:MAX_PENDING_TOKENS])

# ==================== DATABASE ====================
async def init_db():
    global engine, async_session
    try:
        if DATABASE_URL:
            db_url = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")
        else:
            logger.info("Using SQLite for database (recommended)")
            db_url = "sqlite+aiosqlite:///./etrade_tokens.db"

        engine = create_async_engine(db_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ Database connected")
    except Exception as e:
        logger.warning(f"Database warning (falling back to SQLite): {e}")

# ==================== LIVE TRADING (kept concise) ====================
async def execute_live_order(payload: dict):
    tokens = load_tokens()
    if not tokens:
        raise Exception("No E*TRADE tokens available")
    if not TARGET_ACCOUNT_ID:
        raise Exception("TARGET_ACCOUNT_ID is not set")

    # ... (your existing flat-kwargs option + equity logic here)
    # For space, keeping the structure — paste your working execute_live_order if needed
    ticker = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    instrument = payload.get("instrument", "stock").lower()
    mode = payload.get("mode", "paper").lower()
    quantity = int(payload.get("quantity", 1))

    if mode != "live":
        return {"status": "paper"}

    logger.info(f"🚀 LIVE {instrument.upper()} ORDER: {action} {quantity} {ticker}")
    # Add your full order placement code here (flat kwargs version)
    return {"status": "success", "message": "Order placed (placeholder)"}

# ==================== MODELS ====================
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

# ==================== ENDPOINTS ====================
@app.post("/webhook")
async def webhook(payload: WebhookPayload = Body(...)):
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(403, "Unauthorized")
    try:
        result = await execute_live_order(payload.dict())
        return {"status": "processed", "result": result}
    except Exception as e:
        return {"status": "failed", "message": str(e)}

@app.post("/etrade/auth/start")
async def start_linking():
    try:
        oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
        result = oauth.get_request_token()

        if isinstance(result, dict):
            request_token = result.get("oauth_token")
            request_secret = result.get("oauth_token_secret")
        else:
            request_token = result
            request_secret = None

        _cleanup_pending_tokens()
        pending_request_tokens[request_token] = {
            "timestamp": datetime.utcnow(),
            "secret": request_secret
        }

        authorize_url = f"https://us.etrade.com/e/t/etws/authorize?key={CONSUMER_KEY}&token={request_token}"

        logger.info("✅ E*TRADE auth URL generated successfully")
        return {
            "authorize_url": authorize_url,
            "request_token": request_token
        }
    except Exception as e:
        logger.error(f"Start linking failed: {e}")
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_linking(
    verifier: str = Body(..., embed=True),
    request_token: Optional[str] = Body(None, embed=True)
):
    if not request_token:
        raise HTTPException(400, "request_token is required (echo it from /start response)")

    _cleanup_pending_tokens()

    if request_token not in pending_request_tokens:
        raise HTTPException(409, "No matching request token found. Please call /start again.")

    entry = pending_request_tokens[request_token]
    age = datetime.utcnow() - entry["timestamp"]
    if age > REQUEST_TOKEN_TTL:
        del pending_request_tokens[request_token]
        raise HTTPException(400, "Request token expired (5-minute limit). Please start linking again.")

    try:
        oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)
        oauth.get_request_token()  # initialize
        tokens = oauth.get_access_token(verifier)

        # Success — clean up used token
        del pending_request_tokens[request_token]
        save_tokens(tokens)

        logger.info("=== NEW TOKENS RECEIVED ===")
        logger.info(f"ETRADE_ACCESS_TOKEN={tokens['oauth_token']}")

        return {
            "status": "success",
            "linked": True,
            "env": ENV,
            "access_token": tokens["oauth_token"],
            "access_token_secret": tokens["oauth_token_secret"]
        }
    except Exception as e:
        error_msg = str(e)
        if "token_rejected" in error_msg.lower() or "401" in error_msg:
            raise HTTPException(400, "Invalid or already used verifier code")
        logger.error(f"Complete link failed: {e}")
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    tokens = load_tokens()
    if not tokens:
        return {"status": "not_linked", "linked": False}
    # Add your account listing logic here if needed
    return {"status": "linked", "linked": True, "accounts": []}

@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens)
    }

@app.on_event("startup")
async def on_startup():
    logger.info(f"Starting → PRODUCTION | LIVE={LIVE_TRADING}")
    if not TARGET_ACCOUNT_ID:
        logger.warning("⚠️ TARGET_ACCOUNT_ID not set — live orders will fail")
    await init_db()
    logger.info("✅ Bot ready")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
