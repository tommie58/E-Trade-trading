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
import urllib.parse
from datetime import datetime
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from dotenv import load_dotenv

# ==================== ENVIRONMENT INITIALIZATION ====================
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

ENV = "production"
LIVE_TRADING = True
is_sandbox = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Trading Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== GLOBALS & DB SCHEMA ====================
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

# ==================== FIXED PRODUCTION OAUTH SETUP ====================
pyetrade.ETradeOAuth.BASE_URL = "https://etrade.com"

oauth = pyetrade.ETradeOAuth(
    consumer_key=CONSUMER_KEY,
    consumer_secret=CONSUMER_SECRET
)

oauth.request_token_url = "https://etrade.com/oauth/request_token"
oauth.access_token_url = "https://etrade.com/oauth/access_token"
oauth.authorize_url = "https://etrade.com{}&token={}"

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

# ==================== OAUTH LINKING ====================
@app.api_route("/etrade/auth/start", methods=["GET", "POST"])
@app.api_route("/link", methods=["GET", "POST"])
async def etrade_auth_start():
    try:
        auth_url = oauth.get_request_token()

        if not auth_url:
            raise HTTPException(500, detail="Failed to generate authorization URL")

        parsed_url = urllib.parse.urlparse(auth_url)
        url_params = urllib.parse.parse_qs(parsed_url.query)
        token_list = url_params.get('oauth_token', [''])
        token_val = token_list[0] if token_list else ""
        secret_val = oauth.client.client.resource_owner_secret if hasattr(oauth, 'client') else ""

        if async_session and token_val and secret_val:
            async with async_session() as session:
                async with session.begin():
                    state = ETradeSessionState(id="active_state", oauth_token=token_val, oauth_token_secret=secret_val)
                    await session.merge(state)
                    logger.info("✅ Temporary verification tokens cached inside database record")

        logger.info("✅ E*TRADE auth URL generated successfully")

        return {
            "status": "success",
            "auth_url": auth_url,
            "authorize_url": auth_url,
            "url": auth_url,
            "authorization_url": auth_url,
            "message": "Open this URL in browser to authorize E*TRADE production mapping"
        }

    except Exception as e:
        logger.error(f"Start linking failed: {str(e)}")
        raise HTTPException(500, detail=f"Could not start linking: {str(e)}")

@app.post("/etrade/auth/complete")
@app.post("/complete-link")
@app.post("/oauth/complete")
async def etrade_auth_complete(data: dict = Body(...)):
    try:
        verifier = (
            data.get("oauth_verifier")
            or data.get("verifier")
            or data.get("code")
        )

        if not verifier:
            raise HTTPException(400, "Missing verification code")

        logger.info(f"Retrieving cached connection context tracking data...")

        if async_session:
            async with async_session() as session:
                cached_state = await session.get(ETradeSessionState, "active_state")
                if cached_state and hasattr(oauth, 'client'):
                    oauth.client.client.resource_owner_key = cached_state.oauth_token
                    oauth.client.client.resource_owner_secret = cached_state.oauth_token_secret
                    logger.info("✅ Cryptographic session parameters restored successfully")

        logger.info(f"Attempting token verification handshake...")
        access_token, access_token_secret = oauth.get_access_token(verifier)

        if access_token == "oauth_token" or len(access_token) < 20:
            logger.error("E*TRADE returned dummy/placeholder tokens")
            raise HTTPException(
                500, 
                detail="Linking failed. E*TRADE did not return valid live tokens yet. Try clearing browser cache."
            )

        save_tokens(access_token, access_token_secret)
        logger.info("✅ E*TRADE linking completed successfully with REAL tokens")

        return {
            "status": "success",
            "message": "E*TRADE Account Successfully Linked!"
        }

    except HTTPException as he:
        raise he

    except Exception as e:
        logger.error(f"Complete link failed: {str(e)}")
        raise HTTPException(
            500, 
            detail=f"Linking failed. Handshake rejection reason: {str(e)}"
        )

# ==================== E*TRADE ACCOUNT STATUS ====================
@app.get("/etrade/account")
async def get_etrade_account():
    tokens = load_tokens()
    
    if not tokens:
        return {
            "status": "not_linked",
            "message": "E*TRADE account is not linked",
            "linked": False
        }
    
    return {
        "status": "linked",
        "message": "E*TRADE account is successfully linked",
        "linked": True,
        "has_tokens": True
    }

# ==================== DATABASE ====================
async def init_db():
    global engine, async_session
    try:
        local_db_url = "sqlite+aiosqlite:///etrade_cache.db"
        target_url = DATABASE_URL if (DATABASE_URL and "railway" in DATABASE_URL) else local_db_url
        
        if not target_url or "internal" in target_url:
            target_url = local_db_url

        logger.info(f"Connecting to token state storage via: {target_url.split('@')[-1]}")
        engine = create_async_engine(target_url, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
        logger.info("✅ Token tracking file structures initialized successfully")
    except Exception as e:
        logger.error(f"Database failed to initialize: {e}")

@app.on_event("startup")
async def startup_event():
    await init_db()

# ==================== SAFETY ====================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open")

# ==================== LIVE TRADING ====================
async def execute_live_order(payload: dict):
    if not LIVE_TRADING or is_sandbox:
        return {"status": "skipped", "reason": "Not in live mode or sandbox"}

    await check_risk_limits()

    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE active session tokens not set")

    orders = pyetrade.ETradeOrder(
        CONSUMER_KEY,
        CONSUMER_SECRET,
        tokens["oauth_token"],
        tokens["oauth_token_secret"],
        dev=is_sandbox
    )

    ticker = payload["ticker"]
    action = payload["action"]
    account_id = TARGET_ACCOUNT_ID
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
