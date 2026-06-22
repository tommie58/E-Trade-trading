from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import Optional, List
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

# ==================== DATABASE MODEL FOR OAUTH STATE ====================
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
                    state = ETradeSessionState(
                        id="active_state",
                        oauth_token=str(token_val),
                        oauth_token_secret=str(secret_val)
                    )
                    await session.merge(state)

        logger.info("✅ E*TRADE auth URL generated successfully")
        return {
            "status": "success",
            "auth_url": auth_url,
            "authorize_url": auth_url,
            "url": auth_url,
            "authorization_url": auth_url,
            "message": "Open this URL in browser to authorize E*TRADE"
        }

    except Exception as e:
        logger.error(f"Start linking failed: {str(e)}")
        raise HTTPException(500, detail=f"Could not start linking: {str(e)}")


@app.post("/etrade/auth/complete")
@app.post("/complete-link")
@app.post("/oauth/complete")
async def etrade_auth_complete(data: dict = Body(...)):
    try:
        verifier = data.get("oauth_verifier") or data.get("verifier") or data.get("code")
        if not verifier:
            raise HTTPException(400, "Missing verification code")

        token_val, secret_val = None, None
        if async_session:
            async with async_session() as session:
                cached_state = await session.get(ETradeSessionState, "active_state")
                if cached_state:
                    token_val = cached_state.oauth_token
                    secret_val = cached_state.oauth_token_secret

        if not token_val or not secret_val:
            raise HTTPException(400, "No active request token found. Please start linking again.")

        etrade_session = OAuth1Session(
            client_key=CONSUMER_KEY,
            client_secret=CONSUMER_SECRET,
            resource_owner_key=token_val,
            resource_owner_secret=secret_val,
            verifier=verifier
        )
        access_tokens = etrade_session.fetch_access_token(ACCESS_TOKEN_URL)

        final_token = access_tokens.get("oauth_token")
        final_secret = access_tokens.get("oauth_token_secret")

        if not final_token or not final_secret:
            raise Exception("Failed to get access tokens from E*TRADE")

        save_tokens(final_token, final_secret)
        logger.info("✅ E*TRADE linking completed successfully with REAL tokens")

        return {"status": "success", "message": "E*TRADE Account Successfully Linked!"}

    except Exception as e:
        logger.error(f"Complete link failed: {str(e)}")
        raise HTTPException(500, detail=f"Linking failed: {str(e)}")


# ==================== IMPROVED: Token Renew Endpoint ====================
@app.post("/etrade/auth/renew")
async def etrade_auth_renew(data: dict = Body(...)):
    try:
        # Get tokens from request body (mobile app) or environment
        access_token = data.get("access_token") or os.getenv("ETRADE_ACCESS_TOKEN")
        access_token_secret = data.get("access_token_secret") or os.getenv("ETRADE_ACCESS_TOKEN_SECRET")

        if not access_token or not access_token_secret:
            raise HTTPException(400, "Missing access tokens for renewal")

        logger.info("Attempting to validate current tokens before renewal...")

        # Step 1: Try to use current tokens with a lightweight call
        try:
            accounts = pyetrade.ETradeAccounts(
                CONSUMER_KEY,
                CONSUMER_SECRET,
                access_token,
                access_token_secret,
                dev=is_sandbox
            )
            # Lightweight call to test if tokens are still valid
            test_response = await asyncio.to_thread(accounts.list_accounts, resp_format="json")
            logger.info("✅ Current tokens are still valid. No renewal needed.")
            return {"status": "success", "message": "Tokens are still valid", "renewed": False}

        except Exception as auth_error:
            logger.warning(f"Current tokens appear invalid or expired: {auth_error}")

        # Step 2: Attempt renewal using ETradeAccessManager
        logger.info("Attempting token renewal via ETradeAccessManager...")
        auth_manager = pyetrade.ETradeAccessManager(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            access_token,
            access_token_secret
        )

        renewed = await asyncio.to_thread(auth_manager.renew_access_token)

        if renewed:
            new_token = auth_manager.oauth_token
            new_secret = auth_manager.oauth_token_secret
            save_tokens(new_token, new_secret)
            logger.info("✅ Tokens renewed successfully")
            return {
                "status": "success",
                "message": "Tokens renewed successfully",
                "renewed": True
            }
        else:
            logger.error("Token renewal returned False")
            raise HTTPException(400, "Token renewal failed")

    except Exception as e:
        logger.error(f"Renew failed: {str(e)}")
        raise HTTPException(500, detail=f"Renew failed: {str(e)}")


# ==================== Quote Endpoint ====================
@app.get("/etrade/quote")
async def get_quotes(symbols: str = Query(...)):
    try:
        tokens = load_tokens()
        if not tokens:
            raise HTTPException(401, "E*TRADE account not linked")

        market = pyetrade.ETradeMarket(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            tokens["oauth_token"],
            tokens["oauth_token_secret"],
            dev=is_sandbox
        )

        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
        response = await asyncio.to_thread(
            market.get_quote,
            symbol_list,
            resp_format="json"
        )

        return response

    except Exception as e:
        logger.error(f"Quote failed: {str(e)}")
        raise HTTPException(500, detail=f"Failed to get quotes: {str(e)}")


# ==================== E*TRADE ACCOUNT STATUS ====================
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
        logger.error(f"Database failed: {e}")


# ==================== SAFETY ====================
async def check_risk_limits():
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open")


# ==================== LIVE TRADING ====================
async def execute_live_order(payload: dict):
    if not LIVE_TRADING or is_sandbox:
        return {"status": "skipped"}

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

        preview_resp = await asyncio.to_thread(
            orders.preview_equity_order,
            resp_format="json",
            accountIdKey=TARGET_ACCOUNT_ID,
            order=order_payload,
            clientOrderId=client_order_id
        )
        preview_id = preview_resp['PreviewOrderResponse']['PreviewIds']['PreviewId'][0]['previewId']

        final_resp = await asyncio.to_thread(
            orders.place_equity_order,
            resp_format="json",
            accountIdKey=TARGET_ACCOUNT_ID,
            order=order_payload,
            clientOrderId=client_order_id,
            previewId=preview_id
        )

        logger.info(f"✅ LIVE TRADE EXECUTED: {ticker}")
        return {"status": "success", "response": final_resp}

    except Exception as e:
        consecutive_failures += 1
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            global circuit_breaker_open
            circuit_breaker_open = True
        logger.error(f"Trade failed: {e}")
        raise


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
        "linked": bool(tokens),
        "ready_for_live_trading": bool(tokens and TARGET_ACCOUNT_ID and LIVE_TRADING and not is_sandbox)
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
