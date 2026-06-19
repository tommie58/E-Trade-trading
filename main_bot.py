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

is_sandbox = os.getenv("ETRADE_ENV", "production").lower() == "sandbox"


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

# ==================== OAUTH SETUP ====================
oauth = pyetrade.ETradeOAuth(
    consumer_key=os.getenv("ETRADE_CONSUMER_KEY"),
    consumer_secret=os.getenv("ETRADE_CONSUMER_SECRET")
)

oauth.request_token_url = "https://etrade.com"
oauth.access_token_url = "https://etrade.com"
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

        logger.info("✅ E*TRADE auth URL generated successfully")

        return {
            "status": "success",
            "auth_url": auth_url,
            "authorize_url": auth_url,
            "url": auth_url,
            "authorization_url": auth_url,
            "request_token": auth_url,
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
        verifier = (
            data.get("oauth_verifier")
            or data.get("verifier")
            or data.get("code")
        )

        if not verifier:
            raise HTTPException(400, "Missing verification code")

        logger.info(f"Attempting to exchange verifier code...")

        access_token, access_token_secret = oauth.get_access_token(verifier)

        if access_token == "oauth_token" or len(access_token) < 20:
            logger.error("E*TRADE returned dummy/placeholder tokens")
            raise HTTPException(
                500, 
                detail="Linking failed. E*TRADE did not return valid tokens yet. Please wait a while and try linking again."
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
            detail="Linking failed. Please wait and try again later. If the problem continues, check your production keys or contact support."
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
    if not DATABASE_URL:
        logger.warning("No DATABASE_URL set")
        return
    try:
        engine = create_async_engine(DATABASE_URL, echo=False)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        logger.info("✅ Database connected")
    except Exception as e:
        logger.error(f"Database failed: {e}")

# ==================== SAFETY ====================
async def check_risk_limits():
    global circuit_breaker_open
    if circuit_breaker_open:
        raise HTTPException(503, "Circuit breaker open")

# ==================== LIVE TRADING (UPDATED - Preview + Place) ====================
async def execute_live_order(payload: dict):
    if not LIVE_TRADING or is_sandbox:
        return {"status": "skipped", "reason": "Not in live mode or sandbox"}

    await check_risk_limits()

    tokens = load_tokens()
    if not tokens:
        raise Exception("E*TRADE tokens not set")

    orders = pyetrade.ETradeOrder(
        os.getenv("ETRADE_CONSUMER_KEY"),
        os.getenv("ETRADE_CONSUMER_SECRET"),
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
        # Build order payload
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

        # Step 1: Preview the order (recommended by E*TRADE)
        logger.info(f"Previewing order for {ticker}...")
        preview_response = await asyncio.to_thread(
            orders.preview_equity_order,
            resp_format="json",
            accountIdKey=account_id,
            order=order_payload,
            clientOrderId=client_order_id
        )

        preview_id = preview_response['PreviewOrderResponse']['PreviewIds']['PreviewId'][0]['previewId']
        logger.info(f"Preview successful. Preview ID: {preview_id}")

        # Step 2: Place the actual order using the preview ID
        logger.info(f"Placing live order for {ticker}...")
        final_response = await asyncio.to_thread(
            orders.place_equity_order,
            resp_format="json",
            accountIdKey=account_id,
            order=order_payload,
            clientOrderId=client_order_id,
            previewId=preview_id
        )

        logger.info(f"✅ LIVE TRADE EXECUTED: {ticker} | {action}")
        return {"status": "success", "response": final_response}

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
            logger.info("✅ Redis connected")
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
    job = {"payload": payload.dict()}
    await redis.rpush(QUEUE_KEY, json.dumps(job))
    return {"status": "queued"}

@app.get("/health")
async def health():
    tokens = load_tokens()

    critical_vars = {
        "ETRADE_CONSUMER_KEY": bool(os.getenv("ETRADE_CONSUMER_KEY")),
        "ETRADE_CONSUMER_SECRET": bool(os.getenv("ETRADE_CONSUMER_SECRET")),
        "ETRADE_ACCESS_TOKEN": bool(os.getenv("ETRADE_ACCESS_TOKEN")),
        "ETRADE_ACCESS_TOKEN_SECRET": bool(os.getenv("ETRADE_ACCESS_TOKEN_SECRET")),
        "TARGET_ACCOUNT_ID": bool(TARGET_ACCOUNT_ID),
        "WEBHOOK_SECRET": bool(WEBHOOK_SECRET),
        "REDIS_URL": bool(REDIS_URL),
    }

    missing = [key for key, present in critical_vars.items() if not present]

    return {
        "status": "ok",
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "is_sandbox": is_sandbox,
        "linked": bool(tokens),
        "ready_for_linking": bool(
            os.getenv("ETRADE_CONSUMER_KEY") and os.getenv("ETRADE_CONSUMER_SECRET")
        ),
        "ready_for_live_trading": bool(
            tokens and TARGET_ACCOUNT_ID and LIVE_TRADING and not is_sandbox
        ),
        "missing_critical_vars": missing,
        "redis_connected": redis is not None,
        "database_connected": engine is not None,
        "message": "All critical variables present" if not missing else "Some variables are missing"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
