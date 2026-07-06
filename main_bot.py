from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any, List, Tuple
import pyetrade
import os
import logging
import uuid
import asyncio
import urllib.parse
from datetime import datetime, timedelta, date
from requests_oauthlib import OAuth1Session
from redis.asyncio import from_url as redis_from_url
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, Text, DateTime
from dotenv import load_dotenv

load_dotenv()

# ==================== CONFIG ====================
VERSION = "2.8.0-trailing-stop"
ENV = os.getenv("ETRADE_ENV", "production").lower()
LIVE_TRADING = os.getenv("LIVE_TRADING", "true").lower() == "true"
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")
REDIS_URL = os.getenv("REDIS_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

is_sandbox = ENV == "sandbox"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Trading Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ==================== GLOBALS ====================
redis = None
engine = None
async_session = None
_current_tokens: Dict[str, str] = {}
pending_request_tokens: Dict[str, dict] = {}
_resolved_account_id: Optional[str] = None
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
    global pending_request_tokens
    now = datetime.utcnow()
    pending_request_tokens = {
        k: v for k, v in pending_request_tokens.items()
        if now - v["timestamp"] < REQUEST_TOKEN_TTL
    }
    if len(pending_request_tokens) > MAX_PENDING_TOKENS:
        sorted_tokens = sorted(
            pending_request_tokens.items(),
            key=lambda x: x[1]["timestamp"],
            reverse=True
        )
        pending_request_tokens = dict(sorted_tokens[:MAX_PENDING_TOKENS])

async def get_resolved_account_id() -> str:
    global _resolved_account_id
    if _resolved_account_id:
        return _resolved_account_id

    tokens = load_tokens()
    if not tokens:
        raise Exception("No tokens available")

    try:
        accounts_client = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret'],
            dev=is_sandbox
        )
        raw = accounts_client.list_accounts()
        if not raw or 'AccountListResponse' not in raw:
            raise Exception("Failed to fetch accounts")

        accounts = raw['AccountListResponse'].get('Accounts', {}).get('Account', [])
        if not isinstance(accounts, list):
            accounts = [accounts]

        if TARGET_ACCOUNT_ID:
            for acc in accounts:
                if str(acc.get("accountId")) == str(TARGET_ACCOUNT_ID) or \
                   str(acc.get("accountIdKey")) == str(TARGET_ACCOUNT_ID):
                    _resolved_account_id = acc.get("accountIdKey")
                    return _resolved_account_id

        for acc in accounts:
            if acc.get("accountStatus", "").upper() == "ACTIVE":
                _resolved_account_id = acc.get("accountIdKey")
                return _resolved_account_id

        if accounts:
            _resolved_account_id = accounts[0].get("accountIdKey")
            return _resolved_account_id

        raise Exception("No accounts found")
    except Exception as e:
        logger.error(f"Account resolution failed: {e}")
        raise

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

# ==================== OPTION CHAIN SNAPPING ====================
def _snap_to_real_contract(
    symbol: str,
    requested_expiry: str,
    requested_strike: float,
    call_put: str
) -> Tuple[str, float]:
    tokens = load_tokens()
    if not tokens:
        return requested_expiry, requested_strike

    try:
        market = pyetrade.ETradeMarket(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret'],
            dev=is_sandbox
        )
        chain = market.get_option_chains(symbol=symbol, resp_format="json")

        if not chain or 'OptionChainResponse' not in chain:
            return requested_expiry, requested_strike

        options = chain.get('OptionChainResponse', {}).get('OptionPair', [])
        if not isinstance(options, list):
            options = [options]

        expirations = set()
        strikes = set()

        for pair in options:
            for key in ['Call', 'Put']:
                if key in pair:
                    opt = pair[key]
                    exp = opt.get('expiryDate')
                    if exp:
                        if isinstance(exp, dict):
                            exp_str = f"{exp['year']}-{str(exp['month']).zfill(2)}-{str(exp['day']).zfill(2)}"
                        else:
                            exp_str = str(exp)
                        expirations.add(exp_str)

                    strike = opt.get('strikePrice')
                    if strike:
                        strikes.add(float(strike))

        if not expirations or not strikes:
            return requested_expiry, requested_strike

        try:
            req_date = datetime.strptime(requested_expiry, "%Y-%m-%d").date()
        except:
            req_date = datetime.strptime(requested_expiry.split("T")[0], "%Y-%m-%d").date()

        closest_expiry = min(
            expirations,
            key=lambda x: abs(datetime.strptime(x, "%Y-%m-%d").date() - req_date)
        )
        closest_strike = min(strikes, key=lambda x: abs(x - requested_strike))

        if closest_expiry != requested_expiry or closest_strike != requested_strike:
            logger.info(f"Snapped contract: {requested_expiry}@{requested_strike} → {closest_expiry}@{closest_strike}")

        return closest_expiry, closest_strike

    except Exception as e:
        logger.warning(f"Option chain lookup failed (using original values): {e}")
        return requested_expiry, requested_strike

# ==================== AUTO-RENEW HELPER ====================
def _get_market_client(tokens: dict):
    return pyetrade.ETradeMarket(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens['oauth_token'], tokens['oauth_token_secret'],
        dev=is_sandbox
    )

def _get_order_client(tokens: dict):
    return pyetrade.ETradeOrder(
        CONSUMER_KEY, CONSUMER_SECRET,
        tokens['oauth_token'], tokens['oauth_token_secret'],
        dev=is_sandbox
    )

async def _renew_tokens_if_needed(tokens: dict) -> dict:
    try:
        am = pyetrade.authorization.ETradeAccessManager(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret']
        )
        new_tokens = am.renew_access_token()
        if new_tokens and 'oauth_token' in new_tokens:
            save_tokens(new_tokens)
            logger.info("✅ Tokens automatically renewed")
            return new_tokens
    except Exception as e:
        logger.warning(f"Token renewal failed: {e}")
    return tokens

# ==================== LIVE TRADING ====================
async def execute_live_order(payload: dict):
    tokens = load_tokens()
    if not tokens:
        raise Exception("No E*TRADE tokens available")

    account_id_key = await get_resolved_account_id()

    ticker = payload.get("ticker")
    action = payload.get("action", "BUY").upper()
    instrument = payload.get("instrument", "stock").lower()
    mode = payload.get("mode", "paper").lower()
    quantity = int(payload.get("quantity", 1))
    client_order_id = str(uuid.uuid4())[:20]

    strike = payload.get("strike") or payload.get("strike_hint")
    expiry = (
        payload.get("expiry")
        or payload.get("expiration_hint")
        or payload.get("expiration_date")
    )

    if not expiry and payload.get("expiration_year"):
        y = payload.get("expiration_year")
        m = payload.get("expiration_month", 1)
        d = payload.get("expiration_day", 1)
        expiry = f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"

    # Stop / Trailing Stop parameters
    stop_price = payload.get("stop_price")
    trailing_stop_amount = payload.get("trailing_stop_amount")
    trailing_stop_percent = payload.get("trailing_stop_percent")

    logger.info(f"📥 Received signal → mode={mode}, instrument={instrument}, ticker={ticker}, action={action}")

    if mode != "live":
        return {"status": "paper"}

    try:
        if instrument == "option":
            if not strike or not expiry:
                raise Exception(f"Missing strike or expiry. Got strike={strike}, expiry={expiry}")

            call_put = "CALL" if str(payload.get("call_put", "call")).lower() == "call" else "PUT"
            order_action = "BUY_OPEN" if action == "BUY" else "SELL_CLOSE"

            expiry_str, strike_price = _snap_to_real_contract(
                ticker, str(expiry), float(strike), call_put
            )

            # Determine price type and stop parameters
            price_type = "MARKET"
            stop_params = {}

            if trailing_stop_amount:
                price_type = "TRAILING_STOP"
                stop_params = {"trailingStopAmount": float(trailing_stop_amount)}
                logger.info(f"Using TRAILING STOP with amount: {trailing_stop_amount}")
            elif trailing_stop_percent:
                price_type = "TRAILING_STOP"
                # E*TRADE sometimes expects amount; convert percent if needed
                stop_params = {"trailingStopAmount": float(trailing_stop_percent)}
                logger.info(f"Using TRAILING STOP with percent: {trailing_stop_percent}")
            elif stop_price:
                price_type = "STOP"
                stop_params = {"stopPrice": float(stop_price)}
                logger.info(f"Using STOP order at: {stop_price}")

            logger.info(f"🚀 LIVE OPTION ORDER: {order_action} {quantity} {ticker} {call_put} {strike_price} {expiry_str} | type={price_type}")

            orders = _get_order_client(tokens)

            try:
                final = await asyncio.to_thread(
                    orders.place_option_order,
                    resp_format="json",
                    accountIdKey=account_id_key,
                    symbol=ticker,
                    callPut=call_put,
                    expiryDate=expiry_str,
                    strikePrice=strike_price,
                    orderAction=order_action,
                    clientOrderId=client_order_id,
                    priceType=price_type,
                    quantity=quantity,
                    orderTerm="GOOD_FOR_DAY",
                    marketSession="REGULAR",
                    **stop_params
                )
            except Exception as e:
                if "401" in str(e) or "Unauthorized" in str(e):
                    logger.warning("Got 401 on option order — attempting renewal...")
                    tokens = await _renew_tokens_if_needed(tokens)
                    orders = _get_order_client(tokens)
                    final = await asyncio.to_thread(
                        orders.place_option_order,
                        resp_format="json",
                        accountIdKey=account_id_key,
                        symbol=ticker,
                        callPut=call_put,
                        expiryDate=expiry_str,
                        strikePrice=strike_price,
                        orderAction=order_action,
                        clientOrderId=client_order_id,
                        priceType=price_type,
                        quantity=quantity,
                        orderTerm="GOOD_FOR_DAY",
                        marketSession="REGULAR",
                        **stop_params
                    )
                else:
                    raise
            return {"status": "success", "result": final}

        else:
            logger.info(f"🚀 LIVE EQUITY ORDER: {action} {quantity} {ticker}")

            orders = _get_order_client(tokens)

            try:
                final = await asyncio.to_thread(
                    orders.place_equity_order,
                    resp_format="json",
                    accountIdKey=account_id_key,
                    symbol=ticker,
                    orderAction=action,
                    clientOrderId=client_order_id,
                    priceType="MARKET",
                    quantity=quantity,
                    orderTerm="GOOD_FOR_DAY",
                    marketSession="REGULAR",
                )
            except Exception as e:
                if "401" in str(e) or "Unauthorized" in str(e):
                    logger.warning("Got 401 on equity order — attempting renewal...")
                    tokens = await _renew_tokens_if_needed(tokens)
                    orders = _get_order_client(tokens)
                    final = await asyncio.to_thread(
                        orders.place_equity_order,
                        resp_format="json",
                        accountIdKey=account_id_key,
                        symbol=ticker,
                        orderAction=action,
                        clientOrderId=client_order_id,
                        priceType="MARKET",
                        quantity=quantity,
                        orderTerm="GOOD_FOR_DAY",
                        marketSession="REGULAR",
                    )
                else:
                    raise
            return {"status": "success", "result": final}

    except Exception as e:
        logger.error(f"❌ LIVE TRADE FAILED: {e}")
        raise

# ==================== MODELS ====================
class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra='allow')

    secret: str
    ticker: str
    action: str
    mode: Optional[str] = "paper"
    instrument: Optional[str] = "stock"
    quantity: Optional[int] = 1
    strike: Optional[float] = None
    strike_hint: Optional[float] = None
    expiry: Optional[str] = None
    expiration_hint: Optional[str] = None
    expiration_date: Optional[str] = None
    expiration_year: Optional[int] = None
    expiration_month: Optional[int] = None
    expiration_day: Optional[int] = None
    call_put: Optional[str] = None
    option_right: Optional[str] = None
    option_contracts: Optional[int] = None
    option_limit_price: Optional[float] = None
    # Stop / Trailing Stop
    stop_price: Optional[float] = None
    trailing_stop_amount: Optional[float] = None
    trailing_stop_percent: Optional[float] = None

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
        oauth = OAuth1Session(
            client_key=CONSUMER_KEY,
            client_secret=CONSUMER_SECRET,
            callback_uri="oob"
        )
        request_token = oauth.fetch_request_token(
            "https://api.etrade.com/oauth/request_token"
        )

        oauth_token = request_token["oauth_token"]
        oauth_token_secret = request_token["oauth_token_secret"]

        _cleanup_pending_tokens()
        pending_request_tokens[oauth_token] = {
            "timestamp": datetime.utcnow(),
            "secret": oauth_token_secret
        }

        encoded_token = urllib.parse.quote(oauth_token, safe='')
        authorize_url = f"https://us.etrade.com/e/t/etws/authorize?key={CONSUMER_KEY}&token={encoded_token}"

        logger.info(f"✅ [v{VERSION}] Request token generated | token_len={len(oauth_token)}")
        return {"authorize_url": authorize_url, "request_token": oauth_token}
    except Exception as e:
        logger.error(f"Start linking failed: {e}")
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_linking(
    verifier: str = Body(..., embed=True),
    request_token: Optional[str] = Body(None, embed=True)
):
    if not request_token:
        raise HTTPException(400, "request_token is required")

    _cleanup_pending_tokens()

    if request_token not in pending_request_tokens:
        raise HTTPException(409, "No matching request token. Please start again.")

    entry = pending_request_tokens[request_token]
    if datetime.utcnow() - entry["timestamp"] > REQUEST_TOKEN_TTL:
        del pending_request_tokens[request_token]
        raise HTTPException(400, "Request token expired. Please start again.")

    try:
        oauth_token_secret = entry["secret"]

        oauth = OAuth1Session(
            client_key=CONSUMER_KEY,
            client_secret=CONSUMER_SECRET,
            resource_owner_key=request_token,
            resource_owner_secret=oauth_token_secret
        )

        access_token = oauth.fetch_access_token(
            "https://api.etrade.com/oauth/access_token",
            verifier=verifier
        )

        tokens = {
            "oauth_token": access_token["oauth_token"],
            "oauth_token_secret": access_token["oauth_token_secret"]
        }

        del pending_request_tokens[request_token]
        save_tokens(tokens)

        try:
            await get_resolved_account_id()
        except Exception as e:
            logger.warning(f"Could not resolve account key yet: {e}")

        logger.info("=== NEW TOKENS RECEIVED ===")
        return {
            "status": "success",
            "linked": True,
            "env": ENV,
            "access_token": tokens["oauth_token"],
            "access_token_secret": tokens["oauth_token_secret"]
        }
    except Exception as e:
        logger.error(f"Complete link failed: {e}")
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    tokens = load_tokens()
    if not tokens:
        return {"status": "not_linked", "linked": False}
    try:
        accounts_client = pyetrade.ETradeAccounts(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret'],
            dev=is_sandbox
        )
        raw = accounts_client.list_accounts()
        account_list = []
        if raw and 'AccountListResponse' in raw:
            accs = raw['AccountListResponse'].get('Accounts', {}).get('Account', [])
            if not isinstance(accs, list):
                accs = [accs]
            for a in accs:
                account_list.append({
                    "accountIdKey": a.get("accountIdKey"),
                    "accountId": a.get("accountId"),
                    "accountType": a.get("accountType")
                })
        return {"status": "linked", "linked": True, "accounts": account_list}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/etrade/auth/renew")
async def renew_tokens():
    tokens = load_tokens()
    if not tokens:
        raise HTTPException(400, "No tokens to renew")
    try:
        am = pyetrade.authorization.ETradeAccessManager(
            CONSUMER_KEY, CONSUMER_SECRET,
            tokens['oauth_token'], tokens['oauth_token_secret']
        )
        new_tokens = am.renew_access_token()
        if new_tokens:
            save_tokens(new_tokens)
            return {"status": "success", "new_tokens": new_tokens}
        return {"status": "failed"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/etrade/disconnect")
async def disconnect():
    global _current_tokens, _resolved_account_id
    _current_tokens = {}
    _resolved_account_id = None
    return {"status": "disconnected"}

@app.get("/etrade/quote")
async def get_quote(symbols: str = Query(...)):
    tokens = load_tokens()
    if not tokens:
        raise HTTPException(401, "Not linked")

    try:
        market = _get_market_client(tokens)
        return market.get_quote(symbols.split(","), resp_format="json")
    except Exception as e:
        if "401" in str(e) or "Unauthorized" in str(e):
            logger.warning("Got 401 on quote — attempting renewal...")
            tokens = await _renew_tokens_if_needed(tokens)
            market = _get_market_client(tokens)
            try:
                return market.get_quote(symbols.split(","), resp_format="json")
            except Exception as e2:
                logger.error(f"Quote still failing after renewal: {e2}")
                raise HTTPException(401, "Token renewal failed. Please re-link.")
        logger.error(f"Quote failed: {e}")
        raise HTTPException(500, str(e))

@app.get("/health")
async def health():
    tokens = load_tokens()
    return {
        "status": "ok",
        "version": VERSION,
        "env": ENV,
        "live_trading": LIVE_TRADING,
        "linked": bool(tokens),
        "target_account_set": bool(TARGET_ACCOUNT_ID),
        "resolved_account_key": _resolved_account_id
    }

@app.on_event("startup")
async def on_startup():
    logger.info(f"Starting → PRODUCTION | LIVE={LIVE_TRADING} | version={VERSION}")
    if not TARGET_ACCOUNT_ID:
        logger.warning("⚠️ TARGET_ACCOUNT_ID not set (will auto-resolve after linking)")
    if REDIS_URL:
        try:
            global redis
            redis = await redis_from_url(REDIS_URL, decode_responses=True)
        except:
            logger.warning("No REDIS_URL set — running without Redis queue")
    await init_db()
    logger.info("✅ Bot ready")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main_bot:app", host="0.0.0.0", port=port)
