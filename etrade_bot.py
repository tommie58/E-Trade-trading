from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

import pyetrade
import os
import json
import logging
import asyncio

from datetime import datetime
import pytz

# =========================================================
# CONFIG
# =========================================================

TOKENS_FILE = ".etrade_tokens.json"

ENV = os.getenv("ETRADE_ENV", "sandbox")

LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

TARGET_ACCOUNT_ID = os.getenv("ETRADE_ACCOUNT_ID")

MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "5"))

dev_mode = ENV == "sandbox"

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

# =========================================================
# FASTAPI
# =========================================================

app = FastAPI(title="E*TRADE Options Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================================================
# OAUTH
# =========================================================

oauth = pyetrade.ETradeOAuth(
    os.getenv("ETRADE_CONSUMER_KEY"),
    os.getenv("ETRADE_CONSUMER_SECRET")
)

# =========================================================
# HELPERS
# =========================================================

def build_occ_symbol(ticker, expiry, call_put, strike):
    dt = datetime.strptime(expiry, "%Y-%m-%d")
    yy = dt.strftime("%y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    cp = "C" if call_put == "CALL" else "P"
    strike_formatted = f"{int(float(strike) * 1000):08d}"
    return f"{ticker.upper():<6}{yy}{mm}{dd}{cp}{strike_formatted}"

def validate_market_hours():
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)
    if now.weekday() >= 5:
        raise HTTPException(400, "Market closed (weekend)")
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        raise HTTPException(400, "Market not open")
    if now.hour >= 16:
        raise HTTPException(400, "Market closed")

def load_session():
    try:
        with open(TOKENS_FILE) as f:
            tokens = json.load(f)

        consumer_key = os.getenv("ETRADE_CONSUMER_KEY")
        consumer_secret = os.getenv("ETRADE_CONSUMER_SECRET")

        order_session = pyetrade.ETradeOrder(
            consumer_key,
            consumer_secret,
            tokens["oauth_token"],
            tokens["oauth_token_secret"],
            dev=dev_mode
        )

        accounts = pyetrade.ETradeAccounts(
            consumer_key,
            consumer_secret,
            tokens["oauth_token"],
            tokens["oauth_token_secret"],
            dev=dev_mode
        )

        acct_list = accounts.list_accounts()
        account_list = acct_list["AccountListResponse"]["Accounts"]["Account"]

        selected_account = None
        for acct in account_list:
            if TARGET_ACCOUNT_ID is None or acct["accountIdKey"] == TARGET_ACCOUNT_ID:
                selected_account = acct
                break

        if not selected_account:
            raise Exception("Target account not found")

        account_id_key = selected_account["accountIdKey"]

        logger.info(f"Loaded account: {account_id_key}")
        return order_session, account_id_key

    except Exception as e:
        logger.exception("Load session failed")
        return None, None

# =========================================================
# HEALTH CHECK
# =========================================================

@app.get("/")
async def root():
    return {
        "status": "running",
        "env": ENV,
        "live_trading": LIVE_TRADING
    }

# =========================================================
# AUTH ENDPOINTS (kept from previous version)
# =========================================================

@app.post("/etrade/auth/start")
async def start_auth():
    try:
        url = oauth.get_request_token()
        return {"authorize_url": url}
    except Exception as e:
        logger.exception("Auth start failed")
        raise HTTPException(500, "Auth start failed")

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or data).strip()
        tokens = oauth.get_access_token(verifier)
        with open(TOKENS_FILE, "w") as f:
            json.dump(tokens, f)
        logger.info("OAuth tokens saved")
        return {"status": "linked"}
    except Exception:
        logger.exception("Auth complete failed")
        raise HTTPException(500, "Auth complete failed")

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked"}

# =========================================================
# ADVANCED OPTION WEBHOOK
# =========================================================

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()

        # WEBHOOK AUTH
        secret = payload.get("secret")
        if secret != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        # PARSE PAYLOAD
        ticker = payload.get("ticker")
        action = payload.get("action", "").upper()
        contracts = int(payload.get("contracts", 0))
        call_put = payload.get("call_put", "").upper()
        strike = float(payload.get("strike"))
        limit_price = float(payload.get("limit_price"))
        expiry = payload.get("expiry")

        # VALIDATION
        if not ticker:
            raise HTTPException(400, "Missing ticker")
        if action not in ["BUY_OPEN", "SELL_CLOSE", "SELL_OPEN", "BUY_CLOSE"]:
            raise HTTPException(400, f"Invalid action: {action}")
        if contracts <= 0:
            raise HTTPException(400, "contracts must be > 0")
        if contracts > MAX_CONTRACTS:
            raise HTTPException(400, "Contract limit exceeded")
        if call_put not in ["CALL", "PUT"]:
            raise HTTPException(400, "Invalid call_put")
        if strike <= 0:
            raise HTTPException(400, "Invalid strike")
        if limit_price <= 0:
            raise HTTPException(400, "Invalid limit price")

        # MARKET HOURS CHECK
        validate_market_hours()

        # EXPIRATION PARSING
        dt = datetime.strptime(expiry, "%Y-%m-%d")
        if dt.date() <= datetime.utcnow().date():
            raise HTTPException(400, "Option expiration invalid")

        expiry_year = dt.year
        expiry_month = dt.month
        expiry_day = dt.day

        # OCC SYMBOL
        occ_symbol = build_occ_symbol(ticker, expiry, call_put, strike)

        logger.info(f"SIGNAL: {action} {contracts} {occ_symbol} LIMIT={limit_price}")

        # LOAD SESSION
        session, account_id_key = load_session()
        if not session:
            raise HTTPException(500, "Session unavailable")

        # CLIENT ORDER ID
        client_order_id = str(int(datetime.utcnow().timestamp()))

        # PREVIEW OPTION ORDER
        preview = session.preview_option_order(
            account_id_key=account_id_key,
            client_order_id=client_order_id,
            symbol=occ_symbol,
            order_action=action,
            quantity=str(contracts),
            price_type="LIMIT",
            limit_price=round(limit_price, 2),
            call_put=call_put,
            strike_price=float(strike),
            expiry_year=int(expiry_year),
            expiry_month=int(expiry_month),
            expiry_day=int(expiry_day),
            routing_destination="AUTO",
            market_session="REGULAR",
            order_term="GOOD_FOR_DAY",
            all_or_none=False,
            reserve_order=False
        )

        logger.info(f"PREVIEW RESPONSE:\n{json.dumps(preview, indent=2)}")

        if "PreviewOrderResponse" not in preview:
            return {"status": "error", "reason": "preview_failed", "details": preview}

        # EXTRACT PREVIEW ID
        preview_ids = preview["PreviewOrderResponse"]["PreviewIds"]["previewId"]
        preview_id = preview_ids[0]["previewId"] if isinstance(preview_ids, list) else preview_ids["previewId"]

        logger.info(f"PREVIEW ID: {preview_id}")

        # PAPER MODE
        if not LIVE_TRADING:
            return {"status": "paper_only", "preview": preview}

        # PLACE ORDER
        order = session.place_option_order(
            account_id_key=account_id_key,
            preview_id=preview_id,
            client_order_id=client_order_id
        )

        logger.info(f"ORDER RESPONSE:\n{json.dumps(order, indent=2)}")

        return {"status": "success", "order": order}

    except HTTPException:
        raise
    except Exception:
        logger.exception("ORDER FAILURE")
        raise HTTPException(500, "Order execution failed")
