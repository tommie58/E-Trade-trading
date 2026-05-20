from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

import pyetrade
import os
import json
import logging
import uuid

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etrade-bot")

app = FastAPI(title="E*TRADE Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
        order_session = pyetrade.ETradeOrder(consumer_key, consumer_secret, tokens["oauth_token"], tokens["oauth_token_secret"], dev=dev_mode)
        accounts = pyetrade.ETradeAccounts(consumer_key, consumer_secret, tokens["oauth_token"], tokens["oauth_token_secret"], dev=dev_mode)
        acct_list = accounts.list_accounts()
        account_list = acct_list["AccountListResponse"]["Accounts"]["Account"]
        selected_account = next((acct for acct in account_list if TARGET_ACCOUNT_ID is None or acct["accountIdKey"] == TARGET_ACCOUNT_ID), None)
        if not selected_account:
            raise Exception("Target account not found")
        account_id_key = selected_account["accountIdKey"]
        logger.info(f"✅ Loaded account: {account_id_key}")
        return order_session, account_id_key
    except Exception as e:
        logger.exception("Load session failed")
        return None, None

# =========================================================
# ENDPOINTS
# =========================================================
@app.get("/")
async def root():
    return {"status": "running", "env": ENV}

@app.post("/etrade/auth/start")
async def start_auth():
    url = oauth.get_request_token()
    return {"authorize_url": url}

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    data = await request.json()
    verifier = str(data.get("verifier") or data.get("code") or data).strip()
    tokens = oauth.get_access_token(verifier)
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f)
    logger.info("✅ Tokens saved")
    return {"status": "linked"}

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked"}

# =========================================================
# WEBHOOK
# =========================================================
@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        logger.info(f"📥 FULL PAYLOAD:\n{json.dumps(payload, indent=2)}")

        if payload.get("secret") != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        ticker = payload.get("ticker")
        raw_action = payload.get("action", "").upper()
        instrument = payload.get("instrument", "stock").lower()
        mode = payload.get("mode", "paper").lower()

        if not ticker:
            raise HTTPException(400, "Missing ticker")

        session, account_id_key = load_session()
        if not session:
            raise HTTPException(500, "Session unavailable")

        client_order_id = str(uuid.uuid4())

        if instrument == "option":

            # LIVE TRADING SAFETY
            if mode == "live" and not LIVE_TRADING:
                logger.warning("⚠️ Live trading blocked by LIVE_TRADING flag")
                return {"status": "paper_only"}

            # EXTRACT PAYLOAD
            contracts = int(payload.get("option_contracts") or payload.get("contracts") or 0)
            call_put = payload.get("option_right", "").upper()
            strike = float(payload.get("strike_hint") or payload.get("strike") or 0)
            # FIXED: fallback to "entry" if limit_price is missing
            limit_price = float(payload.get("limit_price") or payload.get("entry") or 0)
            expiry = payload.get("expiration_hint") or payload.get("expiry")

            # VALIDATION
            if contracts <= 0:
                raise HTTPException(400, "Invalid contracts quantity")
            contracts = min(contracts, MAX_CONTRACTS)
            if call_put not in ["CALL", "PUT"]:
                raise HTTPException(400, "Invalid option_right")
            if strike <= 0:
                raise HTTPException(400, "Invalid strike")
            if limit_price <= 0:
                raise HTTPException(400, "Invalid limit price (no limit_price or entry provided)")
            if not expiry:
                raise HTTPException(400, "Missing expiration_hint")

            dt = datetime.strptime(expiry, "%Y-%m-%d")
            if dt.date() < datetime.utcnow().date():
                raise HTTPException(400, "Contract already expired")

            # ORDER ACTION
            if raw_action == "BUY":
                action = "BUY_OPEN"
            elif raw_action in ["SELL", "EXIT", "CLOSE"]:
                action = "SELL_CLOSE"
            else:
                raise HTTPException(400, f"Unsupported action: {raw_action}")

            occ_symbol = build_occ_symbol(ticker, expiry, call_put, strike)

            logger.info(f"🚀 OPTION SIGNAL: {action} {contracts} {occ_symbol} @ {limit_price}")

            # DIRECT PLACE (no preview — pyetrade doesn't support it)
            order = session.place_option_order(
                accountIdKey=account_id_key,
                symbol=occ_symbol,
                orderAction=action,
                quantity=str(contracts),
                priceType="LIMIT",
                limitPrice=round(limit_price, 2),
                callPut=call_put,
                strikePrice=float(strike),
                expiryDate=expiry,
                expiryYear=dt.year,
                expiryMonth=dt.month,
                expiryDay=dt.day,
                routingDestination="AUTO",
                marketSession="REGULAR",
                orderTerm="GOOD_FOR_DAY",
                allOrNone=False,
                reserveOrder=False,
                clientOrderId=client_order_id
            )

            logger.info(f"✅ OPTION ORDER PLACED:\n{json.dumps(order, indent=2)}")
            return {"status": "success", "order": order}

        else:
            # STOCK FALLBACK
            action = raw_action
            shares = int(payload.get("position_size_shares") or 0)
            if shares <= 0:
                raise HTTPException(400, "Invalid shares quantity")

            preview = session.preview_equity_order(
                accountIdKey=account_id_key,
                symbol=ticker,
                orderAction=action,
                quantity=str(shares),
                priceType="MARKET",
                marketSession="REGULAR",
                orderTerm="GOOD_FOR_DAY",
                allOrNone=False,
                reserveOrder=False
            )

            preview_ids = preview["PreviewOrderResponse"]["PreviewIds"]["previewId"]
            preview_id = preview_ids[0]["previewId"] if isinstance(preview_ids, list) else preview_ids["previewId"]

            if mode == "live":
                order = session.place_equity_order(
                    accountIdKey=account_id_key,
                    previewId=preview_id
                )
            else:
                return {"status": "paper_only"}

        return {"status": "success", "order": order}

    except Exception as e:
        logger.exception("ORDER FAILURE")
        raise HTTPException(500, f"Order failed: {str(e)}")
