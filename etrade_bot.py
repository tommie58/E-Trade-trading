from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

import pyetrade
import os
import json
import logging
import uuid
import time
import traceback

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

# =========================================================
# GLOBALS
# =========================================================
recent_orders = {}
broker_down_until = 0

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

def is_duplicate(key: str, seconds: int = 30) -> bool:
    now = time.time()
    if key in recent_orders and (now - recent_orders[key]) < seconds:
        return True
    recent_orders[key] = now
    return False

def load_session():
    try:
        if not os.path.exists(TOKENS_FILE):
            raise Exception("Token file missing")

        with open(TOKENS_FILE) as f:
            tokens = json.load(f)

        oauth_token = tokens.get("oauth_token")
        oauth_secret = tokens.get("oauth_token_secret")

        if not oauth_token or not oauth_secret:
            raise Exception("Invalid OAuth tokens")

        consumer_key = os.getenv("ETRADE_CONSUMER_KEY")
        consumer_secret = os.getenv("ETRADE_CONSUMER_SECRET")

        order_session = pyetrade.ETradeOrder(
            consumer_key, consumer_secret,
            oauth_token, oauth_secret,
            dev=dev_mode
        )

        accounts = pyetrade.ETradeAccounts(
            consumer_key, consumer_secret,
            oauth_token, oauth_secret,
            dev=dev_mode
        )

        acct_list = accounts.list_accounts()
        account_list = acct_list["AccountListResponse"]["Accounts"]["Account"]

        selected_account = next(
            (acct for acct in account_list if TARGET_ACCOUNT_ID is None or acct["accountIdKey"] == TARGET_ACCOUNT_ID),
            None
        )

        if not selected_account:
            raise Exception("Target account not found")

        account_id_key = selected_account["accountIdKey"]
        logger.info(f"✅ Loaded account: {account_id_key}")
        return order_session, account_id_key

    except Exception:
        logger.exception("❌ Failed to load session")
        return None, None

def classify_error(error_msg: str) -> str:
    error_msg = error_msg.lower()
    if "code: 100" in error_msg or "temporarily unavailable" in error_msg:
        return "broker_unavailable"
    return "other_error"

# =========================================================
# ROUTES
# =========================================================
@app.get("/")
async def root():
    return {"status": "running", "env": ENV}

# =========================================================
# E*TRADE LINKING ROUTES (NEW)
# =========================================================
@app.post("/etrade/auth/start")
async def start_auth():
    try:
        url = oauth.get_request_token()
        logger.info("✅ Auth start successful")
        return {"authorize_url": url}
    except Exception as e:
        logger.exception("❌ Auth start failed")
        raise HTTPException(500, f"Failed to start linking: {str(e)}")

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or data).strip()

        tokens = oauth.get_access_token(verifier)

        with open(TOKENS_FILE, "w") as f:
            json.dump(tokens, f)

        logger.info("✅ E*TRADE account linked successfully")
        return {"status": "linked", "message": "Account linked successfully"}

    except Exception as e:
        logger.exception("❌ Auth complete failed")
        raise HTTPException(500, f"Failed to complete linking: {str(e)}")

@app.get("/etrade/account")
async def get_account_status():
    try:
        if not os.path.exists(TOKENS_FILE):
            return {"status": "not_linked"}

        with open(TOKENS_FILE) as f:
            tokens = json.load(f)

        if tokens.get("oauth_token") and tokens.get("oauth_token_secret"):
            return {"status": "linked"}
        else:
            return {"status": "not_linked"}

    except Exception:
        return {"status": "not_linked"}

# =========================================================
# WEBHOOK
# =========================================================
@app.post("/webhook")
async def webhook(request: Request):
    global broker_down_until

    try:
        if time.time() < broker_down_until:
            return {
                "status": "cooldown",
                "message": "E*TRADE temporarily unavailable. Please try again later."
            }

        payload = await request.json()
        logger.info(f"📥 PAYLOAD:\n{json.dumps(payload, indent=2)}")

        if payload.get("secret") != WEBHOOK_SECRET:
            raise HTTPException(403, "Unauthorized")

        ticker = str(payload.get("ticker", "")).upper()
        raw_action = str(payload.get("action", "")).upper()
        instrument = str(payload.get("instrument", "stock")).lower()
        mode = str(payload.get("mode", "paper")).lower()

        if not ticker:
            raise HTTPException(400, "Missing ticker")

        if raw_action not in ["BUY", "SELL", "EXIT", "CLOSE"]:
            raise HTTPException(400, f"Invalid action: {raw_action}")

        # Duplicate protection
        if instrument == "option":
            strike = payload.get("strike_hint") or payload.get("strike")
            expiry = payload.get("expiration_hint") or payload.get("expiry")
            duplicate_key = f"{ticker}_{raw_action}_{strike}_{expiry}"
        else:
            duplicate_key = f"{ticker}_{raw_action}"

        if is_duplicate(duplicate_key):
            logger.warning(f"⚠️ Duplicate signal blocked: {duplicate_key}")
            return {"status": "ignored", "message": "Duplicate signal blocked"}

        session, account_id_key = load_session()
        if not session:
            return {"status": "failed", "message": "Session unavailable"}

        client_order_id = str(uuid.uuid4())

        # =========================================================
        # OPTION ORDERS
        # =========================================================
        if instrument == "option":

            if mode == "live" and not LIVE_TRADING:
                return {"status": "paper_only"}

            contracts = int(payload.get("option_contracts") or payload.get("contracts") or 0)
            call_put = str(payload.get("option_right", "")).upper()
            strike = float(payload.get("strike_hint") or payload.get("strike") or 0)
            limit_price = float(payload.get("limit_price") or payload.get("entry") or 0)
            expiry = payload.get("expiration_hint") or payload.get("expiry")

            if contracts <= 0:
                raise HTTPException(400, "Invalid contracts quantity")
            contracts = min(contracts, MAX_CONTRACTS)

            if call_put not in ["CALL", "PUT"]:
                raise HTTPException(400, "Invalid option_right")
            if strike <= 0 or limit_price <= 0:
                raise HTTPException(400, "Invalid strike or limit price")
            if not expiry:
                raise HTTPException(400, "Missing expiration_hint")

            dt = datetime.strptime(expiry, "%Y-%m-%d")
            if dt.date() < datetime.utcnow().date():
                raise HTTPException(400, "Contract already expired")

            action = "BUY_OPEN" if raw_action == "BUY" else "SELL_CLOSE"
            occ_symbol = build_occ_symbol(ticker, expiry, call_put, strike)

            logger.info(f"🚀 OPTION SIGNAL: {action} {contracts} {occ_symbol} @ {limit_price}")

            MAX_RETRIES = 5
            last_error = None

            for attempt in range(MAX_RETRIES):
                try:
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

                    logger.info(f"✅ ORDER PLACED successfully on attempt {attempt + 1}")
                    return {"status": "success", "attempt": attempt + 1, "order": order}

                except Exception as e:
                    last_error = str(e)
                    error_type = classify_error(last_error)

                    logger.error(f"❌ Attempt {attempt + 1} failed: {last_error}")

                    if error_type == "broker_unavailable" and attempt < MAX_RETRIES - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"⏳ Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue

                    if error_type == "broker_unavailable":
                        broker_down_until = time.time() + 300
                        return {
                            "status": "broker_unavailable",
                            "message": "E*TRADE temporarily unavailable",
                            "retry_after_seconds": 300
                        }

                    return {"status": "failed", "error": last_error}

        # =========================================================
        # STOCK ORDERS
        # =========================================================
        else:
            shares = int(payload.get("position_size_shares") or 0)
            if shares <= 0:
                raise HTTPException(400, "Invalid share quantity")

            logger.info(f"🚀 STOCK SIGNAL: {raw_action} {shares} {ticker}")

            MAX_RETRIES = 3
            for attempt in range(MAX_RETRIES):
                try:
                    preview = session.preview_equity_order(
                        accountIdKey=account_id_key,
                        symbol=ticker,
                        orderAction=raw_action,
                        quantity=str(shares),
                        priceType="MARKET",
                        marketSession="REGULAR",
                        orderTerm="GOOD_FOR_DAY"
                    )

                    preview_ids = preview["PreviewOrderResponse"]["PreviewIds"]["previewId"]
                    preview_id = preview_ids[0]["previewId"] if isinstance(preview_ids, list) else preview_ids["previewId"]

                    if mode == "live":
                        order = session.place_equity_order(
                            accountIdKey=account_id_key,
                            previewId=preview_id
                        )
                        return {"status": "success", "order": order}

                    return {"status": "paper_only"}

                except Exception as e:
                    last_error = str(e)
                    if classify_error(last_error) == "broker_unavailable" and attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        continue

                    if classify_error(last_error) == "broker_unavailable":
                        broker_down_until = time.time() + 300
                        return {"status": "broker_unavailable", "message": "E*TRADE temporarily unavailable"}

                    return {"status": "failed", "error": last_error}

    except HTTPException as he:
        raise he

    except Exception as e:
        logger.error("❌ WEBHOOK FAILURE")
        traceback.print_exc()
        return {"status": "failed", "message": str(e)}
