from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

import pyetrade
import os
import json
import logging
import uuid
import time
import traceback
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
ENABLE_MARKET_HOURS_CHECK = os.getenv("ENABLE_MARKET_HOURS_CHECK", "false").lower() == "true"
BROKER_TIMEOUT_SECONDS = int(os.getenv("BROKER_TIMEOUT_SECONDS", "25"))
VERIFY_POSITIONS_ON_CLOSE = os.getenv("VERIFY_POSITIONS_ON_CLOSE", "false").lower() == "true"
REJECT_0_DTE = os.getenv("REJECT_0_DTE", "false").lower() == "true"      # Default = false (allowed)
ZERO_DTE_DELAY_SECONDS = int(os.getenv("ZERO_DTE_DELAY_SECONDS", "15"))  # Wait before first attempt

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
def validate_market_hours():
    if not ENABLE_MARKET_HOURS_CHECK:
        return
    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)
    if now.weekday() >= 5:
        raise HTTPException(400, "Market is closed (weekend)")
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        raise HTTPException(400, "Market is not open yet")
    if now.hour >= 16:
        raise HTTPException(400, "Market is closed")

def build_occ_symbol(ticker, expiry, call_put, strike, days_to_expiry=0):
    dt = datetime.strptime(expiry, "%Y-%m-%d")
    yy = dt.strftime("%y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    cp = "C" if call_put == "CALL" else "P"
    
    # For 0 DTE, round to nearest 5 (much more likely to exist)
    if days_to_expiry == 0:
        strike_rounded = round(float(strike) / 5) * 5
    else:
        strike_rounded = round(float(strike) * 2) / 2
    
    strike_formatted = f"{int(strike_rounded * 1000):08d}"
    return f"{ticker.upper()}{yy}{mm}{dd}{cp}{strike_formatted}"

def is_duplicate(key: str, seconds: int = 30) -> bool:
    now = time.time()
    for k in list(recent_orders.keys()):
        if now - recent_orders[k] > seconds:
            recent_orders.pop(k, None)
    if key in recent_orders:
        return True
    recent_orders[key] = now
    return False

def classify_error(error_msg: str) -> str:
    msg = error_msg.lower()
    if any(kw in msg for kw in ["code: 100", "temporarily unavailable", "gateway timeout", "service unavailable"]):
        return "broker_unavailable"
    if any(kw in msg for kw in ["oauth", "token", "unauthorized", "401"]):
        return "auth_error"
    return "other_error"

def extract_preview_id(preview_response):
    try:
        preview_ids = preview_response["PreviewOrderResponse"]["PreviewIds"]["previewId"]
        if isinstance(preview_ids, list):
            return preview_ids[0]["previewId"]
        return preview_ids["previewId"]
    except Exception:
        logger.error(f"Malformed preview response: {json.dumps(preview_response, indent=2)}")
        raise Exception("Failed to extract previewId")

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
            consumer_key, consumer_secret, oauth_token, oauth_secret, dev=dev_mode
        )
        accounts = pyetrade.ETradeAccounts(
            consumer_key, consumer_secret, oauth_token, oauth_secret, dev=dev_mode
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
        return order_session, accounts, account_id_key
    except Exception:
        logger.exception("❌ Failed to load session")
        return None, None, None

async def run_with_timeout(func, *args, **kwargs):
    return await asyncio.wait_for(
        asyncio.to_thread(func, *args, **kwargs),
        timeout=BROKER_TIMEOUT_SECONDS
    )

async def verify_option_position(accounts, account_id_key, occ_symbol, quantity):
    if not VERIFY_POSITIONS_ON_CLOSE:
        return True
    try:
        portfolio = await run_with_timeout(accounts.get_account_portfolio, account_id_key)
        positions = portfolio.get("PortfolioResponse", {}).get("AccountPortfolio", [])
        if isinstance(positions, dict):
            positions = [positions]
        total_qty = 0
        for acct in positions:
            for pos in acct.get("Position", []):
                if isinstance(pos, dict):
                    product = pos.get("Product", {})
                    if product.get("symbol", "").strip() == occ_symbol.strip():
                        total_qty += float(pos.get("quantity", 0))
        return total_qty >= quantity
    except Exception as e:
        logger.warning(f"Position verification failed (continuing anyway): {e}")
        return True

# =========================================================
# ROUTES
# =========================================================
@app.get("/")
async def root():
    return {"status": "running", "env": ENV}

@app.post("/etrade/auth/start")
async def start_auth():
    try:
        url = oauth.get_request_token()
        return {"authorize_url": url}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or data).strip()
        tokens = oauth.get_access_token(verifier)
        with open(TOKENS_FILE, "w") as f:
            json.dump(tokens, f)
        return {"status": "linked"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account_status():
    try:
        if not os.path.exists(TOKENS_FILE):
            return {"status": "not_linked"}
        with open(TOKENS_FILE) as f:
            tokens = json.load(f)
        return {"status": "linked" if tokens.get("oauth_token") else "not_linked"}
    except Exception:
        return {"status": "not_linked"}

@app.post("/etrade/disconnect")
async def disconnect():
    try:
        if os.path.exists(TOKENS_FILE):
            os.remove(TOKENS_FILE)
        return {"status": "disconnected"}
    except Exception as e:
        raise HTTPException(500, str(e))

# =========================================================
# WEBHOOK
# =========================================================
@app.post("/webhook")
async def webhook(request: Request):
    global broker_down_until

    try:
        if time.time() < broker_down_until:
            return {"status": "cooldown", "message": "E*TRADE temporarily unavailable"}

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

        if instrument == "option":
            strike = payload.get("strike_hint") or payload.get("strike")
            expiry = payload.get("expiration_hint") or payload.get("expiry")
            duplicate_key = f"{ticker}_{raw_action}_{strike}_{expiry}"
        else:
            duplicate_key = f"{ticker}_{raw_action}"

        if is_duplicate(duplicate_key):
            return {"status": "ignored", "message": "Duplicate signal blocked"}

        session, accounts, account_id_key = load_session()
        if not session:
            return {"status": "failed", "message": "Session unavailable"}

        validate_market_hours()
        client_order_id = uuid.uuid4().hex[:20]

        # ===================== OPTION ORDERS =====================
        if instrument == "option":
            if mode == "live" and not LIVE_TRADING:
                return {"status": "paper_only"}

            contracts = int(payload.get("option_contracts") or payload.get("contracts") or 0)
            call_put = str(payload.get("option_right", "")).upper()
            strike = float(payload.get("strike_hint") or payload.get("strike") or 0)
            limit_price = float(payload.get("limit_price") or payload.get("entry") or 0)
            expiry = payload.get("expiration_hint") or payload.get("expiry")
            days_to_expiry = int(payload.get("days_to_expiry_hint", 0))

            if contracts <= 0:
                raise HTTPException(400, "Invalid contracts quantity")
            contracts = min(contracts, MAX_CONTRACTS)
            if call_put not in ["CALL", "PUT"]:
                raise HTTPException(400, "Invalid option_right")
            if strike <= 0 or limit_price <= 0:
                raise HTTPException(400, "Invalid strike or limit price")
            if not expiry:
                raise HTTPException(400, "Missing expiration_hint")

            # 0 DTE handling
            if days_to_expiry == 0:
                logger.warning(f"⚠️ 0 DTE option detected for {ticker} {call_put} {strike} - these contracts may not exist yet in the morning")
                if REJECT_0_DTE:
                    raise HTTPException(400, "0 DTE options are not supported yet")
                # Short delay to give E*TRADE time to load the chain
                await asyncio.sleep(ZERO_DTE_DELAY_SECONDS)

            dt = datetime.strptime(expiry, "%Y-%m-%d")
            if dt.date() < datetime.utcnow().date():
                raise HTTPException(400, "Option already expired")

            action = "BUY_OPEN" if raw_action == "BUY" else "SELL_CLOSE"
            occ_symbol = build_occ_symbol(ticker, expiry, call_put, strike, days_to_expiry)

            if action == "SELL_CLOSE":
                has_position = await verify_option_position(accounts, account_id_key, occ_symbol, contracts)
                if not has_position:
                    raise HTTPException(400, "No matching option position found for SELL_CLOSE")

            logger.info(f"🚀 OPTION SIGNAL: {action} {contracts} {occ_symbol} @ {limit_price}")

            MAX_RETRIES = 5
            for attempt in range(MAX_RETRIES):
                try:
                    preview = await run_with_timeout(
                        session.preview_equity_order,
                        accountIdKey=account_id_key,
                        orderType="OPTN",
                        symbol=occ_symbol,
                        orderAction=action,
                        quantity=str(contracts),
                        priceType="LIMIT",
                        limitPrice=str(round(limit_price, 2)),
                        callPut=call_put,
                        strikePrice=float(strike),
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

                    logger.info(f"OPTION PREVIEW:\n{json.dumps(preview, indent=2)}")
                    preview_id = extract_preview_id(preview)

                    if mode == "live":
                        order = await run_with_timeout(
                            session.place_equity_order,
                            accountIdKey=account_id_key,
                            previewId=preview_id
                        )
                        logger.info(f"✅ OPTION ORDER PLACED:\n{json.dumps(order, indent=2)}")
                        return {"status": "success", "order": order}

                    return {"status": "paper_only", "preview": preview}

                except Exception as e:
                    last_error = str(e)
                    error_type = classify_error(last_error)
                    logger.error(f"OPTION ATTEMPT {attempt + 1} FAILED: {last_error}")

                    if error_type == "broker_unavailable" and attempt < MAX_RETRIES - 1:
                        wait = 2 ** attempt
                        logger.warning(f"⏳ Retrying in {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    if error_type == "broker_unavailable":
                        broker_down_until = time.time() + 300
                        return {"status": "broker_unavailable", "retry_after_seconds": 300}

                    return {"status": "failed", "error": last_error}

        # ===================== STOCK ORDERS =====================
        else:
            shares = int(payload.get("position_size_shares") or 0)
            if shares <= 0:
                raise HTTPException(400, "Invalid share quantity")

            stock_action = "SELL" if raw_action in ["EXIT", "CLOSE"] else raw_action
            logger.info(f"🚀 STOCK SIGNAL: {stock_action} {shares} {ticker}")

            MAX_RETRIES = 3
            for attempt in range(MAX_RETRIES):
                try:
                    preview = await run_with_timeout(
                        session.preview_equity_order,
                        accountIdKey=account_id_key,
                        orderType="EQ",
                        symbol=ticker,
                        orderAction=stock_action,
                        quantity=str(shares),
                        priceType="MARKET",
                        marketSession="REGULAR",
                        orderTerm="GOOD_FOR_DAY",
                        clientOrderId=client_order_id
                    )

                    logger.info(f"STOCK PREVIEW:\n{json.dumps(preview, indent=2)}")
                    preview_id = extract_preview_id(preview)

                    if mode == "live":
                        order = await run_with_timeout(
                            session.place_equity_order,
                            accountIdKey=account_id_key,
                            previewId=preview_id
                        )
                        logger.info(f"✅ STOCK ORDER PLACED:\n{json.dumps(order, indent=2)}")
                        return {"status": "success", "order": order}

                    return {"status": "paper_only"}

                except Exception as e:
                    last_error = str(e)
                    error_type = classify_error(last_error)
                    logger.error(f"STOCK ATTEMPT {attempt + 1} FAILED: {last_error}")

                    if error_type == "broker_unavailable" and attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue

                    if error_type == "broker_unavailable":
                        broker_down_until = time.time() + 300
                        return {"status": "broker_unavailable"}

                    return {"status": "failed", "error": last_error}

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error("❌ WEBHOOK FAILURE")
        traceback.print_exc()
        return {"status": "failed", "message": str(e)}
