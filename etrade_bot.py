from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json
from datetime import datetime

app = FastAPI(title="E*TRADE Bot")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

oauth = pyetrade.ETradeOAuth(
    os.getenv("ETRADE_CONSUMER_KEY"),
    os.getenv("ETRADE_CONSUMER_SECRET")
)

TOKENS_FILE = ".etrade_tokens.json"
ENV = os.getenv("ETRADE_ENV", "sandbox")

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
            tokens["oauth_token_secret"]
        )

        accounts = pyetrade.ETradeAccounts(
            consumer_key,
            consumer_secret,
            tokens["oauth_token"],
            tokens["oauth_token_secret"]
        )

        acct_list = accounts.list_accounts()
        account = acct_list["AccountListResponse"]["Accounts"]["Account"][0]
        account_id_key = account["accountIdKey"]

        print(f"✅ Loaded account: {account_id_key}")
        return order_session, account_id_key

    except Exception as e:
        print(f"❌ Load session failed: {e}")
        return None, None

@app.get("/")
async def root():
    return {"status": "✅ Bot is running!"}

@app.post("/etrade/auth/start")
async def start_auth():
    try:
        url = oauth.get_request_token()
        return {"authorize_url": url}
    except Exception as e:
        raise HTTPException(500, f"Start failed: {str(e)}")

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or data).strip()
        tokens = oauth.get_access_token(verifier)
        with open(TOKENS_FILE, "w") as f:
            json.dump(tokens, f)
        print("✅ Tokens saved")
        return {"status": "linked", "message": "✅ Linked!"}
    except Exception as e:
        raise HTTPException(500, f"Complete failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked"}

# =========================
# NEW OPTION WEBHOOK
# =========================
@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()

        # =========================
        # PARSE PAYLOAD
        # =========================
        ticker = payload.get("ticker")
        action = payload.get("action", "").upper()
        contracts = int(payload.get("contracts", 0))
        call_put = payload.get("call_put", "").upper()
        strike = float(payload.get("strike"))
        limit_price = float(payload.get("limit_price"))
        expiry = payload.get("expiry")

        # =========================
        # VALIDATION
        # =========================
        if not ticker:
            raise HTTPException(400, "Missing ticker")
        if action not in ["BUY_OPEN", "SELL_CLOSE", "SELL_OPEN", "BUY_CLOSE"]:
            raise HTTPException(400, f"Invalid action: {action}")
        if contracts <= 0:
            raise HTTPException(400, "contracts must be > 0")
        if call_put not in ["CALL", "PUT"]:
            raise HTTPException(400, "call_put must be CALL or PUT")
        if strike <= 0:
            raise HTTPException(400, "Invalid strike")
        if limit_price <= 0:
            raise HTTPException(400, "Invalid limit price")

        # =========================
        # PARSE EXPIRATION
        # =========================
        dt = datetime.strptime(expiry, "%Y-%m-%d")
        expiry_year = dt.year
        expiry_month = dt.month
        expiry_day = dt.day

        print(f"🚀 OPTION SIGNAL: {action} {contracts} {ticker} {call_put} {strike}")
        print(f"EXP: {expiry} | LIMIT: {limit_price}")

        # =========================
        # LOAD SESSION
        # =========================
        session, account_id_key = load_session()
        if not session or not account_id_key:
            print("❌ No valid session")
            return {"status": "error", "reason": "not_linked"}

        # =========================
        # PREVIEW OPTION ORDER
        # =========================
        preview = session.preview_option_order(
            account_id_key=account_id_key,
            symbol=ticker,
            order_action=action,
            quantity=contracts,
            price_type="LIMIT",
            limit_price=limit_price,
            call_put=call_put,
            strike_price=strike,
            expiry_year=expiry_year,
            expiry_month=expiry_month,
            expiry_day=expiry_day,
            market_session="REGULAR",
            order_term="GOOD_FOR_DAY"
        )

        print("PREVIEW RESPONSE:", preview)

        if "PreviewOrderResponse" not in preview:
            return {
                "status": "error",
                "reason": "preview_failed",
                "details": preview
            }

        # =========================
        # EXTRACT PREVIEW ID
        # =========================
        preview_ids = preview["PreviewOrderResponse"]["PreviewIds"]["previewId"]
        preview_id = preview_ids[0]["previewId"] if isinstance(preview_ids, list) else preview_ids["previewId"]

        print(f"✅ PREVIEW ID: {preview_id}")

        # =========================
        # PLACE OPTION ORDER
        # =========================
        order = session.place_option_order(
            account_id_key=account_id_key,
            preview_id=preview_id
        )

        print("ORDER RESPONSE:", order)
        print(f"✅ OPTION ORDER PLACED SUCCESSFULLY")

        return {
            "status": "success",
            "details": order
        }

    except Exception as e:
        print(f"❌ ORDER ERROR: {str(e)}")
        raise HTTPException(500, f"Order failed: {str(e)}")
