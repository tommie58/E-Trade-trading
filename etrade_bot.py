from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json

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

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload.get("ticker")
        action = payload.get("action", "BUY").upper()
        shares = int(payload.get("position_size_shares", 0))

        print(f"🚀 SIGNAL RECEIVED: {action} {shares} {ticker}")

        session, account_id_key = load_session()
        if not session or not account_id_key:
            print("❌ No valid session")
            return {"status": "error", "reason": "not_linked"}

        # Preview order
        preview = session.preview_equity_order(
            accountIdKey=account_id_key,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET",
            marketSession="REGULAR",
            orderTerm="GOOD_FOR_DAY"
        )

        print("PREVIEW RESPONSE:", preview)

        if "PreviewOrderResponse" not in preview:
            return {"status": "error", "reason": "preview_failed", "details": preview}

        # Extract previewId
        preview_ids = preview["PreviewOrderResponse"]["PreviewIds"]["previewId"]
        preview_id = preview_ids[0]["previewId"] if isinstance(preview_ids, list) else preview_ids["previewId"]

        print(f"✅ PREVIEW ID: {preview_id}")

        # Place order using previewId
        order = session.place_equity_order(
            accountIdKey=account_id_key,
            previewId=preview_id
        )

        print("ORDER RESPONSE:", order)
        print(f"✅ ORDER PLACED SUCCESSFULLY: {action} {shares} {ticker}")

        return {"status": "success", "details": order}

    except Exception as e:
        print(f"❌ ORDER ERROR: {str(e)}")
        raise HTTPException(500, f"Order failed: {str(e)}")
