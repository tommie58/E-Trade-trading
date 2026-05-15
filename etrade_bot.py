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
        accounts = pyetrade.ETradeAccounts(tokens)   # ← Fixed: removed sandbox argument
        acct_list = accounts.list_accounts()
        account = acct_list['AccountListResponse']['Accounts']['Account'][0]
        account_id_key = account['accountIdKey']
        print(f"✅ LOADED ACCOUNT: {account_id_key} ({ENV})")
        return accounts, account_id_key
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

        print(f"🚀 SIGNAL: {action} {shares} {ticker}")

        session, account_id_key = load_session()
        if not session or not account_id_key:
            print("❌ No valid session")
            return {"status": "error", "reason": "not_linked"}

        # Preview + Place
        session.preview_equity_order(
            accountIdKey=account_id_key,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET"
        )

        order = session.place_equity_order(
            accountIdKey=account_id_key,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET"
        )

        print(f"✅ ORDER PLACED: {action} {shares} {ticker}")
        return {"status": "success"}

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        raise HTTPException(500, f"Order failed: {str(e)}")
