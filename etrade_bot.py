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
        accounts = pyetrade.ETradeAccounts(tokens, sandbox=ENV == "sandbox")
        acct_list = accounts.list_accounts()
        account = acct_list['AccountListResponse']['Accounts']['Account'][0]
        return accounts, account['accountIdKey']
    except:
        return None, None

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload.get("ticker")
        action = payload.get("action", "BUY").upper()
        shares = int(payload.get("position_size_shares", 0))

        session, account_id_key = load_session()
        if not session or not account_id_key:
            return {"status": "error", "reason": "not_linked"}

        # Preview then Place
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
        print(f"❌ ERROR: {e}")
        raise HTTPException(500, str(e))
