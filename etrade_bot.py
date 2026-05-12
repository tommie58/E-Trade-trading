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

def load_session():
    try:
        with open(TOKENS_FILE) as f:
            tokens = json.load(f)
        accounts = pyetrade.ETradeAccounts(tokens, sandbox=True)  # Force sandbox for now
        acct_list = accounts.list_accounts()
        account = acct_list['AccountListResponse']['Accounts']['Account'][0]
        account_id_key = account['accountIdKey']
        print(f"✅ Loaded account: {account_id_key}")
        return accounts, account_id_key
    except Exception as e:
        print(f"❌ SESSION LOAD FAILED: {e}")
        return None, None

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload.get("ticker")
        action = payload.get("action", "BUY").upper()
        shares = int(payload.get("position_size_shares", 0))
        stop_price = payload.get("stop")
        target_price = payload.get("target")

        print(f"🚀 WEBHOOK RECEIVED: {action} {shares} {ticker}")

        session, account_id_key = load_session()
        if not session or not account_id_key:
            print("❌ No valid E*TRADE session")
            return {"status": "error", "reason": "no_session"}

        # 1. Preview (mandatory)
        try:
            preview = session.preview_equity_order(
                accountIdKey=account_id_key,
                symbol=ticker,
                quantity=shares,
                orderAction=action,
                priceType="MARKET"
            )
            print("✅ Preview successful")
        except Exception as e:
            print(f"❌ PREVIEW FAILED: {e}")

        # 2. Place order
        try:
            order = session.place_equity_order(
                accountIdKey=account_id_key,
                symbol=ticker,
                quantity=shares,
                orderAction=action,
                priceType="MARKET",
                stopPrice=stop_price,
                limitPrice=target_price if action == "SELL" else None
            )
            print(f"✅ ORDER PLACED SUCCESSFULLY: {action} {shares} {ticker}")
            return {"status": "success", "order": order}
        except Exception as e:
            print(f"❌ PLACE ORDER FAILED: {e}")
            raise

    except Exception as e:
        print(f"❌ GENERAL ERROR: {e}")
        raise HTTPException(500, str(e))

@app.get("/")
async def root():
    return {"status": "✅ Bot is running!"}
