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

def load_session():
    try:
        with open(".etrade_tokens.json") as f:
            tokens = json.load(f)
        # Use ETradeAccounts to get account info
        accounts = pyetrade.ETradeAccounts(tokens)
        account_list = accounts.list_accounts()
        if not account_list or not account_list.get('AccountListResponse', {}).get('Accounts', {}).get('Account'):
            print("❌ No accounts found")
            return None
        # Take first account
        account = account_list['AccountListResponse']['Accounts']['Account'][0]
        account_id_key = account['accountIdKey']
        print(f"✅ Loaded account: {account_id_key}")
        return accounts, account_id_key
    except Exception as e:
        print(f"❌ Load session failed: {e}")
        return None, None

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload["ticker"]
        action = payload["action"].upper()
        shares = int(payload.get("position_size_shares", 0))
        stop_price = payload.get("stop")
        target_price = payload.get("target")

        print(f"🚀 SIGNAL RECEIVED: {action} {shares} {ticker}")

        session, account_id_key = load_session()
        if not session or not account_id_key:
            print("❌ No valid E*TRADE session")
            return {"status": "error", "reason": "not_linked"}

        # Preview first (mandatory for E*TRADE)
        preview_response = session.preview_equity_order(
            accountIdKey=account_id_key,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET"
        )
        print("✅ Preview successful")

        # Place the order
        order_response = session.place_equity_order(
            accountIdKey=account_id_key,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET",
            stopPrice=stop_price,
            limitPrice=target_price if action == "SELL" else None
        )

        print(f"✅ ORDER PLACED: {action} {shares} {ticker}")
        return {"status": "success", "response": order_response}

    except Exception as e:
        print(f"❌ ORDER ERROR: {str(e)}")
        raise HTTPException(500, f"Order failed: {str(e)}")

@app.get("/")
async def root():
    return {"status": "✅ Bot is running"}
