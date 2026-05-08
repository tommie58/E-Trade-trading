from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json
from datetime import datetime

app = FastAPI(title="E*TRADE Bot")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# E*TRADE Setup
oauth = pyetrade.ETradeOAuth(
    os.getenv("ETRADE_CONSUMER_KEY"),
    os.getenv("ETRADE_CONSUMER_SECRET")
)

# Load tokens (if they exist)
def get_session():
    try:
        with open(".etrade_tokens.json") as f:
            tokens = json.load(f)
        return pyetrade.ETradeAccounts(tokens)
    except:
        return None

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload.get("ticker")
        action = payload.get("action")  # BUY or SELL
        shares = payload.get("position_size_shares", 0)

        print(f"📈 Received signal: {action} {shares} {ticker}")

        session = get_session()
        if not session:
            print("❌ No E*TRADE session - tokens missing")
            return {"status": "error", "reason": "not_linked"}

        # Place the order
        order = {
            "symbol": ticker,
            "action": action,
            "quantity": shares,
            "orderType": "MARKET",
            "priceType": "MARKET",
        }

        # TODO: Add stop loss and target later
        response = session.place_equity_order(**order)
        print("✅ Order placed:", response)

        return {"status": "success", "order": response}

    except Exception as e:
        print("Webhook error:", e)
        raise HTTPException(500, str(e))
