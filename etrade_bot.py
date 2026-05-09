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
        return pyetrade.ETradeOrder(tokens)  # Use ETradeOrder for trading
    except:
        return None

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        ticker = payload["ticker"]
        action = payload["action"].upper()
        shares = int(payload.get("position_size_shares", 0))
        stop_price = payload.get("stop")
        limit_price = payload.get("target")

        print(f"🚀 SIGNAL: {action} {shares} {ticker}")

        session = load_session()
        if not session:
            print("❌ No E*TRADE session loaded")
            return {"status": "error", "reason": "not_linked"}

        # 1. Preview the order first (required by E*TRADE)
        preview = session.preview_equity_order(
            accountIdKey=session.accountIdKey,  # You may need to set this
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET"
        )
        print("✅ Preview successful")

        # 2. Place the actual order
        order_response = session.place_equity_order(
            accountIdKey=session.accountIdKey,
            symbol=ticker,
            quantity=shares,
            orderAction=action,
            priceType="MARKET",
            stopPrice=stop_price,
            limitPrice=limit_price
        )

        print(f"✅ ORDER PLACED: {action} {shares} {ticker}")
        return {"status": "success", "response": order_response}

    except Exception as e:
        print(f"❌ ORDER ERROR: {str(e)}")
        raise HTTPException(500, f"Order failed: {str(e)}")

@app.get("/")
async def root():
    return {"status": "✅ Bot running"}
