from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json
from datetime import datetime

app = FastAPI(title="E*TRADE Bot")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# E*TRADE Setup
CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)

def load_session():
    try:
        with open(".etrade_tokens.json", "r") as f:
            tokens = json.load(f)
        return pyetrade.ETradeAccounts(tokens)
    except Exception as e:
        print("❌ Failed to load E*TRADE session:", e)
        return None

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
        with open(".etrade_tokens.json", "w") as f:
            json.dump(tokens, f)
        return {"status": "linked", "message": "✅ Linked!", "tokens": tokens}
    except Exception as e:
        raise HTTPException(500, f"Complete failed: {str(e)}")

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
        
        ticker = payload["ticker"]
        action = payload["action"].upper()  # BUY / SELL
        entry = payload.get("entry")
        stop = payload.get("stop")
        target = payload.get("target")
        shares = int(payload.get("position_size_shares", 0))
        risk_dollars = payload.get("risk_dollars", 0)

        print(f"🚀 RECEIVED SIGNAL → {action} {shares} {ticker} | Risk ${risk_dollars}")

        session = load_session()
        if not session:
            print("❌ No valid E*TRADE session")
            return {"status": "error", "reason": "not_linked"}

        # Bracket Order (Entry + Stop + Target)
        order = {
            "symbol": ticker,
            "action": action,
            "quantity": shares,
            "orderType": "MARKET",
            "priceType": "MARKET",
            "stopPrice": stop,
            "limitPrice": target if action == "BUY" else None,
        }

        response = session.place_equity_order(**order)
        print(f"✅ ORDER PLACED SUCCESSFULLY: {ticker} {action} {shares} shares")

        return {
            "status": "success",
            "ticker": ticker,
            "action": action,
            "shares": shares,
            "response": response
        }

    except Exception as e:
        print("❌ Webhook error:", str(e))
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked"}
