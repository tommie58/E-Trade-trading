from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json

app = FastAPI(title="E*TRADE Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")

oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)

@app.get("/")
async def root():
    return {"status": "✅ Bot is running!"}

@app.post("/etrade/auth/start")
async def start_auth():
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise HTTPException(400, "ETRADE keys not set in Variables")
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
        
        if len(verifier) < 4:
            raise HTTPException(400, "Invalid verification code")

        tokens = oauth.get_access_token(verifier)
        
        # Return tokens so the app can store them permanently
        return {
            "status": "linked",
            "message": "✅ E*TRADE linked!",
            "tokens": tokens   # ← This is the key change
        }
    except Exception as e:
        raise HTTPException(500, f"Complete failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    # Simple check - app will handle persistence
    return {"status": "linked"}
