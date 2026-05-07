from fastapi import FastAPI, HTTPException
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
        raise HTTPException(400, "ETRADE keys not configured")
    try:
        url = oauth.get_request_token()
        return {"authorize_url": url}
    except Exception as e:
        raise HTTPException(500, f"Start auth failed: {str(e)}")

@app.post("/etrade/auth/complete")
async def complete_auth(verifier: str):
    if not verifier or len(verifier.strip()) < 5:
        raise HTTPException(400, "Invalid verification code")
    try:
        tokens = oauth.get_access_token(verifier.strip())
        with open(".etrade_tokens.json", "w") as f:
            json.dump(tokens, f)
        return {"status": "linked", "message": "E*TRADE account successfully linked!"}
    except Exception as e:
        raise HTTPException(500, f"Complete auth failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked" if os.path.exists(".etrade_tokens.json") else "not_linked"}
