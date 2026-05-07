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

oauth = pyetrade.ETradeOAuth(
    os.getenv("ETRADE_CONSUMER_KEY"),
    os.getenv("ETRADE_CONSUMER_SECRET")
)

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
async def complete_auth(verifier: str):
    verifier = verifier.strip()
    if not verifier or len(verifier) < 4:
        raise HTTPException(400, "Invalid verification code")
    try:
        tokens = oauth.get_access_token(verifier)
        with open(".etrade_tokens.json", "w") as f:
            json.dump(tokens, f)
        return {"status": "linked", "message": "✅ Successfully linked to E*TRADE!"}
    except Exception as e:
        raise HTTPException(500, f"Complete failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked" if os.path.exists(".etrade_tokens.json") else "not_linked"}
