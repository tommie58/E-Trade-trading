from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import json
import os

app = FastAPI(title="E*TRADE Trading Bot")

# Enable CORS so your mobile app can connect
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
        raise HTTPException(500, f"Failed to start auth: {str(e)}")

@app.post("/etrade/auth/complete")
async def complete_auth(verifier: str):
    try:
        tokens = oauth.get_access_token(verifier)
        with open(".etrade_tokens.json", "w") as f:
            json.dump(tokens, f)
        return {"status": "linked", "message": "Success!"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked" if os.path.exists(".etrade_tokens.json") else "not_linked"}

@app.post("/webhook")
async def webhook(request: Request):
    return {"status": "received"}
