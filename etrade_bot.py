from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os

app = FastAPI(title="E*TRADE Bot")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = str(data.get("verifier") or data.get("code") or data).strip()
        tokens = oauth.get_access_token(verifier)
        return {
            "status": "linked",
            "message": "Success!",
            "tokens": tokens   # ← Send tokens to app
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    return {"status": "linked"}   # App will handle real persistence
