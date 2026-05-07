from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os

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

# Support both GET (for browser testing) and POST (for the app)
@app.get("/etrade/auth/start")
@app.post("/etrade/auth/start")
async def start_auth():
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise HTTPException(400, "ETRADE keys not set in Variables tab")
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
