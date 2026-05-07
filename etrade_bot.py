from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import pyetrade
import os
import json

app = FastAPI(title="E*TRADE Trading Bot")

# CORS for mobile app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
TOKENS_FILE = ".etrade_tokens.json"

oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)

def load_tokens():
    if os.path.exists(TOKENS_FILE):
        try:
            with open(TOKENS_FILE) as f:
                return json.load(f)
        except:
            pass
    return None

def save_tokens(tokens):
    try:
        with open(TOKENS_FILE, "w") as f:
            json.dump(tokens, f)
    except:
        pass  # Fail silently if file write fails

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
        raise HTTPException(500, f"Start auth failed: {str(e)}")

@app.post("/etrade/auth/complete")
async def complete_auth(request: Request):
    try:
        data = await request.json()
        verifier = data.get("verifier") or data.get("code") or str(data).strip()
        verifier = str(verifier).strip()
        
        if len(verifier) < 4:
            raise HTTPException(400, "Invalid verification code")

        tokens = oauth.get_access_token(verifier)
        save_tokens(tokens)
        
        return {
            "status": "linked", 
            "message": "✅ E*TRADE account successfully linked!"
        }
    except Exception as e:
        raise HTTPException(500, f"Complete auth failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    tokens = load_tokens()
    return {
        "status": "linked" if tokens else "not_linked",
        "has_tokens": bool(tokens)
    }

@app.post("/webhook")
async def webhook(request: Request):
    return {"status": "received"}
