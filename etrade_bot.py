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

# Store tokens in environment variable (more persistent on Railway)
TOKEN_ENV_VAR = "ETRADE_ACCESS_TOKENS"

def get_tokens():
    tokens_str = os.getenv(TOKEN_ENV_VAR)
    if tokens_str:
        try:
            return json.loads(tokens_str)
        except:
            pass
    return None

def save_tokens(tokens):
    try:
        os.environ[TOKEN_ENV_VAR] = json.dumps(tokens)
        print("✅ Tokens saved to environment variable")
    except Exception as e:
        print(f"⚠️ Failed to save tokens: {e}")

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
        verifier = data.get("verifier") or data.get("code") or str(data)
        verifier = str(verifier).strip()
        
        if len(verifier) < 4:
            raise HTTPException(400, "Invalid verification code")

        tokens = oauth.get_access_token(verifier)
        save_tokens(tokens)
        
        return {"status": "linked", "message": "✅ E*TRADE account successfully linked!"}
    except Exception as e:
        raise HTTPException(500, f"Complete auth failed: {str(e)}")

@app.get("/etrade/account")
async def get_account():
    tokens = get_tokens()
    return {
        "status": "linked" if tokens else "not_linked",
        "has_tokens": bool(tokens)
    }

@app.post("/webhook")
async def webhook(request: Request):
    return {"status": "received"}
