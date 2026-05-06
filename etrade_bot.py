from fastapi import FastAPI, HTTPException, Request
import pyetrade
import json
import os

app = FastAPI()

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "default-secret")

TOKENS_FILE = ".etrade_tokens.json"

def load_tokens():
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE) as f:
            return json.load(f)
    return None

def save_tokens(tokens):
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f)

tokens = load_tokens()

oauth = pyetrade.ETradeOAuth(CONSUMER_KEY, CONSUMER_SECRET)

@app.post("/etrade/auth/start")
async def start_auth():
    try:
        url = oauth.get_request_token()
        return {"authorize_url": url}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/etrade/auth/complete")
async def complete_auth(verifier: str):
    try:
        new_tokens = oauth.get_access_token(verifier)
        save_tokens(new_tokens)
        return {"status": "linked"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/etrade/account")
async def get_account():
    if not tokens:
        return {"status": "not_linked"}
    return {"status": "linked"}

@app.post("/webhook")
async def webhook(request: Request):
    return {"status": "received"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
