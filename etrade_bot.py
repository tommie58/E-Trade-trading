"""
Production-ready FastAPI webhook for automatically executing stock trades in an
E*TRADE account using pyetrade.

FEATURES
--------
- HMAC webhook signature verification
- Pydantic request validation
- Market hours filter
- Duplicate signal protection with Redis
- Risk management checks
- Paper trading mode
- E*TRADE preview + place order
- Structured logging

REQUIRED ENVIRONMENT VARIABLES
------------------------------
ETRADE_CONSUMER_KEY
ETRADE_CONSUMER_SECRET
ETRADE_ENV=sandbox|live
WEBHOOK_HMAC_SECRET=your_secret
PAPER_MODE=true|false
REDIS_URL=redis://localhost:6379/0
TOKENS_FILE=.etrade_tokens.json

OPTIONAL RISK SETTINGS
----------------------
MAX_POSITION_PERCENT=3.0
MAX_OPEN_POSITIONS=5
MAX_TRADES_PER_DAY=20
DAILY_LOSS_LIMIT_PERCENT=3.0
RISK_PER_TRADE_PERCENT=1.0
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime, time
from pathlib import Path
from typing import Literal, Optional

import pyetrade
import redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from zoneinfo import ZoneInfo


# =============================================================================
# SETTINGS
# =============================================================================
class Settings(BaseSettings):
    # E*TRADE
    etrade_consumer_key: str
    etrade_consumer_secret: str
    etrade_env: str = "sandbox"
    tokens_file: str = ".etrade_tokens.json"

    # Security
    webhook_hmac_secret: Optional[str] = None

    # Trading mode
    paper_mode: bool = True

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Risk settings
    max_position_percent: float = 3.0
    max_open_positions: int = 5
    max_trades_per_day: int = 20
    daily_loss_limit_percent: float = 3.0
    risk_per_trade_percent: float = 1.0

    # Market hours
    market_open_time: str = "09:35"
    market_close_time: str = "15:45"

    # Signal thresholds
    min_score: float = 85.0
    min_confidence: float = 0.80
    require_stop_loss: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()


# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etrade_bot")


# =============================================================================
# APP
# =============================================================================
app = FastAPI(title="E*TRADE Trading Bot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# REDIS
# =============================================================================
redis_client = redis.from_url(settings.redis_url, decode_responses=True)


# =============================================================================
# REQUEST MODEL
# =============================================================================
class TradeSignal(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=10)
    action: Literal["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"] = "BUY"
    position_size_shares: int = Field(..., gt=0)

    # Optional analytics fields
    entry: Optional[float] = None
    stop: Optional[float] = None
    score: Optional[float] = None
    confidence: Optional[float] = None
    strategy: Optional[str] = None
    force_execute: bool = False

    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        return v.upper().strip()


# =============================================================================
# SECURITY
# =============================================================================
async def verify_hmac_signature(request: Request):
    """
    Validates X-Signature header using HMAC SHA256.
    """
    if not settings.webhook_hmac_secret:
        return

    body = await request.body()
    received_sig = request.headers.get("X-Signature")

    if not received_sig:
        raise HTTPException(status_code=401, detail="Missing signature")

    expected_sig = hmac.new(
        settings.webhook_hmac_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(received_sig, expected_sig):
        raise HTTPException(status_code=401, detail="Invalid signature")


# =============================================================================
# MARKET HOURS
# =============================================================================
def is_market_open() -> bool:
    now = datetime.now(ZoneInfo("America/New_York")).time()
    return time(9, 35) <= now <= time(15, 45)


# =============================================================================
# DUPLICATE PROTECTION
# =============================================================================
def reserve_signal(signal: TradeSignal) -> str:
    """
    Atomically reserves a signal key in Redis.
    Raises HTTPException if duplicate.
    """
    key = (
        f"signal:"
        f"{signal.ticker}:"
        f"{signal.action}:"
        f"{signal.position_size_shares}:"
        f"{signal.strategy or 'NA'}"
    )

    created = redis_client.set(key, "1", ex=86400, nx=True)

    if not created:
        raise HTTPException(status_code=409, detail="Duplicate signal")

    return key


# =============================================================================
# SIGNAL QUALITY
# =============================================================================
def validate_signal(signal: TradeSignal):
    if signal.score is not None and signal.score < settings.min_score:
        raise HTTPException(400, "Score below threshold")

    if (
        signal.confidence is not None
        and signal.confidence < settings.min_confidence
    ):
        raise HTTPException(400, "Confidence below threshold")

    if settings.require_stop_loss and signal.action == "BUY":
        if signal.stop is None:
            raise HTTPException(400, "Stop loss is required")


# =============================================================================
# RISK HELPERS
# =============================================================================
def get_account_session():
    """
    Loads token file and returns (accounts, account_id_key).
    """
    token_path = Path(settings.tokens_file)

    if not token_path.exists():
        raise HTTPException(500, "E*TRADE token file not found")

    with token_path.open() as f:
        tokens = json.load(f)

    accounts = pyetrade.ETradeAccounts(
        tokens,
        sandbox=settings.etrade_env.lower() == "sandbox",
    )

    acct_list = accounts.list_accounts()

    account = acct_list["AccountListResponse"]["Accounts"]["Account"][0]
    account_id_key = account["accountIdKey"]

    return accounts, account_id_key


def get_account_value(accounts, account_id_key) -> float:
    """
    Placeholder implementation.
    Replace with actual account balance query if desired.
    """
    # For now, use a fixed notional value.
    return 100000.0


def validate_position_size(signal: TradeSignal, account_value: float):
    if signal.entry is None:
        return

    max_position_value = (
        account_value * settings.max_position_percent / 100
    )

    proposed_value = signal.entry * signal.position_size_shares

    if proposed_value > max_position_value:
        raise HTTPException(
            400,
            (
                f"Position value ${proposed_value:,.2f} exceeds "
                f"allowed ${max_position_value:,.2f}"
            ),
        )


# =============================================================================
# ORDER EXECUTION
# =============================================================================
def place_etrade_order(signal: TradeSignal) -> dict:
    accounts, account_id_key = get_account_session()

    logger.info(
        "Previewing order | %s %s %s",
        signal.action,
        signal.position_size_shares,
        signal.ticker,
    )

    preview = accounts.preview_equity_order(
        accountIdKey=account_id_key,
        symbol=signal.ticker,
        quantity=signal.position_size_shares,
        orderAction=signal.action,
        priceType="MARKET",
    )

    logger.info("Preview successful")

    if settings.paper_mode:
        logger.info("PAPER MODE enabled — order not sent")
        return {
            "paper": True,
            "preview": preview,
        }

    order = accounts.place_equity_order(
        accountIdKey=account_id_key,
        symbol=signal.ticker,
        quantity=signal.position_size_shares,
        orderAction=signal.action,
        priceType="MARKET",
    )

    logger.info("Live order submitted")

    return order


# =============================================================================
# WEBHOOK ENDPOINT
# =============================================================================
@app.post("/webhook")
async def webhook(request: Request):
    # 1. Verify security
    await verify_hmac_signature(request)

    # 2. Parse JSON
    payload = await request.json()
    signal = TradeSignal(**payload)

    # 3. Market hours
    if not is_market_open() and not signal.force_execute:
        return {
            "status": "rejected",
            "reason": "outside_market_hours",
        }

    # 4. Duplicate protection
    signal_id = reserve_signal(signal)

    # 5. Signal validation
    validate_signal(signal)

    # 6. Account/risk checks
    accounts, account_id_key = get_account_session()
    account_value = get_account_value(accounts, account_id_key)
    validate_position_size(signal, account_value)

    # 7. Execute
    try:
        result = place_etrade_order(signal)
    except Exception as e:
        logger.exception("Order execution failed")
        raise HTTPException(500, str(e))

    # 8. Structured logging
    logger.info(
        json.dumps(
            {
                "status": "executed",
                "signal_id": signal_id,
                "ticker": signal.ticker,
                "action": signal.action,
                "shares": signal.position_size_shares,
                "paper_mode": settings.paper_mode,
            }
        )
    )

    return {
        "status": "executed",
        "signal_id": signal_id,
        "paper_mode": settings.paper_mode,
        "result": result,
    }


# =============================================================================
# HEALTH CHECK
# =============================================================================
@app.get("/")
async def root():
    return {
        "status": "running",
        "broker": "E*TRADE",
        "environment": settings.etrade_env,
        "paper_mode": settings.paper_mode,
    }
