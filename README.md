# E*TRADE Trading Bot

FastAPI service that receives signals from the mobile app and places orders at E*TRADE.

`main_bot.py` is the running source in this repo. Put secrets in the host environment. Do not put them in this file.

## Required environment

| Variable | Purpose |
| --- | --- |
| `WEBHOOK_SECRET` | Shared secret. When it is unset the process refuses live orders and returns 401 on every route except `GET /health` and `GET /healthz`. |
| `ETRADE_CONSUMER_KEY` | From the E*TRADE developer portal. |
| `ETRADE_CONSUMER_SECRET` | From the E*TRADE developer portal. |
| `ETRADE_ENV` | `sandbox` or `production`. The code default is `production`. |
| `LIVE_TRADING` | `true` places real orders. The code default is `true`. Set `false` for a dry deploy. |
| `REDIS_URL` | Required in production. Positions, the kill switch, parked signals, and the placement queue live here. |

## Risk defaults when the variable is unset

A value set on the host overrides these.

| Variable | Code default |
| --- | --- |
| `RISK_PER_TRADE_PCT` | `8` |
| `DAILY_LOSS_LIMIT_PCT` | `20` |
| `MIN_SCORE` | `70` |
| `MIN_SCORE_TRENDING` | `65` |
| `MIN_RVOL` | `0.9` |
| `MIN_MTF` | `1` |
| `COMMISSION_PER_CONTRACT` | `0.52` |
| `DAY_TRADE_MODE` | `true` (3:40 ET flattens every open option; 0–7 DTE expression) |
| `LEARNING_ENABLED` | `true` (live closes resize / disable setup×regime cells) |
| `SCHEDULED_EXIT_ENABLED` | `true` |

`DAILY_LOSS_LIMIT_DOLLARS` is not read. `PAPER_MODE` is not read. Paper versus live is the payload `mode` plus `LIVE_TRADING` and `ETRADE_ENV`.

## Requests

Send `X-Rork-Secret` on every request except `GET /health` and `GET /healthz`.

`POST /webhook` and `POST /explain` also accept the secret in the JSON body, or `X-Rork-Signature` as HMAC-SHA256 of the raw body. A bad or empty signature does not reject a request that also carries the right secret.

Live entries must include `signal_id`. The bot dedupes on that id and derives the broker `clientOrderId` from it. A retry of the same id is the same order, not a second one.

`force_execute` in a payload does nothing unless `FORCE_EXECUTE_ALLOWED=true` on the host.

### Crypto (`instrument=crypto`)

E*TRADE crypto (zerohash) is supported alongside options-only mode for **BTC**, **ETH**, and **SOL**. Send `instrument: "crypto"` with either:

- `amount_usd` (or `dollar_amount`) — USD notional, matching the app ticket Amount field ($10 min / $500k max), or
- `coin_quantity` / `quantity_type: "QUANTITY"` — fractional coin amount (up to 8 decimals)

Market or limit (`limit_price`), Good for Day. Crypto is **24/7** (no equity calendar gate). Orders route to the linked Individual Crypto account only (`ETRADE_CRYPTO_ACCOUNT_ID=8881038595` by default, Redis pin via `POST /etrade/crypto/account/select`, or auto-detected from account description). That account is **crypto-only** — it cannot be pinned as the options/equity trading account. Broker stop orders are not available for crypto — exits are MARKET/LIMIT only.

`GET /health` and `GET /healthz` return status, version, and whether a secret is configured. Positions, stops, the guardian, and the cost model are on `GET /status`, which requires the secret.

### Per-trade review & learning

Every close writes a full review snapshot to the immutable ledger (`position_closed`) and a capped Redis list. That includes setup, origin (app/scanner), signal id, contract, stops, trail/eyes ATR, peak R/MFE, and realized P&L. The learning loop still sizes/disables setup×regime cells from those closes when `LEARNING_ENABLED=true`.

| Endpoint | Purpose |
| --- | --- |
| `GET /trades` | Newest closes with the full review payload (secret required) |
| `GET /study` | SQL study layer: coverage, expectancy, recent closes |
| `GET /learning/report` | Live setup×regime cell weights / disablements |
| `GET /ledger` | Raw hash-chained event tail |

### Bot eyes (scanner + smarter trails)

The server-side scanner samples Massive (when `MASSIVE_API_KEY` is set) or E*TRADE quotes on a cadence, scores setups, and keeps a rolling series per symbol. With `TRAIL_EYES_ENABLED=true` (default), that series feeds the trailing-stop ATR noise floor and can freeze the ratchet to breakeven-only when SPY's session looks stressed. Configure via `POST /scanner/config` and inspect with `GET /scanner/status`. Autonomous entries stay off unless `SCANNER_ALLOW_AUTONOMOUS_ENTRY=true` and mode is `live`.

The link-complete response does not include OAuth tokens. The server stores them.

## Local run

```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn main_bot:app --host 0.0.0.0 --port 8000
```
