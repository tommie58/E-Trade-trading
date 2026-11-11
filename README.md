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

`DAILY_LOSS_LIMIT_DOLLARS` is not read. `PAPER_MODE` is not read. Paper versus live is the payload `mode` plus `LIVE_TRADING` and `ETRADE_ENV`.

## Requests

Send `X-Rork-Secret` on every request except `GET /health` and `GET /healthz`.

`POST /webhook` and `POST /explain` also accept the secret in the JSON body, or `X-Rork-Signature` as HMAC-SHA256 of the raw body. A bad or empty signature does not reject a request that also carries the right secret.

Live entries must include `signal_id`. The bot dedupes on that id and derives the broker `clientOrderId` from it. A retry of the same id is the same order, not a second one.

`force_execute` in a payload does nothing unless `FORCE_EXECUTE_ALLOWED=true` on the host.

`GET /health` and `GET /healthz` return status, version, and whether a secret is configured. Positions, stops, the guardian, and the cost model are on `GET /status`, which requires the secret.

The link-complete response does not include OAuth tokens. The server stores them.

## Local run

```bash
pip install -r requirements.txt
cp .env.example .env
uvicorn main_bot:app --host 0.0.0.0 --port 8000
```
