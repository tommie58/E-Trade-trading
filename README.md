# E*TRADE Trading Bot

FastAPI webhook that receives signals from your mobile app and executes trades on E*TRADE.

## Environment Variables (Railway)

| Variable                        | Example Value          | Description |
|--------------------------------|------------------------|-----------|
| `ETRADE_ENV`                   | `sandbox`              | `sandbox` or `live` |
| `LIVE_TRADING`                 | `true`                | Set to `true` only when ready for real money |
| `WEBHOOK_SECRET`               | `TO@#ot122808`     | Must match your app |
| `ETRADE_CONSUMER_KEY`          | `...`                  | From E*TRADE developer portal |
| `ETRADE_CONSUMER_SECRET`       | `...`                  | From E*TRADE developer portal |
| `ENABLE_MARKET_HOURS_CHECK`    | `true`                 | Prevents trading outside market hours |
| `BROKER_TIMEOUT_SECONDS`       | `25`                   | Timeout for E*TRADE API calls |
| `VERIFY_POSITIONS_ON_CLOSE`    | `true`                 | Safer option closing |
| `MAX_CONTRACTS`                | `5`                    | Max contracts per signal |

## How to Deploy

1. Push code to GitHub
2. Connect repo to Railway
3. Add the environment variables above
4. Deploy

## Testing

- Link account via `/etrade/auth/start`
- Send test signals to `/webhook`
