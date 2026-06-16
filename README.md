# E*TRADE Trading Bot

FastAPI webhook that receives signals from your mobile app and executes trades on E*TRADE.

## Environment Variables (Railway)

| Variable                        | Example Value          
|--------------------------------|------------------------
| `ETRADE_ENV`                   | `production`              
| `LIVE_TRADING`                 | `true`                
| `WEBHOOK_SECRET`               | `TO@#ot122808`     
| `ETRADE_CONSUMER_KEY`          | `926ff2672fdcf9642547613c7f5a5c38`            
|`ETRADE_CONSUMER_SECRET`        | `592af2baf1665fa939c40ef54467178b7e3fa2ed6199b9a03a30e1366a8a4d08`                 
| `ENABLE_MARKET_HOURS_CHECK`    | `true`                 
| `BROKER_TIMEOUT_SECONDS`       | `25`                  
| `VERIFY_POSITIONS_ON_CLOSE`    | `true`                 
| `MAX_CONTRACTS`                | `5`                    
| `DAILY_LOSS_LIMIT_DOLLARS      |  `500`

