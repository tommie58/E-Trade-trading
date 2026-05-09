@app.post("/webhook")
async def webhook(request: Request):
    await verify_hmac_signature(request)

    data = await request.json()
    signal = TradeSignal(**data)

    signal_key = get_signal_key(signal)

    logger.info(f"Received signal: {signal.ticker} | {signal.action} | Strategy: {signal.strategy} | Score: {signal.score}")

    # 1. Market filter (fastest check first)
    if not is_market_open() and not signal.force_execute:
        return {"status": "rejected", "reason": "market_closed"}

    # 2. Idempotency - Atomic + State Machine (Fixed Race Condition)
    if not redis_client.set(signal_key, "processing", ex=86400, nx=True):
        existing_status = redis_client.get(signal_key)
        logger.info(f"Duplicate signal detected: {signal_key} (status: {existing_status})")
        return {"status": "duplicate", "existing_status": existing_status}

    try:
        # 3. Quality Validation
        validate_signal_quality(signal)

        # Optional: Enrich signal with extra data (e.g. current price, ATR, etc.)
        # await enrich_signal(signal)   # you can implement this

        session = get_session()

        # 4. Risk & Safety Checks
        check_circuit_breaker(session)
        check_daily_loss_limit(session)
        check_daily_trade_limit(session)
        check_portfolio_heat(session)

        # 5. Position Sizing (Risk-based)
        account_value = get_account_value(session)
        if signal.stop:
            signal.position_size_shares = calculate_position_size(
                account_value, signal.entry, signal.stop
            )

            # Final position size sanity check
            if signal.position_size_shares is None or signal.position_size_shares < 1:
                raise HTTPException(400, "Invalid position size calculated")

        # 6. Ticker Cooldown (moved earlier)
        cooldown_key = f"cooldown:{signal.ticker}"
        if redis_client.exists(cooldown_key):
            redis_client.set(signal_key, "cooldown", ex=86400)
            return {"status": "cooldown", "reason": "ticker_in_cooldown"}

        redis_client.set(cooldown_key, "1", ex=settings.ticker_cooldown_minutes * 60)

        # 7. Execution
        if settings.paper_mode:
            order_result = {
                "mode": "paper",
                "filled": True,
                "entry": signal.entry,
                "shares": signal.position_size_shares
            }
            logger.info(f"PAPER EXECUTION: {signal.ticker} {signal.action} x{signal.position_size_shares}")
        else:
            order_result = place_order(session, signal)

        # 8. Success Path
        redis_client.set(signal_key, "executed", ex=86400)
        save_trade_to_db(signal, order_result)

        # 9. Decision Trace for debugging/monitoring
        trace = build_decision_trace(
            signal=signal,
            checks={"market_open": True, "quality": True, "risk": True, "cooldown": False},
            rejection_reasons=[]
        )

        return {
            "status": "executed",
            "signal_id": signal_key,
            "position_size": signal.position_size_shares,
            "trace": trace,
            "mode": "paper" if settings.paper_mode else "live"
        }

    except HTTPException as http_exc:
        redis_client.set(signal_key, "rejected", ex=86400)
        logger.warning(f"Signal rejected: {signal.ticker} - {http_exc.detail}")
        return {"status": "rejected", "reason": http_exc.detail}

    except Exception as e:
        redis_client.set(signal_key, "failed", ex=86400)
        logger.error(f"Unexpected error processing {signal.ticker}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal execution error")
