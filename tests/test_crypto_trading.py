"""Pure helpers for crypto (BTC/ETH/SOL) order planning and instrument gates."""
import main_bot


def test_normalize_crypto_symbol_accepts_pairs():
    assert main_bot.normalize_crypto_symbol("BTC/USD") == "BTC"
    assert main_bot.normalize_crypto_symbol("eth-usd") == "ETH"
    assert main_bot.normalize_crypto_symbol("SOLUSD") == "SOL"
    assert main_bot.normalize_crypto_symbol(" BTC ") == "BTC"


def test_entry_instrument_permitted_allows_crypto_under_options_only():
    ok, reason = main_bot.entry_instrument_permitted("crypto", False, True, True)
    assert ok and reason == ""
    ok, reason = main_bot.entry_instrument_permitted("stock", False, True, True)
    assert not ok and "options-only" in reason
    ok, reason = main_bot.entry_instrument_permitted("crypto", False, True, False)
    assert not ok and "CRYPTO_ENABLED" in reason
    ok, _ = main_bot.entry_instrument_permitted("crypto", True, True, False)
    assert ok  # closes always pass


def test_plan_crypto_order_dollar_buy():
    plan = main_bot.plan_crypto_order(
        {"ticker": "BTC/USD", "action": "BUY", "instrument": "crypto", "amount_usd": 100},
    )
    assert plan["ok"] is True
    assert plan["symbol"] == "BTC"
    assert plan["action"] == "BUY"
    assert plan["quantity_type"] == "DOLLAR"
    assert plan["quantity"] == 100.0
    assert plan["price_type"] == "MARKET"


def test_plan_crypto_order_rejects_below_minimum():
    plan = main_bot.plan_crypto_order(
        {"ticker": "ETH", "action": "BUY", "amount_usd": 5},
    )
    assert plan["ok"] is False
    assert "minimum" in plan["reason"]


def test_plan_crypto_order_rejects_unknown_symbol():
    plan = main_bot.plan_crypto_order({"ticker": "DOGE", "action": "BUY", "amount_usd": 50})
    assert plan["ok"] is False
    assert "BTC" in plan["reason"]


def test_plan_crypto_order_sizes_from_buying_power():
    plan = main_bot.plan_crypto_order(
        {"ticker": "SOL", "action": "BUY"},
        available_usd=1000.0,
        risk_pct=8.0,
    )
    assert plan["ok"] is True
    assert plan["quantity_type"] == "DOLLAR"
    assert plan["quantity"] == 80.0
    assert plan.get("sized_from_risk") is True


def test_plan_crypto_order_coin_quantity_and_limit():
    plan = main_bot.plan_crypto_order({
        "ticker": "BTC",
        "action": "BUY",
        "quantity_type": "QUANTITY",
        "quantity": 0.01,
        "limit_price": 95000.5,
    })
    assert plan["ok"] is True
    assert plan["quantity_type"] == "QUANTITY"
    assert plan["quantity"] == 0.01
    assert plan["price_type"] == "LIMIT"
    assert plan["limit_price"] == 95000.5


def test_plan_crypto_order_close_all_i_own():
    plan = main_bot.plan_crypto_order(
        {"ticker": "ETH/USD", "action": "SELL", "intent": "close"},
    )
    assert plan["ok"] is True
    assert plan["is_close"] is True
    assert plan["action"] == "SELL"
    assert plan["quantity_type"] == "ALL_I_OWN"


def test_is_crypto_account_detects_zerohash_desc():
    assert main_bot.is_crypto_account({"accountDesc": "Individual Crypto -8595"})
    assert main_bot.is_crypto_account({"accountDesc": "Individual Crypto 888103-8595"})
    assert main_bot.is_crypto_account({"accountType": "CRYPTO", "accountDesc": "x"})
    assert not main_bot.is_crypto_account({"accountDesc": "Individual Brokerage"})
    assert not main_bot.is_crypto_account(None)


def test_sanitize_account_id_strips_ui_dashes_and_spaces():
    cleaned, repaired = main_bot.sanitize_account_id("888103-8595")
    assert cleaned == "8881038595" and repaired
    cleaned, repaired = main_bot.sanitize_account_id("146 261816")
    assert cleaned == "146261816" and repaired
    cleaned, repaired = main_bot.sanitize_account_id("8881038595")
    assert cleaned == "8881038595" and not repaired


def test_account_matches_id_accepts_dashed_ui_form():
    acct = {
        "accountId": "8881038595",
        "accountIdKey": "opaqueCryptoKey",
        "accountDesc": "Individual Crypto 888103-8595",
    }
    assert main_bot.account_matches_id(acct, "888103-8595")
    assert main_bot.account_matches_id(acct, "8881038595")
    assert main_bot.account_matches_id(acct, "opaqueCryptoKey")
    assert not main_bot.account_matches_id(acct, "9999999999")


def test_crypto_account_default_targets_individual_crypto_8595():
    assert main_bot.TARGET_CRYPTO_ACCOUNT_ID == "8881038595"
    assert main_bot.CRYPTO_ENABLED is True


def test_brokerage_accounts_excludes_crypto():
    rows = [
        {"accountId": "111", "accountDesc": "Individual Brokerage"},
        {"accountId": "8881038595", "accountDesc": "Individual Crypto 888103-8595"},
    ]
    brokerage = main_bot._brokerage_accounts(rows)
    assert len(brokerage) == 1
    assert brokerage[0]["accountId"] == "111"


def test_crypto_instrument_payload_shape():
    inst = main_bot.ETradeAsyncClient.crypto_instrument("BTC/USD", "BUY", 25.0, "DOLLAR")
    assert inst["Product"]["securityType"] == "CRYPTO"
    assert inst["Product"]["symbol"] == "BTC"
    assert inst["quantityType"] == "DOLLAR"
    assert inst["quantity"] == 25.0
    assert inst["currency"] == "USD"
    assert inst["orderAction"] == "BUY"


def test_describe_dispatch_route_crypto():
    intent, side = main_bot.describe_dispatch_route({
        "instrument": "crypto", "action": "BUY", "ticker": "BTC",
    })
    assert intent == "entry" and side == "BUY"
    intent, side = main_bot.describe_dispatch_route({
        "instrument": "crypto", "action": "SELL", "intent": "close", "ticker": "BTC",
    })
    assert intent == "close" and side == "SELL"
