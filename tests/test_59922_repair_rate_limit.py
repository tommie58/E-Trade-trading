"""The at-bid repair alert map does not grow without a bound."""
import main_bot


def test_stale_and_overflow_keys_are_evicted(monkeypatch):
    main_bot._REPAIR_AT_MARKET_LOG_AT.clear()
    now = 10_000.0
    monkeypatch.setattr(main_bot.time, "monotonic", lambda: now)
    main_bot._REPAIR_AT_MARKET_LOG_AT["OLD"] = now - main_bot._REPAIR_AT_MARKET_TTL_S - 1
    main_bot._REPAIR_AT_MARKET_LOG_AT["FRESH"] = now - 10
    assert main_bot._repair_at_market_due("NEW") is True
    assert "OLD" not in main_bot._REPAIR_AT_MARKET_LOG_AT
    assert "FRESH" in main_bot._REPAIR_AT_MARKET_LOG_AT
    assert "NEW" in main_bot._REPAIR_AT_MARKET_LOG_AT

    main_bot._REPAIR_AT_MARKET_LOG_AT.clear()
    for i in range(main_bot._REPAIR_AT_MARKET_MAX_KEYS + 10):
        main_bot._REPAIR_AT_MARKET_LOG_AT[f"T{i}"] = now - 1000 + i
    assert main_bot._repair_at_market_due("EXTRA") is True
    assert len(main_bot._REPAIR_AT_MARKET_LOG_AT) <= main_bot._REPAIR_AT_MARKET_MAX_KEYS
    assert "EXTRA" in main_bot._REPAIR_AT_MARKET_LOG_AT
    main_bot._REPAIR_AT_MARKET_LOG_AT.clear()
