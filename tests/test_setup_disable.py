"""SETUP_DISABLE_ENABLED. Flag off does not change an admit or a size."""
import asyncio
import importlib.util
import inspect
import logging
from pathlib import Path

import pytest

import main_bot


class _ReachedUnchangedPath(Exception):
    pass


def _reset(monkeypatch, *, enabled=None, shadow=None, items=None):
    if enabled is None:
        monkeypatch.delenv("SETUP_DISABLE_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SETUP_DISABLE_ENABLED", enabled)
    if shadow is None:
        monkeypatch.delenv("SETUP_DISABLE_SHADOW", raising=False)
    else:
        monkeypatch.setenv("SETUP_DISABLE_SHADOW", shadow)
    if items is None:
        monkeypatch.delenv("SETUP_DISABLE_LIST", raising=False)
    else:
        monkeypatch.setenv("SETUP_DISABLE_LIST", items)
    main_bot.reset_setup_disable_state()


def _entry(**overrides):
    payload = {
        "ticker": "NVDA",
        "action": "BUY",
        "instrument": "option",
        "setup": "breakdown",
        "timestamp": "2026-11-25T15:00:00+00:00",
    }
    payload.update(overrides)
    return payload


async def _kill_switch_raises():
    raise _ReachedUnchangedPath()


def test_flag_off_does_not_change_admit_or_size(monkeypatch):
    _reset(monkeypatch, enabled="false", shadow="false")
    assert main_bot.setup_disable_enabled() is False
    assert main_bot.judge_setup_disable("breakdown") == "allow"
    assert main_bot.judge_setup_disable("vwap_rejection") == "allow"
    assert main_bot.judge_setup_disable("momentum") == "allow"
    assert main_bot.cap_kept_setup_quantity("orb", 4) == 4
    assert main_bot.cap_kept_setup_quantity("momentum fade", 3) == 3
    assert main_bot.cap_kept_setup_quantity("momentum_fade", 6) == 6
    monkeypatch.setattr(main_bot.state, "is_killed", _kill_switch_raises)
    with pytest.raises(_ReachedUnchangedPath):
        asyncio.run(main_bot._passes_entry_filters(_entry()))
    assert main_bot.setup_disable_status()["counts"]["would_skip"] == 0
    assert main_bot.setup_disable_status()["counts"]["skipped"] == 0


def test_flag_on_skips_each_disabled_setup(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")
    monkeypatch.setattr(main_bot.state, "is_killed", _kill_switch_raises)
    for setup in ("breakdown", "vwap_rejection", "vwap rejection", "momentum"):
        main_bot.reset_setup_disable_state()
        ok, reasons = asyncio.run(main_bot._passes_entry_filters(_entry(setup=setup)))
        assert ok is False
        assert reasons == ["setup_disabled"]
        assert main_bot.setup_disable_status()["counts"]["skipped"] == 1


def test_momentum_does_not_match_momentum_fade(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")
    monkeypatch.setattr(main_bot.state, "is_killed", _kill_switch_raises)
    assert main_bot.canonical_setup_id("momentum_fade") == "momentum fade"
    assert main_bot.canonical_setup_id("momentum") == "momentum"
    assert main_bot.judge_setup_disable(
        "momentum fade", enabled=True, shadow=False, disabled=frozenset({"momentum"}),
    ) == "allow"
    assert main_bot.judge_setup_disable(
        "momentum_fade", enabled=True, shadow=False, disabled=frozenset({"momentum"}),
    ) == "allow"
    for setup in ("momentum fade", "momentum_fade"):
        with pytest.raises(_ReachedUnchangedPath):
            asyncio.run(main_bot._passes_entry_filters(_entry(setup=setup, ticker="UBER")))
    ok, reasons = asyncio.run(main_bot._passes_entry_filters(_entry(setup="momentum")))
    assert ok is False and reasons == ["setup_disabled"]


def test_kept_setups_cap_at_one_and_never_round_up(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")
    assert main_bot.cap_kept_setup_quantity("momentum fade", 4) == 1
    assert main_bot.cap_kept_setup_quantity("momentum_fade", 2) == 1
    assert main_bot.cap_kept_setup_quantity("orb", 3) == 1
    assert main_bot.cap_kept_setup_quantity("orb", 1) == 1
    assert main_bot.cap_kept_setup_quantity("orb", 0) == 0
    assert main_bot.cap_kept_setup_quantity("orb", 0.4) == 0
    assert main_bot.cap_kept_setup_quantity("orb", 1.8) == 1
    assert main_bot.cap_kept_setup_quantity("breakout", 5) == 5
    assert main_bot.apply_kept_setup_cap("orb", 4, ticker="CRM", cycle="c1") == 1
    assert main_bot.apply_kept_setup_cap("orb", 0, ticker="CRM", cycle="c2") == 0
    _reset(monkeypatch, enabled="false", shadow="false")
    assert main_bot.cap_kept_setup_quantity("orb", 4) == 4
    assert main_bot.apply_kept_setup_cap("momentum fade", 3, ticker="AMZN", cycle="c3") == 3


def test_shadow_counts_and_does_not_block(monkeypatch):
    _reset(monkeypatch, enabled="false", shadow=None)
    assert main_bot.setup_disable_shadow() is True
    monkeypatch.setattr(main_bot.state, "is_killed", _kill_switch_raises)

    async def _process(pd, replay=False):
        main_bot.observe_setup_disable(pd, path="scanner")
        return {"status": "queued"}

    monkeypatch.setattr(main_bot, "process_signal", _process)
    result = asyncio.run(main_bot._scanner_dispatch(_entry(setup="momentum")))
    assert result["status"] == "queued"
    counts = main_bot.setup_disable_status()["counts"]
    assert counts["would_skip"] == 1
    assert counts["skipped"] == 0
    with pytest.raises(_ReachedUnchangedPath):
        asyncio.run(main_bot._passes_entry_filters(_entry(setup="breakdown", ticker="SHOP")))
    counts = main_bot.setup_disable_status()["counts"]
    assert counts["would_skip"] == 2
    assert counts["skipped"] == 0


def test_scanner_flag_on_skips_without_placing(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")
    seen = []

    async def _process(pd, replay=False):
        seen.append(pd.get("setup"))
        return {"status": "queued"}

    monkeypatch.setattr(main_bot, "process_signal", _process)
    for setup in ("breakdown", "vwap_rejection", "momentum"):
        result = asyncio.run(main_bot._scanner_dispatch(_entry(setup=setup, ticker=setup[:4].upper())))
        assert result["status"] == "rejected"
        assert result["reason"] == "setup_disabled"
    fade = asyncio.run(main_bot._scanner_dispatch(
        _entry(setup="momentum_fade", ticker="UBER"),
    ))
    assert fade["status"] == "queued"
    assert seen == ["momentum_fade"]


def test_same_candidate_logs_once_per_cycle(monkeypatch, caplog):
    _reset(monkeypatch, enabled="false", shadow="true")
    payload = _entry(setup="breakdown", ticker="SHOP")
    with caplog.at_level(logging.INFO, logger="etrade-bot"):
        assert main_bot.observe_setup_disable(payload, path="app") is None
        assert main_bot.observe_setup_disable(payload, path="scanner") is None
    assert main_bot.setup_disable_status()["counts"]["would_skip"] == 1
    logs = [rec.message for rec in caplog.records if "setup_disabled shadow" in rec.message]
    assert len(logs) == 1
    later = _entry(setup="breakdown", ticker="SHOP", timestamp="2026-11-25T15:01:00+00:00")
    assert main_bot.observe_setup_disable(later, path="app") is None
    assert main_bot.setup_disable_status()["counts"]["would_skip"] == 2


def test_unknown_name_is_ignored_with_a_warning(monkeypatch, caplog):
    _reset(monkeypatch, items="breakdown,not_a_setup,momentum")
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        parsed = main_bot.resolved_setup_disable_list()
    assert parsed["unparseable"] is False
    assert parsed["ids"] == frozenset({"breakdown", "momentum"})
    assert "not_a_setup" in parsed["ignored"]
    assert any("unknown name" in rec.message and "not_a_setup" in rec.message for rec in caplog.records)
    assert main_bot.judge_setup_disable(
        "vwap rejection", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "allow"


def test_unparseable_env_falls_back_to_the_default_list(monkeypatch, caplog):
    _reset(monkeypatch, items="breakdown;momentum")
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        parsed = main_bot.resolved_setup_disable_list()
    assert parsed["unparseable"] is True
    assert parsed["source"] == "default_fallback"
    assert parsed["ids"] == frozenset({"breakdown", "vwap rejection", "momentum"})
    assert any("unparseable" in rec.message for rec in caplog.records)
    assert "breakdown;momentum" not in " ".join(rec.message for rec in caplog.records)


def test_status_exposes_read_only_counts(monkeypatch):
    _reset(monkeypatch, enabled="false", shadow="true")
    main_bot.observe_setup_disable(_entry(setup="momentum"), path="app")
    body = main_bot._pro_trader_status_payload(1)
    block = body["setup_disable"]
    assert block["read_only"] is True
    assert block["enabled"] is False
    assert block["shadow"] is True
    assert block["env_read"] == "each_entry"
    assert block["counts"]["would_skip"] == 1
    assert "momentum fade" in block["known_setups"]
    assert "vwap rejection" in block["list"]
    assert block["kept_capped_at_1"] == ["momentum fade", "orb"]
    assert body["hard_rails"]["max_size_multiplier"] == 1.0
    dumped = str(block)
    assert "RORK_SECRET" not in dumped
    assert "accountId" not in dumped


def test_close_is_not_skipped(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")

    async def _process(pd, replay=False):
        return {"status": "queued"}

    monkeypatch.setattr(main_bot, "process_signal", _process)
    result = asyncio.run(main_bot._scanner_dispatch(
        _entry(action="CLOSE", setup="breakdown"),
    ))
    assert result["status"] == "queued"
    assert main_bot.setup_disable_status()["counts"]["skipped"] == 0


def test_default_list_uses_the_codes_setup_strings():
    main_bot.reset_setup_disable_state()
    parsed = main_bot.parse_setup_disable_list(None)
    assert parsed["ids"] == frozenset({"breakdown", "vwap rejection", "momentum"})
    assert main_bot.canonical_setup_id("vwap_rejection") == main_bot.edge.normalize_setup("vwap_rejection")
    assert "momentum fade" not in parsed["ids"]


def test_replay_script_matches_exact_setup_ids():
    path = Path(__file__).resolve().parents[1] / "scripts" / "replay_setup_disable.py"
    spec = importlib.util.spec_from_file_location("replay_setup_disable", path)
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    assert replay.canonical_setup_id("momentum_fade") == main_bot.canonical_setup_id("momentum_fade")
    disabled = replay.disabled_ids()
    assert "momentum" in disabled
    assert "momentum fade" not in disabled
    closes = replay.load_closes(replay.TRADES)
    assert len(closes) == 57
    without = replay.summarize(closes, disabled, apply_flag=False)
    flagged = replay.summarize(closes, disabled, apply_flag=True)
    assert without["skipped_n"] == 0
    assert without["net"] == replay._money("-1063.28")
    assert flagged["skipped_n"] == 27
    assert {row["setup"] for row in flagged["skipped"]} <= disabled
    assert any(row["setup"] == "momentum fade" for row in closes)
    assert not any(row["setup"] == "momentum fade" for row in flagged["skipped"])
    ex = [row for row in closes if not replay.is_amzn_nov5_winner(row)]
    assert len(ex) == 56
    ex_flagged = replay.summarize(ex, disabled, apply_flag=True)
    assert ex_flagged["net"] == flagged["net"]
    assert ex_flagged["max_dd"] == flagged["max_dd"]


def test_kept_setup_cap_is_the_last_step_before_placement():
    src = inspect.getsource(main_bot.execute_live_order)
    cap_at = src.index("commit_final_kept_setup_qty(")
    assert src.index("_apply_learning_weight") < cap_at
    assert src.index("_apply_risk_weight") < cap_at
    assert src.index("_apply_pro_trader_size") < cap_at
    assert src.index("assess_hard_size_cap(") < cap_at
    assert cap_at < src.index("_place_entry_with_funds_clamp")


def test_final_placement_qty_caps_three_and_can_skip(monkeypatch):
    """Upstream sizing of 3 is sent as 1. Upstream 0 is a skip, not a round-up."""
    _reset(monkeypatch, enabled="true", shadow="false")
    order = {"quantity": 3}
    sent = main_bot.commit_final_kept_setup_qty("orb", 3, order, ticker="CRM", cycle="cap-3")
    assert sent == 1
    assert order["quantity"] == 1
    order = {"quantity": 3}
    sent = main_bot.commit_final_kept_setup_qty(
        "momentum_fade", 3, order, ticker="UBER", cycle="cap-fade",
    )
    assert sent == 1
    assert order["quantity"] == 1
    order = {"quantity": 0}
    sent = main_bot.commit_final_kept_setup_qty("orb", 0, order, ticker="CRM", cycle="cap-0")
    assert sent is None
    assert order["quantity"] == 0
    _reset(monkeypatch, enabled="false", shadow="false")
    order = {"quantity": 3}
    assert main_bot.commit_final_kept_setup_qty("orb", 3, order, ticker="CRM", cycle="off") == 3
    assert order["quantity"] == 3


def test_order_placement_receives_the_capped_quantity(monkeypatch):
    _reset(monkeypatch, enabled="true", shadow="false")
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "unit-secret")
    monkeypatch.setattr(main_bot, "_is_market_open", lambda: True)
    monkeypatch.setattr(main_bot, "_flatten_window_blocks_entry", lambda *a, **k: False)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "unit-token", "oauth_token_secret": "unit-secret",
    })

    async def account_key(tokens):
        return "acct"

    async def equity(*args, **kwargs):
        return 100000.0

    async def quiet(*args, **kwargs):
        return None

    class _Market:
        def __init__(self, *args, **kwargs):
            pass

    def snap(market, symbol, expiry, strike, call_put, liquidity_out=None):
        if liquidity_out is not None:
            liquidity_out["quote_type"] = "REALTIME"
        return ("2026-12-18", 150.0, 4.90, 5.00)

    async def fetch_quote(self, *args, **kwargs):
        return {
            "live": True, "bid": 4.90, "ask": 5.00, "age_s": 1.0,
            "timeframe": "REAL-TIME", "reason": "payoff",
        }

    sent = []

    async def place(kind, common, tokens, unit_cost=None):
        sent.append(int(common["quantity"]))
        return {"Order": [{"orderId": "1"}]}, int(common["quantity"]), str(common["clientOrderId"])

    upstream = {"qty": 3}

    def sized(symbol, qty, payload):
        return upstream["qty"], None

    def allow_hard_cap(premium, stop_premium, qty, equity):
        return {
            "allowed": True, "reason": "ok", "risk_usd": 1.0, "risk_cap_usd": 1000.0,
            "notional_usd": 1.0, "notional_cap_usd": 1000.0, "qty": qty,
        }

    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account_key)
    monkeypatch.setattr(main_bot, "_live_equity", equity)
    monkeypatch.setattr(main_bot, "_hard_cap_equity", equity)
    monkeypatch.setattr(main_bot, "_risk_account_reference", equity)
    monkeypatch.setattr(main_bot, "_index_vwap_quotes", quiet)
    monkeypatch.setattr(main_bot, "_apply_pro_trader_size", sized)
    monkeypatch.setattr(main_bot, "assess_hard_size_cap", allow_hard_cap)
    monkeypatch.setattr(main_bot, "_place_entry_with_funds_clamp", place)
    monkeypatch.setattr(main_bot, "_record_open", quiet)
    monkeypatch.setattr(main_bot, "_arm_stop_guard", quiet)
    monkeypatch.setattr(main_bot, "_stamp_entry_market_context", quiet)
    monkeypatch.setattr(main_bot.alerts, "send", quiet)
    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", _Market)
    monkeypatch.setattr(main_bot, "_snap_option_contract", snap)
    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_quote", fetch_quote)
    saved_state = main_bot.state
    main_bot.state = main_bot.StateStore(None)

    payload = {
        "mode": "live",
        "instrument": "option",
        "ticker": "CRM",
        "action": "BUY",
        "signal_id": "cap-three",
        "_origin": "app",
        "strike": 150,
        "expiration_hint": "2026-12-18",
        "option_right": "CALL",
        "option_contracts": 1,
        "option_limit_price": 5.0,
        "option_stop_price": 3.9,
        "entry": 148.0,
        "stop": 140.0,
        "target": 170.0,
        "option_delta": 0.60,
        "setup": "orb",
        "timestamp": "2026-11-25T15:00:00+00:00",
    }

    async def run():
        three = await main_bot.execute_live_order(dict(payload))
        upstream["qty"] = 0
        payload["signal_id"] = "cap-zero"
        zero = await main_bot.execute_live_order(dict(payload, signal_id="cap-zero"))
        return three, zero

    try:
        three, zero = asyncio.run(run())
    finally:
        main_bot.state = saved_state

    assert three["status"] == "success", three
    assert sent == [1]
    assert zero["status"] == "refused"
    assert zero["code"] == "setup_size_cap"
    assert sent == [1]


def test_shadow_observer_error_does_not_change_the_entry(monkeypatch, caplog):
    _reset(monkeypatch, enabled="false", shadow="true")

    def boom(*args, **kwargs):
        raise RuntimeError("leak acct 999000111 token SUPERSECRET")

    monkeypatch.setattr(main_bot, "_bump_setup_count", boom)
    monkeypatch.setattr(main_bot.state, "is_killed", _kill_switch_raises)
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        with pytest.raises(_ReachedUnchangedPath):
            asyncio.run(main_bot._passes_entry_filters(_entry(setup="breakdown")))
        with pytest.raises(_ReachedUnchangedPath):
            asyncio.run(main_bot._passes_entry_filters(_entry(setup="breakdown", ticker="SHOP")))
    text = " ".join(rec.message for rec in caplog.records)
    assert "setup_disable observer failed (RuntimeError)" in text
    assert "999000111" not in text
    assert "SUPERSECRET" not in text
    assert text.count("setup_disable observer failed") == 1


def test_flag_on_still_skips_when_the_counter_raises(monkeypatch, caplog):
    _reset(monkeypatch, enabled="true", shadow="false")

    def boom(*args, **kwargs):
        raise RuntimeError("leak acct 999000111")

    monkeypatch.setattr(main_bot, "_bump_setup_count", boom)

    async def _process(pd, replay=False):
        raise AssertionError("enforcement must not enter process_signal")

    monkeypatch.setattr(main_bot, "process_signal", _process)
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        ok, reasons = asyncio.run(main_bot._passes_entry_filters(_entry(setup="momentum")))
        result = asyncio.run(main_bot._scanner_dispatch(
            _entry(setup="breakdown", ticker="SHOP", timestamp="2026-11-25T15:05:00+00:00"),
        ))
    assert ok is False
    assert reasons == ["setup_disabled"]
    assert result["status"] == "rejected"
    assert result["reason"] == "setup_disabled"
    text = " ".join(rec.message for rec in caplog.records)
    assert "999000111" not in text
