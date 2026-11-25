"""SETUP_DISABLE_ENABLED. Flag off does not change an admit or a size."""
import asyncio
import importlib.util
import inspect
import logging
import os
from pathlib import Path

import pytest

import main_bot


class _ReachedUnchangedPath(Exception):
    pass


def _open_shadow(tmp_path):
    main_bot.init_setup_disable_shadow_log(tmp_path / "setup_disable_shadow.jsonl")


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
    assert parsed["source"] == "fallback"
    assert parsed["ids"] == frozenset({"breakdown", "breakout", "vwap rejection", "momentum"})
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
    assert set(block["list"]) == {"breakdown", "breakout", "vwap rejection", "momentum"}
    for name in block["list"]:
        assert name in block["counts"]["by_setup"]
        assert set(block["counts"]["by_setup"][name]) == {"skipped", "would_skip"}
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
    assert parsed["ids"] == frozenset({"breakdown", "breakout", "vwap rejection", "momentum"})
    assert main_bot.canonical_setup_id("vwap_rejection") == main_bot.edge.normalize_setup("vwap_rejection")
    assert "momentum fade" not in parsed["ids"]
    for name in (
        "breakout", "breakdown", "vwap rejection", "vwap reclaim",
        "momentum", "momentum fade", "mean reversion", "orb",
    ):
        assert main_bot.canonical_setup_id(name) == name
    assert main_bot.judge_setup_disable(
        "vwap reclaim", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "allow"
    assert main_bot.judge_setup_disable(
        "mean reversion", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "allow"


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
    assert src.rindex("note_setup_disable_decision") > src.index("_place_entry_with_funds_clamp")


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


def test_default_list_is_configurable_subset(monkeypatch):
    _reset(monkeypatch, items="breakdown,breakout")
    parsed = main_bot.resolved_setup_disable_list()
    assert parsed["ids"] == frozenset({"breakdown", "breakout"})
    assert main_bot.judge_setup_disable(
        "vwap rejection", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "allow"
    assert main_bot.judge_setup_disable(
        "breakdown", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "skip"
    assert main_bot.judge_setup_disable(
        "momentum fade", enabled=True, shadow=False, disabled=parsed["ids"],
    ) == "allow"


def test_shadow_ledger_records_one_row_per_signal(tmp_path):
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    payload = {
        "ticker": "FAKE",
        "setup": "vwap rejection",
        "action": "BUY",
        "option_right": "CALL",
        "strike": 100,
        "expiration_hint": "2026-12-18",
        "entry": 10.5,
        "bid": 1.2,
        "ask": 1.3,
        "option_limit_price": 1.25,
        "signal_id": "fake-signal-1",
        "option_contracts": 3,
    }

    async def run():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=3, enforced=False,
            quantity_basis="after_sizing", path="entry",
            option_quote={
                "bid": 1.1, "ask": 1.3, "mid": 1.2, "spread_pct": 16.67,
                "source": "Massive", "quote_time": "2026-11-30T10:00:00-05:00",
                "quote_age_s": 1.0,
            },
            sizing={
                "qty_before_learning": 4, "qty_after_learning": 3,
                "qty_after_risk": 3, "limit_price": 1.25,
                "initial_stop_premium": 0.9, "planned_risk_usd": 105.0,
            },
        )
        await main_bot.persist_setup_disable_decision(
            dict(payload, timestamp="2026-11-30T15:01:00+00:00"),
            quantity=9, enforced=False, quantity_basis="after_sizing", path="entry",
        )

    try:
        asyncio.run(run())
        rows = main_bot.read_setup_disable_shadow(20)
        core = main_bot.trade_ledger.recent(20)
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()

    assert len(rows) == 1
    assert all(rec.get("event") != "setup_disable_shadow" for rec in core)
    data = rows[0]["data"]
    assert data["setup"] == "vwap rejection"
    assert data["symbol"] == "FAKE"
    assert data["direction"] == "bullish"
    assert data["option_type"] == "CALL"
    assert data["signal_id"] == "fake-signal-1"
    assert data["attempt"] == 1
    assert data["enforced"] is False
    assert data["would_be_quantity"] == 3
    assert data["list_source"] in {"default", "env", "fallback"}
    assert data["config"]["bot_version"] == main_bot.BOT_VERSION
    assert "et_timestamp" in data
    assert data["quantity_basis"] == "after_sizing"
    assert data["would_be_contract"]["strike"] == 100
    assert data["would_be_contract"]["expiry"] == "2026-12-18"
    assert data["would_be_contract"]["option_symbol"].startswith("FAKE")
    assert data["entry_price"] == 10.5
    assert data["option_quote"]["bid"] == 1.1
    assert data["option_quote"]["source"] == "Massive"
    assert data["sizing"]["qty_before_learning"] == 4
    assert data["sizing"]["qty_final"] == 3
    assert "account" not in str(data).lower()


def test_occ_order_id_and_signal_id_survive_byte_identical(tmp_path, monkeypatch):
    """A digit-run scrubber would turn the OCC strike and a long id into a redaction."""
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    monkeypatch.setenv("ETRADE_ACCOUNT_ID", "00100000")
    monkeypatch.setenv("ETRADE_ACCOUNT_ID_KEY", "8844221100987766")
    occ = "FAKE  261218C00100000"
    order_id = "8844221100987766-ORDER"
    signal_id = "sig-7f3a9c1e4b8d6a00100000ff"
    payload = {
        "ticker": "FAKE",
        "setup": "vwap rejection",
        "action": "BUY",
        "option_right": "CALL",
        "strike": 100,
        "expiration_hint": "2026-12-18",
        "signal_id": signal_id,
        "accountId": "00100000",
        "accountIdKey": "8844221100987766",
        "underlying_source": "https://quotes.example/FAKE?accountId=00100000",
    }

    async def run():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="scanner", fresh_order=True,
            option_quote={
                "bid": 1.2, "ask": 1.3, "mid": 1.25, "spread_pct": 8.0,
                "source": "Massive", "quote_time": "2026-11-30T11:00:00-05:00",
                "quote_age_s": 0.4,
                "accountId": "00100000",
                "preview_url": "https://broker.example/orders/8844221100987766",
            },
            sizing={"limit_price": "00100000"},
            outcome={
                "order_id": order_id,
                "placement_status": "placed",
                "reason": "placed via https://broker.example/orders/8844221100987766",
                "accountId": "00100000",
            },
        )

    try:
        asyncio.run(run())
        rows = [rec["data"] for rec in main_bot.read_setup_disable_shadow(10)]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()
    assert len(rows) == 1
    data = rows[0]
    assert data["would_be_contract"]["option_symbol"] == occ
    assert data["outcome"]["order_id"] == order_id
    assert data["signal_id"] == signal_id
    assert data["signal_key"] == f"FAKE|vwap rejection|{signal_id}|1"
    assert data["sizing"]["limit_price"] == "[redacted]"
    assert data["underlying_source"] == "[redacted]"
    assert "https://" not in str(data)
    assert "accountId" not in str(data)
    assert "preview_url" not in data["option_quote"]


def test_position_opened_stamps_signal_key(tmp_path, monkeypatch):
    saved = main_bot.trade_ledger._ledger
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    signal_key = "FAKE|breakout|sig-7f3a9c1e4b8d6a00100000ff|1"
    occ = "FAKE  261218C00100000"

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot.state, "set_position", quiet)
    monkeypatch.setattr(main_bot.state, "incr_trades_today", quiet)

    async def run():
        await main_bot._record_open(
            "FAKE", 1, 10.5, 9.0, 12.0,
            {"occ_symbol": occ, "right": "CALL", "strike": 100.0, "expiration": "2026-12-18"},
            "BUY", "breakout",
            signal_id="sig-7f3a9c1e4b8d6a00100000ff",
            instrument="option",
            signal_key=signal_key,
        )

    try:
        asyncio.run(run())
        rows = [
            rec for rec in main_bot.trade_ledger.recent(10)
            if rec.get("event") == "position_opened"
        ]
    finally:
        main_bot.trade_ledger._ledger = saved
    assert len(rows) == 1
    assert rows[0]["data"]["signal_key"] == signal_key
    assert rows[0]["data"]["contract"]["occ_symbol"] == occ


def test_shadow_ledger_failure_does_not_raise(tmp_path, monkeypatch):
    _reset(monkeypatch, enabled="false", shadow="true")
    _open_shadow(tmp_path)

    def boom(data):
        raise RuntimeError("ledger down")

    monkeypatch.setattr(main_bot._shadow_log, "append", boom)

    async def run():
        await main_bot.persist_setup_disable_decision(
            {"ticker": "FAKE", "setup": "breakout", "signal_id": "x", "action": "BUY"},
            quantity=1, enforced=False, quantity_basis="after_sizing", path="entry",
        )

    asyncio.run(run())


def test_second_same_day_entry_gets_its_own_shadow_row(tmp_path):
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    payload = {
        "ticker": "FAKE", "setup": "breakout", "action": "BUY",
        "option_right": "PUT", "signal_id": "same-day", "strike": 50,
        "expiration_hint": "2026-12-18",
    }

    async def run():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "1", "placement_status": "placed"},
        )
        await main_bot.persist_setup_disable_decision(
            payload, quantity=2, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "2", "placement_status": "placed"},
        )

    try:
        asyncio.run(run())
        rows = [rec["data"] for rec in main_bot.read_setup_disable_shadow(20)]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()
    assert [row["attempt"] for row in rows] == [1, 2]
    assert rows[0]["option_type"] == "PUT"
    assert rows[0]["direction"] == "bearish"
    assert rows[1]["outcome"]["order_id"] == "2"


def test_startup_seed_keeps_the_next_same_day_attempt(tmp_path):
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    payload = {
        "ticker": "FAKE", "setup": "momentum", "action": "BUY",
        "option_right": "CALL", "signal_id": "same-day",
    }

    async def run():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "1", "placement_status": "placed", "position_opened_seq": 7},
        )
        main_bot.reset_setup_disable_state()
        main_bot.seed_setup_disable_from_ledger()
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "2", "placement_status": "placed"},
        )

    try:
        asyncio.run(run())
        rows = [rec["data"] for rec in main_bot.read_setup_disable_shadow(20)]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()
    assert [row["attempt"] for row in rows] == [1, 2]
    assert rows[1]["signal_key"].endswith("|2")


def test_empty_or_unknown_list_falls_back(monkeypatch, caplog):
    _reset(monkeypatch, items="   ")
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        blank = main_bot.resolved_setup_disable_list()
    assert blank["source"] == "fallback"
    assert blank["ids"] == frozenset({"breakdown", "breakout", "vwap rejection", "momentum"})
    _reset(monkeypatch, items="not_a_setup,also_unknown")
    with caplog.at_level(logging.WARNING, logger="etrade-bot"):
        unknown = main_bot.resolved_setup_disable_list()
    assert unknown["source"] == "fallback"
    assert unknown["ids"] == blank["ids"]
    text = " ".join(rec.message for rec in caplog.records)
    assert "not_a_setup" in text and "also_unknown" in text


def test_slow_shadow_write_does_not_delay_the_handoff(monkeypatch):
    import time
    _reset(monkeypatch, enabled="false", shadow="true")

    async def slow(event, data=None):
        if event == main_bot.SETUP_DISABLE_EVENT:
            await asyncio.sleep(2)
        return 1

    monkeypatch.setattr(main_bot.trade_ledger, "record", slow)
    payload = {
        "ticker": "FAKE", "setup": "breakout", "action": "BUY",
        "option_right": "CALL", "signal_id": "slow-1",
    }

    async def run():
        started = time.perf_counter()
        main_bot.note_setup_disable_decision(
            payload, quantity=3, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            option_quote={"bid": 1.1, "ask": 1.2, "mid": 1.15, "spread_pct": 8.7,
                          "source": "Massive", "quote_time": None, "quote_age_s": 0.4},
            outcome={"placement_status": "placed", "order_id": "99"},
        )
        return time.perf_counter() - started

    elapsed = asyncio.run(run())
    assert elapsed < 0.5


def test_shadow_side_log_keeps_its_own_hash_chain(tmp_path):
    """A slow shadow append stays on the side log. The core chain is untouched."""
    import time
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    original = main_bot._shadow_log.append

    def slow(data):
        time.sleep(0.3)
        return original(data)

    main_bot._shadow_log.append = slow
    payload = {
        "ticker": "FAKE", "setup": "breakdown", "action": "BUY",
        "option_right": "CALL", "signal_id": "chain-1",
    }

    async def run():
        for i in range(4):
            await main_bot.trade_ledger.record("seed", {"i": i})
        main_bot.note_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
        )
        await asyncio.sleep(0.6)
        await main_bot.trade_ledger.record("heartbeat", {"n": 1})
        if main_bot._shadow_queue is not None:
            await asyncio.wait_for(main_bot._shadow_queue.join(), timeout=2)
        return main_bot.trade_ledger.verify(), main_bot._shadow_log.verify()

    try:
        core, side = asyncio.run(run())
        core_rows = main_bot.trade_ledger.recent(20)
        side_rows = main_bot.read_setup_disable_shadow(20)
        written = main_bot.setup_disable_status()["ledger"]["written"]
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()
    assert core[0] is True and core[2] is None
    assert core[1] == 5
    assert side[0] is True and side[2] is None
    assert side[1] == 1
    assert written == 1
    assert all(row["event"] != "setup_disable_shadow" for row in core_rows)
    assert len(side_rows) == 1


def test_core_open_does_not_wait_on_a_slow_shadow_write(tmp_path):
    import time
    saved = main_bot.trade_ledger._ledger
    saved_state = main_bot.state
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    main_bot.state = main_bot.StateStore(None)
    _open_shadow(tmp_path)
    original = main_bot._shadow_log.append

    def slow(data):
        time.sleep(0.3)
        return original(data)

    main_bot._shadow_log.append = slow

    async def run():
        main_bot.note_setup_disable_decision(
            {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": "slow-open"},
            quantity=1, enforced=False, quantity_basis="after_sizing", path="entry",
            fresh_order=True,
        )
        started = time.perf_counter()
        seq = await main_bot._record_open(
            "FAKE", 1, 10.0, 9.0, 12.0, None, "BUY", "breakdown",
            signal_id="slow-open", instrument="equity",
            signal_key="FAKE|breakdown|slow-open|1",
        )
        elapsed = time.perf_counter() - started
        if main_bot._shadow_queue is not None:
            await asyncio.wait_for(main_bot._shadow_queue.join(), timeout=2)
        return elapsed, seq

    try:
        elapsed, seq = asyncio.run(run())
        core = main_bot.trade_ledger.recent(10)
        side = main_bot.read_setup_disable_shadow(10)
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.state = saved_state
        main_bot.reset_setup_disable_state()
    assert elapsed < 0.15
    assert seq is not None
    assert any(row["event"] == "position_opened" for row in core)
    assert all(row["event"] != "setup_disable_shadow" for row in core)
    assert len(side) == 1


def test_shadow_queue_counts_written_dropped_and_queued(tmp_path, monkeypatch):
    import threading
    _reset(monkeypatch, enabled="false", shadow="true")
    monkeypatch.setattr(main_bot, "_SHADOW_LEDGER_QUEUE_MAX", 2)
    _open_shadow(tmp_path)
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    started = threading.Event()
    release = threading.Event()
    seen = []
    original = main_bot._shadow_log.append

    def blocked(data):
        seen.append((data or {}).get("signal_id"))
        if not started.is_set():
            started.set()
            release.wait(timeout=3)
        return original(data)

    monkeypatch.setattr(main_bot._shadow_log, "append", blocked)

    async def run():
        main_bot.note_setup_disable_decision(
            {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": "q-0"},
            quantity=1, enforced=False, quantity_basis="requested", path="entry",
            fresh_order=True,
        )
        await asyncio.to_thread(started.wait, 1)
        for i in range(1, 4):
            main_bot.note_setup_disable_decision(
                {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": f"q-{i}"},
                quantity=1, enforced=False, quantity_basis="requested", path="entry",
                fresh_order=True,
            )
        mid = main_bot.setup_disable_status()["ledger"]
        started_core = time.perf_counter()
        core = await main_bot.trade_ledger.record("heartbeat", {"ok": True})
        core_elapsed = time.perf_counter() - started_core
        release.set()
        await asyncio.wait_for(main_bot._shadow_queue.join(), timeout=2)
        return mid, core, core_elapsed, main_bot.setup_disable_status()["ledger"], list(seen)

    import time
    mid, core, core_elapsed, done, wrote = asyncio.run(run())
    assert mid["queued"] == 2
    assert mid["dropped"] == 1
    assert mid["written"] == 0
    assert core is not None
    assert core_elapsed < 0.15
    assert done["written"] == 3
    assert done["dropped"] == 1
    assert "q-3" not in wrote
    assert wrote[0] == "q-0"


def test_account_id_substring_survives_the_written_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("ETRADE_ACCOUNT_ID", "00056000")
    saved = main_bot.trade_ledger._ledger
    main_bot.reset_setup_disable_state()
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    _open_shadow(tmp_path)
    occ = "FAKE  261218C00056000"
    order_id = "OID-00056000"
    signal_id = "sig-00056000"
    signal_key = f"FAKE|breakdown|{signal_id}|1"

    async def run():
        seq = await main_bot._record_open(
            "FAKE", 1, 56.0, 50.0, 70.0,
            {"occ_symbol": occ, "right": "CALL", "strike": 56.0, "expiration": "2026-12-18"},
            "BUY", "breakdown",
            signal_id=signal_id, instrument="option", signal_key=signal_key,
        )
        main_bot.note_setup_disable_decision(
            {
                "ticker": "FAKE", "setup": "breakdown", "action": "BUY",
                "option_right": "CALL", "strike": 56, "expiration_hint": "2026-12-18",
                "signal_id": signal_id, "accountId": "00056000",
            },
            quantity=1, enforced=False, quantity_basis="after_sizing", path="scanner",
            fresh_order=True, reserved=(1, signal_key),
            outcome={
                "order_id": order_id, "placement_status": "placed",
                "position_opened_seq": seq,
            },
        )
        await asyncio.wait_for(main_bot._shadow_queue.join(), timeout=2)
        return seq

    try:
        seq = asyncio.run(run())
        rows = main_bot.trade_ledger.recent(10)
        side = main_bot.read_setup_disable_shadow(10)
    finally:
        main_bot.trade_ledger._ledger = saved
        main_bot.reset_setup_disable_state()
    opened = next(row["data"] for row in rows if row["event"] == "position_opened")
    shadow = side[0]["data"]
    assert opened["signal_key"] == signal_key
    assert opened["contract"]["occ_symbol"] == occ
    assert shadow["would_be_contract"]["option_symbol"] == occ
    assert shadow["outcome"]["order_id"] == order_id
    assert shadow["outcome"]["position_opened_seq"] == seq
    assert shadow["signal_key"] == signal_key
    assert "00056000" in occ and "00056000" in order_id and "00056000" in signal_key


def test_shadow_redactor_drops_secret_keys_and_oauth_urls():
    raw = {
        "order_id": "OID-00056000",
        "accountId": "00056000",
        "accountIdKey": "KEY",
        "oauth_token": "tok",
        "oauth_token_secret": "sec",
        "secret": "s",
        "reason": "see https://api.example/oauth?oauth_token=SUPERSECRET",
        "nested": {"oauth_token": "tok", "keep": "yes"},
    }
    out = main_bot._shadow_ledger_value(raw, set())
    assert out["order_id"] == "OID-00056000"
    for key in ("accountId", "accountIdKey", "oauth_token", "oauth_token_secret", "secret"):
        assert key not in out
    assert "oauth_token" not in out["nested"]
    assert out["nested"]["keep"] == "yes"
    assert "https://" not in out["reason"]
    assert "SUPERSECRET" not in out["reason"]
    assert "oauth_token" not in out["reason"]


def test_scanner_signal_fills_underlying_source(monkeypatch):
    import scanner
    monkeypatch.setattr(scanner, "ITM_EXPRESSION", False)
    _reset(monkeypatch)
    candidate = {
        "ticker": "FAKE", "side": "long", "entry": 101.25, "stop": 99.0,
        "target": 110.0, "score": 70, "rvol": 2.0, "setup": "breakdown",
        "ts": "2026-11-30T16:00:00+00:00",
    }
    payload = scanner.build_signal(
        {"bid": 101.2, "ask": 101.3, "price_source": "massive"}, candidate, "live",
    )
    assert payload["underlying_source"] == "Massive"
    row = main_bot.compose_setup_disable_row(
        payload, quantity=1, enforced=False, quantity_basis="requested",
        path="scanner", fresh_order=True,
    )
    assert row["underlying_source"] == "Massive"
    assert row["underlying_price"] == 101.25


def test_attempt_claim_failure_stays_fail_open(monkeypatch):
    _reset(monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("claim broke")

    monkeypatch.setattr(main_bot, "_claim_setup_disable_attempt", boom)
    assert main_bot._reserve_setup_disable_attempt(
        {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": "x"}
    ) is None


def test_position_closed_copies_signal_key_and_open_seq(tmp_path, monkeypatch):
    saved = main_bot.trade_ledger._ledger
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    signal_key = "FAKE|breakdown|sig-close|1"

    async def balance(*, dry_run=False):
        return {"total": 100000.0, "available": 100000.0}

    async def quiet(*args, **kwargs):
        return None

    monkeypatch.setattr(main_bot, "_fetch_broker_balance", balance)
    monkeypatch.setattr(main_bot.alerts, "send", quiet)

    async def run():
        seq = await main_bot._record_open(
            "FAKECLOSE", 1, 10.0, 9.0, 12.0, None, "BUY", "breakdown",
            signal_id="sig-close", instrument="equity", signal_key=signal_key,
        )
        stored = await main_bot.state.get_position("FAKECLOSE")
        await main_bot._record_close(
            "FAKECLOSE", 10.5, {"ticker": "FAKECLOSE", "action": "BUY"},
            close_reason="close_fill",
        )
        return seq, stored

    try:
        seq, stored = asyncio.run(run())
        closed = [
            row["data"] for row in main_bot.trade_ledger.recent(10)
            if row.get("event") == "position_closed"
        ]
    finally:
        main_bot.trade_ledger._ledger = saved
    assert stored["signal_key"] == signal_key
    assert "opened_seq" not in stored
    assert closed[-1]["signal_key"] == signal_key
    assert closed[-1]["opened_seq"] == seq
    assert signal_key not in main_bot._opened_seq_by_signal


def test_open_ledger_write_does_not_restore_a_deleted_position(tmp_path):
    import time
    saved_ledger = main_bot.trade_ledger._ledger
    saved_state = main_bot.state
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    store = main_bot.StateStore(None)
    main_bot.state = store
    ledger = main_bot.trade_ledger._ledger
    original = ledger._append_line

    def slow(line):
        if "position_opened" in line:
            time.sleep(0.3)
        return original(line)

    ledger._append_line = slow

    async def run():
        async def delete_soon():
            await asyncio.sleep(0.1)
            await store.delete_position("FAKE")

        task = asyncio.create_task(delete_soon())
        await main_bot._record_open(
            "FAKE", 1, 10.0, 9.0, 12.0, None, "BUY", "breakdown",
            signal_id="gone", instrument="equity", signal_key="FAKE|breakdown|gone|1",
        )
        await task
        return await store.get_position("FAKE")

    try:
        left = asyncio.run(run())
    finally:
        main_bot.trade_ledger._ledger = saved_ledger
        main_bot.state = saved_state
    assert left is None


def test_open_ledger_write_does_not_clobber_a_concurrent_stop(tmp_path):
    import time
    saved_ledger = main_bot.trade_ledger._ledger
    saved_state = main_bot.state
    main_bot.trade_ledger.init(tmp_path / "ledger.jsonl")
    store = main_bot.StateStore(None)
    main_bot.state = store
    ledger = main_bot.trade_ledger._ledger
    original = ledger._append_line

    def slow(line):
        if "position_opened" in line:
            time.sleep(0.3)
        return original(line)

    ledger._append_line = slow

    async def run():
        async def stamp_stop():
            await asyncio.sleep(0.1)
            current = await store.get_position("FAKE")
            current["stop_order_id"] = "STOP-1"
            await store.set_position("FAKE", current)

        task = asyncio.create_task(stamp_stop())
        await main_bot._record_open(
            "FAKE", 1, 10.0, 9.0, 12.0, None, "BUY", "breakdown",
            signal_id="stop", instrument="equity", signal_key="FAKE|breakdown|stop|1",
        )
        await task
        return await store.get_position("FAKE")

    try:
        left = asyncio.run(run())
    finally:
        main_bot.trade_ledger._ledger = saved_ledger
        main_bot.state = saved_state
    assert left["stop_order_id"] == "STOP-1"
    assert "opened_seq" not in left


def test_shutdown_drain_counts_leftover_shadow_rows_as_dropped(tmp_path, monkeypatch):
    import threading
    _reset(monkeypatch, enabled="false", shadow="true")
    _open_shadow(tmp_path)
    started = threading.Event()
    release = threading.Event()
    original = main_bot._shadow_log.append

    def blocked(data):
        if not started.is_set():
            started.set()
            release.wait(timeout=3)
        return original(data)

    monkeypatch.setattr(main_bot._shadow_log, "append", blocked)

    async def run():
        main_bot.note_setup_disable_decision(
            {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": "d-0"},
            quantity=1, enforced=False, quantity_basis="requested", path="entry",
            fresh_order=True,
        )
        await asyncio.to_thread(started.wait, 1)
        for i in range(1, 3):
            main_bot.note_setup_disable_decision(
                {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": f"d-{i}"},
                quantity=1, enforced=False, quantity_basis="requested", path="entry",
                fresh_order=True,
            )
        dropped = await main_bot.drain_shadow_queue(timeout=0.1)
        status = main_bot.setup_disable_status()["ledger"]
        release.set()
        if main_bot._shadow_queue is not None:
            await asyncio.wait_for(main_bot._shadow_queue.join(), timeout=2)
        return dropped, status

    try:
        dropped, status = asyncio.run(run())
    finally:
        main_bot.reset_setup_disable_state()
    assert dropped == 2
    assert status["dropped"] == 2
    assert status["queued"] == 0


def test_shutdown_drain_writes_rows_that_finish_within_two_seconds(tmp_path):
    main_bot.reset_setup_disable_state()
    _open_shadow(tmp_path)

    async def run():
        for i in range(2):
            main_bot.note_setup_disable_decision(
                {"ticker": "FAKE", "setup": "breakdown", "action": "BUY", "signal_id": f"w-{i}"},
                quantity=1, enforced=False, quantity_basis="requested", path="entry",
                fresh_order=True,
            )
        dropped = await main_bot.drain_shadow_queue()
        return dropped, main_bot.read_setup_disable_shadow(10), main_bot.setup_disable_status()["ledger"]

    try:
        dropped, rows, status = asyncio.run(run())
    finally:
        main_bot.reset_setup_disable_state()
    assert dropped == 0
    assert status["written"] == 2
    assert status["dropped"] == 0
    assert [row["data"]["signal_id"] for row in rows] == ["w-0", "w-1"]
    assert main_bot._shadow_log.verify()[0] is True


def test_shadow_log_survives_a_file_wipe_via_the_mirror(tmp_path):
    main_bot.reset_setup_disable_state()
    path = tmp_path / "setup_disable_shadow.jsonl"
    _open_shadow(tmp_path)
    durable = []

    async def mirror(seq, rec_hash, line):
        durable.append(line)

    main_bot.set_shadow_mirror(mirror)
    payload = {
        "ticker": "FAKE", "setup": "breakdown", "action": "BUY",
        "option_right": "CALL", "signal_id": "restore-me",
    }

    async def write_two():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "1", "placement_status": "placed"},
        )
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "2", "placement_status": "placed"},
        )

    asyncio.run(write_two())
    assert [row["data"]["attempt"] for row in main_bot.read_setup_disable_shadow(10)] == [1, 2]
    path.unlink()
    restored = main_bot.restore_shadow_lines(path, durable)
    main_bot.init_setup_disable_shadow_log(path)
    main_bot.reset_setup_disable_state()
    main_bot._shadow_ledger_counts["durable"] = len(durable)
    main_bot._shadow_ledger_counts["on_disk"] = restored
    main_bot.seed_setup_disable_from_ledger()

    async def write_third():
        await main_bot.persist_setup_disable_decision(
            payload, quantity=1, enforced=False, quantity_basis="after_sizing",
            path="entry", fresh_order=True,
            outcome={"order_id": "3", "placement_status": "placed"},
        )

    try:
        asyncio.run(write_third())
        rows = main_bot.read_setup_disable_shadow(10)
        core = main_bot.trade_ledger.recent(20) if main_bot.trade_ledger._ledger else []
        status = main_bot.setup_disable_status()["ledger"]
    finally:
        main_bot.set_shadow_mirror(None)
        main_bot.reset_setup_disable_state()
    assert restored == 2
    assert [row["data"]["attempt"] for row in rows] == [1, 2, 3]
    keys = [row["data"]["signal_key"] for row in rows]
    assert len(keys) == len(set(keys))
    assert all(row.get("event") != "setup_disable_shadow" for row in core)
    assert status["on_disk"] == 3
    assert status["durable"] == 3
    assert "bot_shadow_ledger" in main_bot._shadow_ledger_table_sql()
    assert "bot_ledger" not in main_bot._shadow_ledger_table_sql().split("bot_shadow_ledger")[0]


def test_fsync_failure_does_not_break_the_next_shadow_hash(tmp_path, monkeypatch):
    main_bot.reset_setup_disable_state()
    _open_shadow(tmp_path)
    log = main_bot._shadow_log
    real = main_bot.os.fsync

    def boom(fd):
        raise OSError("fsync failed")

    monkeypatch.setattr(main_bot.os, "fsync", boom)
    try:
        log.append({"setup": "breakdown", "signal_id": "a"})
    except OSError:
        pass
    monkeypatch.setattr(main_bot.os, "fsync", real)
    log.append({"setup": "breakdown", "signal_id": "b"})
    ok, checked, err = log.verify()
    assert ok is True, err
    assert checked == 2


def test_overlapping_appends_keep_one_shadow_chain(tmp_path, monkeypatch):
    import threading
    import time
    main_bot.reset_setup_disable_state()
    _open_shadow(tmp_path)
    log = main_bot._shadow_log
    real = os.fsync
    started = threading.Event()
    release = threading.Event()

    def slow(fd):
        if not started.is_set():
            started.set()
            release.wait(timeout=2)
        return real(fd)

    monkeypatch.setattr(main_bot.os, "fsync", slow)
    errors = []

    def one(payload):
        try:
            log.append(payload)
        except Exception as exc:
            errors.append(type(exc).__name__)

    first = threading.Thread(target=one, args=({"signal_id": "1"},))
    second = threading.Thread(target=one, args=({"signal_id": "2"},))
    first.start()
    assert started.wait(1)
    second.start()
    time.sleep(0.05)
    release.set()
    first.join(3)
    second.join(3)
    ok, checked, err = log.verify()
    assert not errors
    assert ok is True, err
    assert checked == 2
    assert [row["seq"] for row in log.recent(10)] == [1, 2]


def test_partial_trailing_line_is_truncated_so_verify_passes(tmp_path):
    main_bot.reset_setup_disable_state()
    _open_shadow(tmp_path)
    log = main_bot._shadow_log
    log.append({"signal_id": "whole"})
    with open(log.path, "a", encoding="utf-8") as handle:
        handle.write('{"seq": 2, "partial"')
    reopened = main_bot.SetupDisableShadowLog(log.path)
    ok, checked, err = reopened.verify()
    assert ok is True, err
    assert checked == 1
    assert reopened.partial_truncated == 1


def test_dead_shadow_writer_does_not_make_shutdown_wait(tmp_path):
    import time
    main_bot.reset_setup_disable_state()
    _open_shadow(tmp_path)

    async def run():
        async def dead():
            raise RuntimeError("writer dead")

        task = asyncio.create_task(dead())
        with pytest.raises(RuntimeError):
            await task
        main_bot._shadow_writer = task
        main_bot._shadow_queue = asyncio.Queue()
        main_bot._shadow_queue.put_nowait(({"signal_id": "left"}, None))
        main_bot._shadow_inflight = 1
        started = time.perf_counter()
        dropped = await main_bot.drain_shadow_queue()
        return dropped, time.perf_counter() - started, main_bot.setup_disable_status()["ledger"]["dropped"]

    try:
        dropped, elapsed, counted = asyncio.run(run())
    finally:
        main_bot.reset_setup_disable_state()
    assert dropped == 1
    assert counted == 2
    assert elapsed < 0.5
