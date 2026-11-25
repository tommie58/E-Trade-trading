"""SETUP_DISABLE_ENABLED. Flag off does not change an admit or a size."""
import asyncio
import importlib.util
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
    named = replay.replay_trades(replay.load_csv(replay.TRADES), disabled)
    skipped_setups = {row["setup_id"] for row in named["skipped"]}
    assert skipped_setups <= {"breakdown", "vwap rejection", "momentum"}
    assert "momentum fade" not in skipped_setups
    book = replay.replay_setup_table(replay.load_csv(replay.SETUPS), disabled)
    assert book["trades"] == 57
    assert book["skipped_n"] == 27
    assert book["book_net"] == replay._money("-1063.28")
