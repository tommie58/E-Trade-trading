"""A 9:30 scanner print is not the regular-session VWAP.

2026-11-25 10:26 ET: SPY session sample 700.3759 versus Massive RTH minute
VWAP 703.4367. The gate reported 41.93 bps and failed closed. The seed
skipped the hash because first_ts was the 9:30 minute.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import main_bot


ET = ZoneInfo("America/New_York")
# 10:26 ET is 15:26 UTC. The live snapshot was this minute.
NOW = datetime(2026, 11, 25, 10, 26, 30, tzinfo=ET)
DAY = NOW.date().isoformat()
SAMPLE = 700.3759
RTH = 703.4367


def _run(coro):
    return asyncio.run(coro)


def _minute(hour: int, minute: int) -> int:
    return hour * 60 + minute


def _bar(minute: int, price: float, volume: float = 1000.0) -> dict:
    when = datetime(2026, 11, 25, minute // 60, minute % 60, tzinfo=ET)
    return {"t": int(when.timestamp() * 1000), "vw": price, "v": volume, "c": price}


def _rth_bars() -> list:
    # Completed minutes only: 9:30 through 10:25. 10:26 is still forming.
    return [_bar(m, RTH) for m in range(_minute(9, 30), _minute(10, 26))]


def _premarket() -> list:
    return [_bar(m, 690.0, 5_000_000) for m in range(_minute(4, 0), _minute(9, 30))]


def _open_sample() -> dict:
    first = datetime(2026, 11, 25, 9, 30, tzinfo=ET)
    last = datetime(2026, 11, 25, 10, 25, tzinfo=ET)
    return {
        "sum_pv": repr(SAMPLE * 100_000.0),
        "sum_v": repr(100_000.0),
        "count": "27",
        "first_ts": first.isoformat(),
        "last_ts": last.isoformat(),
        "last_volume": repr(8_000_000.0),
    }


def test_open_sample_is_not_seeded_and_diverges_from_rth_minutes():
    fields = _open_sample()
    assert main_bot.scanner_mod.session_vwap_needs_seed(fields, NOW) is False
    skipped = main_bot.scanner_mod.plan_session_vwap_seed(
        fields, _premarket() + _rth_bars(), NOW, max_age_s=300,
    )
    assert skipped["applied"] is False
    assert skipped["reason"] == "not_needed"
    gap = main_bot._vwap_diff_bps(RTH, SAMPLE)
    assert gap == pytest.approx(43.70, abs=0.05)
    assert abs(gap) > 25


def test_rebase_matches_rth_vwap_and_keeps_the_live_volume_anchor():
    fields = _open_sample()
    bars = _premarket() + _rth_bars() + [_bar(_minute(10, 26), 1.0, 9_000_000)]
    plan = main_bot.scanner_mod.plan_session_vwap_seed(
        fields, bars, NOW, max_age_s=300, replace=True,
    )
    assert plan["applied"] is True
    assert plan["reason"] == "rebased"
    assert plan["vwap"] == pytest.approx(RTH)
    assert plan["vwap"] != pytest.approx(SAMPLE)
    gap = main_bot._vwap_diff_bps(RTH, plan["vwap"])
    assert abs(gap) < 1.0
    # Premarket volume would have pulled the average down. It did not.
    assert plan["vwap"] != pytest.approx(
        (690.0 * 5_000_000 * 330 + RTH * 1000 * plan["bars_used"])
        / (5_000_000 * 330 + 1000 * plan["bars_used"])
    )
    assert float(plan["fields"]["last_volume"]) == pytest.approx(8_000_000.0)
    again = main_bot.scanner_mod.plan_session_vwap_seed(
        plan["fields"], bars, NOW, max_age_s=300, replace=True,
    )
    assert again["applied"] is False
    assert again["reason"] == "not_needed"


def test_stale_minute_bars_do_not_replace_the_sample():
    later = NOW + timedelta(minutes=20)
    plan = main_bot.scanner_mod.plan_session_vwap_seed(
        _open_sample(), _rth_bars(), later, max_age_s=300, replace=True,
    )
    assert plan["applied"] is False
    assert plan["reason"] == "stale"
    assert plan["fields"] is None


def test_gate_stays_closed_on_the_live_gap_and_opens_after_the_rebase():
    """The cross-check is unchanged: a wide gap or a stale print fail-closes."""
    divergent = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": SAMPLE, "reason": None},
        {"vwap": RTH, "reason": None, "age_seconds": 121.2},
    )
    assert divergent["measured"] is False
    assert divergent["reason"] == "crosscheck_gap_bps"
    assert abs(divergent["diff_bps"]) > 25

    plan = main_bot.scanner_mod.plan_session_vwap_seed(
        _open_sample(), _rth_bars(), NOW, max_age_s=300, replace=True,
    )
    aligned = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": plan["vwap"], "reason": None},
        {"vwap": RTH, "reason": None, "age_seconds": 121.2},
    )
    assert aligned["measured"] is True
    assert abs(aligned["diff_bps"]) < 1.0

    stale = main_bot._crosscheck_today_vwap(
        {"measured": True, "vwap": plan["vwap"], "reason": None},
        {"vwap": None, "reason": "massive_rth_stale", "age_seconds": 301},
    )
    assert stale["measured"] is False
    assert stale["reason"] == "massive_rth_stale"


def test_worker_adopts_the_rebase_when_the_open_minute_is_already_sampled(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    main_bot._vwap_seed_attempt_at.clear()
    bars = {"SPY": _premarket() + _rth_bars(), "QQQ": _rth_bars()}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            parts = url.split("/")
            symbol = parts[parts.index("ticker") + 1]
            payload = {"results": list(bars.get(symbol) or [])}
            return type("Resp", (), {
                "status_code": 200,
                "json": lambda _self, payload=payload: payload,
            })()

    monkeypatch.setattr(
        main_bot.httpx, "AsyncClient", lambda *args, **kwargs: _Client(),
    )

    async def scenario():
        key = main_bot.scanner_mod.session_vwap_key("SPY", DAY)
        await store.hash_set(key, _open_sample(), main_bot.scanner_mod.SESSION_VWAP_TTL_S)
        await main_bot.ensure_index_session_vwap_seed(["SPY"])
        return await main_bot.scanner_mod.read_session_vwap(store, "SPY", NOW)

    reading = _run(scenario())
    assert reading["measured"] is True
    assert reading["vwap"] == pytest.approx(round(RTH, 4))
    main_bot._vwap_seed_attempt_at.clear()


def test_report_exposes_the_gap_and_stays_fail_closed_when_stale(monkeypatch):
    store = main_bot.StateStore(None)
    monkeypatch.setattr(main_bot, "state", store)
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))

    async def scenario():
        key = main_bot.scanner_mod.session_vwap_key("SPY", DAY)
        await store.hash_set(key, _open_sample(), main_bot.scanner_mod.SESSION_VWAP_TTL_S)
        qkey = main_bot.scanner_mod.session_vwap_key("QQQ", DAY)
        await store.hash_set(qkey, _open_sample(), main_bot.scanner_mod.SESSION_VWAP_TTL_S)
        main_bot._minute_vwap_cache.clear()
        for sym in ("SPY", "QQQ"):
            main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key(sym, DAY)] = {
                "vwap": RTH if sym == "SPY" else 615.4453,
                "last_bar_epoch": NOW.timestamp(),
                "last_bar_ts": NOW.isoformat(),
                "fetched_at": NOW.timestamp(),
                "bar_count": 56,
                "session_date": DAY,
            }
        report = await main_bot.index_vwap_gate_measurement()
        return report

    report = _run(scenario())
    assert report["mode"] == "fail_closed"
    spy = next(row for row in report["indexes"] if row["symbol"] == "SPY")
    assert spy["session_vwap"] == pytest.approx(round(SAMPLE, 4))
    assert spy["massive_minute_vwap_rth"] == pytest.approx(RTH)
    assert abs(spy["gap_bps"]) > 25
    assert spy["measured"] is False
    assert spy["gate_reason"] == "crosscheck_gap_bps"
