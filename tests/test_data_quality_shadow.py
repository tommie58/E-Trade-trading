"""Shadow data-quality metrics. Live gate inputs stay on the sampled path.

Massive 1-minute bars and IV are required. A missing, stale, 401, or 403
dependency records status=no_data and does not copy the sampled VWAP (or any
Yahoo volume) into the shadow result.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

import main_bot
import pro_trader.data_quality as dq


ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 11, 24, 12, 2, 5, tzinfo=ET)
KEY = "unit-test-key"


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _reset():
    dq.reset_shadow()
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._minute_vwap_refresh_meta.clear()
    yield
    dq.reset_shadow()
    main_bot._minute_vwap_cache.clear()
    main_bot._minute_vwap_attempt_at.clear()
    main_bot._minute_vwap_refresh_meta.clear()


def _ms(hour, minute):
    return datetime(2026, 11, 24, hour, minute, tzinfo=ET).timestamp() * 1000


def _bar(hour, minute, *, vw, vol, high=None, low=None, open_=None, close=None):
    price = vw
    return {
        "t": _ms(hour, minute),
        "o": price if open_ is None else open_,
        "h": price if high is None else high,
        "l": price if low is None else low,
        "c": price if close is None else close,
        "vw": vw,
        "v": vol,
    }


def _ok_feeds():
    dq.note_bars_feed({"status": dq.SHADOW_OK, "reason": None}, session="2026-11-24")
    dq.note_iv_feed({"status": dq.SHADOW_OK, "reason": None}, session="2026-11-24")
    dq.note_symbol_bars("SPY", {"status": dq.SHADOW_OK, "reason": None})
    dq.note_symbol_bars("QQQ", {"status": dq.SHADOW_OK, "reason": None})


def test_completed_bar_vwap_is_sum_vw_v_over_sum_v():
    bars = [
        _bar(8, 0, vw=100, vol=5000),
        _bar(9, 30, vw=200, vol=100),
        _bar(9, 31, vw=210, vol=300),
        _bar(12, 2, vw=999, vol=100),
        _bar(16, 0, vw=300, vol=500),
    ]
    out = dq.completed_rth_vwap(bars, NOW)
    assert out["vwap"] == pytest.approx(207.5)
    assert out["bar_count"] == 2
    assert out["age_seconds"] == pytest.approx((12 * 60 + 2) * 60 + 5 - (9 * 60 + 31) * 60)


def test_stale_and_missing_bars_are_no_data_and_do_not_copy_sampled():
    _ok_feeds()
    missing = dq.publish_vwap_row("QQQ", {
        "sampled_vwap": 634.0,
        "gate_vwap": 634.0,
        "bar_vwap": None,
        "bar_age_s": None,
        "bar_count": 0,
    })
    assert missing["status"] == "no_data"
    assert missing["reason"] == "missing"
    assert missing["bar_vwap"] is None
    assert missing["sampled_vwap"] is None
    assert missing["diff_bps"] is None
    assert missing["live_gate_vwap"] == pytest.approx(634.0)

    stale = dq.publish_vwap_row("QQQ", {
        "sampled_vwap": 634.0,
        "gate_vwap": 634.0,
        "bar_vwap": 630.0,
        "bar_age_s": 150.1,
        "bar_count": 12,
    })
    assert stale["status"] == "no_data"
    assert stale["reason"] == "stale"
    assert stale["bar_vwap"] is None
    assert stale["bar_vwap"] != 634.0
    assert stale["sampled_vwap"] is None
    fresh_edge = dq.publish_vwap_row("QQQ", {
        "sampled_vwap": 634.0,
        "gate_vwap": 634.0,
        "bar_vwap": 633.0,
        "bar_age_s": 150.0,
        "bar_count": 12,
    })
    assert fresh_edge["status"] == "ok"
    assert fresh_edge["bar_vwap"] == pytest.approx(633.0)
    assert fresh_edge["sampled_vwap"] == pytest.approx(634.0)
    assert fresh_edge["diff_bps"] == pytest.approx((633.0 - 634.0) / 634.0 * 10000, abs=0.02)


def test_http_401_and_403_fail_closed_for_bars_and_iv():
    assert dq.classify_http_dependency(401, has_data=False)["reason"] == "unauthorized"
    assert dq.classify_http_dependency(403, has_data=False)["reason"] == "forbidden"
    assert dq.classify_http_dependency(401, has_data=False, kind="iv")["reason"] == "iv_unauthorized"
    assert dq.classify_http_dependency(403, has_data=False, kind="iv")["reason"] == "iv_forbidden"
    dq.note_bars_feed(dq.classify_http_dependency(403, has_data=False))
    dq.note_iv_feed({"status": dq.SHADOW_OK, "reason": None})
    row = dq.publish_vwap_row("SPY", {
        "sampled_vwap": 715.0,
        "gate_vwap": 715.0,
        "bar_vwap": 710.0,
        "bar_age_s": 60,
        "bar_count": 8,
    })
    assert row["status"] == "no_data"
    assert row["reason"] == "forbidden"
    assert row["bar_vwap"] is None
    assert row["diff_bps"] is None
    dq.note_bars_feed({"status": dq.SHADOW_OK, "reason": None})
    dq.note_iv_feed(dq.classify_http_dependency(401, has_data=False, kind="iv"))
    denied = dq.publish_vwap_row("SPY", {
        "sampled_vwap": 715.0,
        "gate_vwap": 715.0,
        "bar_vwap": 710.0,
        "bar_age_s": 60,
        "bar_count": 8,
    })
    assert denied["status"] == "no_data"
    assert denied["reason"] == "iv_unauthorized"
    assert denied["bar_vwap"] is None
    assert not hasattr(dq, "yahoo_vwap")
    source = open(dq.__file__, encoding="utf-8").read().lower()
    assert "yfinance" not in source
    assert "yahoo.com" not in source


def test_iv_payload_missing_and_stale():
    now = NOW.timestamp()
    empty = dq.classify_iv_payload({"results": []}, now, status_code=200)
    assert empty["status"] == "no_data"
    assert empty["reason"] == "iv_missing"
    fresh_ns = int((NOW - timedelta(seconds=2)).timestamp() * 1e9)
    ok = dq.classify_iv_payload({
        "results": [{
            "implied_volatility": 0.18,
            "last_quote": {"last_updated": fresh_ns},
        }],
    }, now, status_code=200)
    assert ok["status"] == "ok"
    old_ns = int((NOW - timedelta(seconds=120)).timestamp() * 1e9)
    stale = dq.classify_iv_payload({
        "results": [{
            "implied_volatility": 0.18,
            "last_quote": {"last_updated": old_ns},
        }],
    }, now, status_code=200)
    assert stale["status"] == "no_data"
    assert stale["reason"] == "iv_stale"
    denied = dq.classify_iv_payload({}, now, status_code=403)
    assert denied["reason"] == "iv_forbidden"


def test_bad_tick_catches_648_and_leaves_normal_bars():
    bars = []
    for minute in range(40, 60):
        bars.append(_bar(11, minute, vw=633.95, vol=10000, open_=633.90, high=634.10, low=633.80, close=634.00))
    bars.append(_bar(12, 1, vw=634.05, vol=12000, open_=633.90, high=648.00, low=633.80, close=634.00))
    report = dq.filter_bad_ticks(bars, "QQQ")
    assert report["flagged_count"] == 1
    assert report["flagged"][0]["high"] == pytest.approx(648.0)
    assert report["flagged"][0]["field"] == "high"
    assert float(report["bars"][-1]["h"]) < 640
    normal = [bar for bar in report["flagged"] if bar.get("t") != bars[-1]["t"]]
    assert normal == []
    off = dq.series_for_extremes(bars, "QQQ")
    assert off["applied"] is False
    assert off["bars"][-1]["h"] == pytest.approx(648.0)
    assert off["flagged_count"] == 1


def test_bad_tick_k_flags_a_name_and_ignores_a_small_wick(monkeypatch):
    monkeypatch.delenv("BAD_TICK_K", raising=False)
    bars = []
    for minute in range(20):
        bars.append(_bar(10, minute, vw=100, vol=1000, open_=100.0, high=100.20, low=99.90, close=100.05))
    quiet = dq.filter_bad_ticks(bars, "NVDA")
    assert quiet["flagged_count"] == 0
    bars.append(_bar(10, 20, vw=100, vol=1000, open_=100.0, high=103.50, low=99.90, close=100.05))
    spiked = dq.filter_bad_ticks(bars, "NVDA")
    assert spiked["flagged_count"] == 1


def test_snapshot_gap_uses_official_open_not_the_first_bar():
    official = dq.snapshot_gap_pct(640.04, 651.03)
    assert official == pytest.approx(-1.688, abs=1e-3)
    first_bar = dq.snapshot_gap_pct(641.20, 651.03)
    assert first_bar != official
    spy = dq.snapshot_gap_pct(715.148, 722.29)
    assert spy == pytest.approx(-0.989, abs=1e-3)
    _ok_feeds()
    quote = {
        "symbol": "QQQ",
        "open": 639.55,
        "previous_close": 651.03,
        "massive_official_open": 640.04,
        "massive_official_prev_close": 651.03,
    }
    verdict = {"gap_pct": -1.763, "verified": True}
    out = dq.apply_gap_source(quote, verdict, lambda o, c: {"gap_pct": dq.snapshot_gap_pct(o, c), "verified": True})
    assert out is verdict
    assert out["gap_pct"] == pytest.approx(-1.763)
    logged = dq.shadow_status()["gaps"]["QQQ"]
    assert logged["status"] == "ok"
    assert logged["snapshot_gap_pct"] == pytest.approx(-1.688, abs=1e-3)
    dq.note_bars_feed(dq.classify_http_dependency(401, has_data=False))
    dq.apply_gap_source(quote, verdict, lambda o, c: {"gap_pct": 0, "verified": True})
    blocked = dq.shadow_status()["gaps"]["QQQ"]
    assert blocked["status"] == "no_data"
    assert "unauthorized" in blocked["reason"]
    assert blocked["snapshot_gap_pct"] is None
    assert blocked["snapshot_gap_pct"] != verdict["gap_pct"]
    assert out["gap_pct"] == pytest.approx(-1.763)


def test_cadence_default_is_90s_and_60s_aligns_after_the_close(monkeypatch):
    monkeypatch.delenv("AGGS_PULL_INTERVAL_S", raising=False)
    assert dq.aggs_pull_interval_s() == 90
    assert dq.aggs_alignment_active() is False
    now = datetime(2026, 11, 24, 12, 2, 40, tzinfo=ET)
    assert dq.agg_pull_due(0, 0, 90, False, now) is True
    assert dq.agg_pull_due(now.timestamp(), now.timestamp() - 89, 90, False, now) is False
    assert dq.agg_pull_due(now.timestamp(), now.timestamp() - 90, 90, False, now) is True
    monkeypatch.setenv("AGGS_PULL_INTERVAL_S", "60")
    assert dq.aggs_pull_interval_s() == 60
    assert dq.aggs_alignment_active() is True
    last = datetime(2026, 11, 24, 12, 1, 40, tzinfo=ET).timestamp()
    assert dq.agg_pull_due(now.timestamp(), last, 60, True, now) is True
    early = datetime(2026, 11, 24, 12, 2, 3, tzinfo=ET)
    last_early = datetime(2026, 11, 24, 12, 1, 6, tzinfo=ET).timestamp()
    assert dq.agg_pull_due(early.timestamp(), last_early, 60, True, early) is False
    budget = dq.call_budget()
    assert budget["requests_per_pull_per_symbol"] == 1
    assert budget["current_requests_per_hour"] == "48-80"
    assert budget["proposal_requests_per_hour"] == 120
    assert dq.agg_path_range("2026-11-24", None) == ("2026-11-24", "2026-11-24")
    assert dq.agg_path_range("2026-11-24", 1_700_000_000.0)[0] == "1700000000000"
    merged = dq.merge_bars(
        [{"t": 1_700_000_000_000, "vw": 10, "v": 1}],
        [{"t": 1_700_000_060_000, "vw": 12, "v": 1}, {"t": 1_700_000_000_000, "vw": 11, "v": 2}],
    )
    assert len(merged) == 2
    assert merged[0]["vw"] == 11


def test_flags_at_default_leave_the_gate_row_unchanged(monkeypatch):
    monkeypatch.delenv("VWAP_SOURCE", raising=False)
    monkeypatch.delenv("GAP_SOURCE", raising=False)
    monkeypatch.delenv("BAD_TICK_FILTER_ENABLED", raising=False)
    _ok_feeds()
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    bars = [_bar(12, 0, vw=710.0, vol=100), _bar(12, 1, vw=730.0, vol=100)]
    day = NOW.date().isoformat()
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key("SPY", day)] = {
        "vwap": 714.1,
        "bars": bars,
        "last_bar_epoch": datetime(2026, 11, 24, 12, 1, tzinfo=ET).timestamp(),
        "session_date": day,
    }
    row = {
        "symbol": "SPY",
        "last": 716.25,
        "vwap": 714.64,
        "vwap_source": "session_sampled",
        "gate_vwap_source": "session_sampled",
        "gate_vwap_verdict": "above",
        "gate_vwap_reason": None,
    }
    before = dict(row)
    resolved = {"measured": True, "vwap": 714.64, "samples": 12}
    main_bot._note_data_quality_vwap(row, resolved, NOW)
    assert row == before
    published = dq.shadow_status()["symbols"]["SPY"]
    assert published["status"] == "ok"
    assert published["bar_vwap"] == pytest.approx(720.0)
    assert published["bar_vwap"] != pytest.approx(714.64)
    assert published["live_gate_vwap"] == pytest.approx(714.64)


def test_unauthorized_bars_do_not_move_the_gate_even_if_bars_source_is_set(monkeypatch):
    monkeypatch.setenv("VWAP_SOURCE", "bars")
    dq.note_bars_feed(dq.classify_http_dependency(401, has_data=False))
    dq.note_iv_feed({"status": dq.SHADOW_OK, "reason": None})
    dq.note_symbol_bars("SPY", {"status": dq.SHADOW_OK, "reason": None})
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    day = NOW.date().isoformat()
    main_bot._minute_vwap_cache[main_bot._minute_vwap_cache_key("SPY", day)] = {
        "bars": [_bar(12, 0, vw=800, vol=10), _bar(12, 1, vw=800, vol=10)],
        "session_date": day,
    }
    row = {
        "symbol": "SPY",
        "last": 716.25,
        "vwap": 714.64,
        "vwap_source": "session_sampled",
        "gate_vwap_source": "session_sampled",
        "gate_vwap_verdict": "above",
        "gate_vwap_reason": None,
    }
    before = dict(row)
    main_bot._note_data_quality_vwap(row, {"measured": True, "vwap": 714.64}, NOW)
    assert row == before
    published = dq.shadow_status()["symbols"]["SPY"]
    assert published["status"] == "no_data"
    assert published["reason"] == "unauthorized"
    assert published["bar_vwap"] is None


def test_entitlement_probe_logs_status_without_keys_or_urls(monkeypatch, caplog):
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    seen = {"n": 0, "urls": []}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            seen["n"] += 1
            seen["urls"].append(str(url))
            assert headers and KEY in headers.get("Authorization", "")
            if "/v3/snapshot/options/" in str(url):
                return httpx.Response(403, json={"status": "NOT_AUTHORIZED"}, request=httpx.Request("GET", url))
            return httpx.Response(401, json={"status": "unauthorized"}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    caplog.set_level(logging.WARNING, logger="etrade-bot.data-quality")
    caplog.set_level(logging.WARNING, logger="etrade-bot")
    fields = _run(main_bot.probe_data_quality_entitlement(force=True))
    assert fields["bars"] == "no_data"
    assert fields["bars_reason"] == "unauthorized"
    assert fields["iv"] == "no_data"
    assert fields["iv_reason"] == "iv_forbidden"
    assert seen["n"] == 2
    again = _run(main_bot.probe_data_quality_entitlement())
    assert again["bars_reason"] == "unauthorized"
    assert seen["n"] == 2
    text = caplog.text
    assert "unauthorized" in text
    assert "iv_forbidden" in text
    assert KEY not in text
    assert "Bearer" not in text
    assert "http" not in text.lower()
    assert "massive.com" not in text
    for url in seen["urls"]:
        assert url not in text


def test_probe_runs_again_on_the_next_session(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    clock = {"now": NOW}

    monkeypatch.setattr(main_bot, "_utcnow", lambda: clock["now"].astimezone(timezone.utc))
    calls = {"n": 0}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            calls["n"] += 1
            if "/options/" in str(url):
                stamp = int(clock["now"].timestamp() * 1e9)
                return httpx.Response(200, json={"results": [{
                    "implied_volatility": 0.2,
                    "last_quote": {"last_updated": stamp},
                }]}, request=httpx.Request("GET", "https://probe.invalid/iv"))
            bar_ms = (clock["now"] - timedelta(seconds=70)).timestamp() * 1000
            return httpx.Response(200, json={"results": [{
                "t": bar_ms, "vw": 634.0, "v": 100, "c": 634.0,
            }]}, request=httpx.Request("GET", "https://probe.invalid/bars"))

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    first = _run(main_bot.probe_data_quality_entitlement(force=True))
    assert first["bars"] == "ok"
    assert first["iv"] == "ok"
    assert calls["n"] == 2
    _run(main_bot.probe_data_quality_entitlement())
    assert calls["n"] == 2
    clock["now"] = NOW + timedelta(days=1)
    second = _run(main_bot.probe_data_quality_entitlement())
    assert second["session"] == "2026-11-25"
    assert calls["n"] == 4


def test_refresh_403_records_no_data_and_does_not_log_the_target(monkeypatch, caplog):
    monkeypatch.setenv("MASSIVE_API_KEY", KEY)
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    caplog.set_level(logging.WARNING, logger="etrade-bot")

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, params=None, headers=None):
            return httpx.Response(403, json={}, request=httpx.Request("GET", "https://probe.invalid/aggs"))

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    _run(main_bot.refresh_massive_minute_vwap(["SPY"]))
    assert dq.bars_feed()["status"] == "no_data"
    assert dq.bars_feed()["reason"] == "forbidden"
    assert KEY not in caplog.text
    assert "http" not in caplog.text.lower()
    assert "probe.invalid" not in caplog.text


def test_status_exposes_the_shadow_block(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    monkeypatch.setattr(main_bot, "_utcnow", lambda: NOW.astimezone(timezone.utc))
    from fastapi.testclient import TestClient

    with TestClient(main_bot.app) as client:
        body = client.get("/pro-trader/status", headers={"X-Rork-Secret": "s3cret"}).json()
    shadow = body["data_quality_shadow"]
    assert shadow["read_only"] is True
    assert shadow["status"] == "no_data"
    assert shadow["vwap_source"] == "sampled"
    assert shadow["gap_source"] == "current"
    assert shadow["bad_tick_filter_enabled"] is False
    assert shadow["aggs_pull_interval_s"] == 90
    assert shadow["call_budget"]["proposal_requests_per_hour"] == 120
    assert shadow["entitlement"]["bars"]["status"] == "no_data"
    assert shadow["bad_ticks_flagged"] is None
