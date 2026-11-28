"""Shadow-only Massive chain-greeks strike comparison. No live order changes."""
from __future__ import annotations

import asyncio
import inspect
import json
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

import main_bot


TODAY = date(2026, 11, 24)
FRIDAY = date(2026, 11, 27)
NOW_NS = 1_763_942_400  # 2026-11-24 15:00:00 UTC, far from the quote stamps below


def _run(coro):
    return asyncio.run(coro)


def _quote(now_s: float, *, age_s: float = 1.0, timeframe: str = "REAL-TIME",
           bid: float = 5.0, ask: float = 5.1) -> dict:
    return {
        "bid": bid,
        "ask": ask,
        "timeframe": timeframe,
        "last_updated": int((now_s - age_s) * 1_000_000_000),
    }


def _leg(strike: float, expiry: str, delta, *, theta=-0.04, oi=500, volume=80,
         bid=5.0, ask=5.1, timeframe="REAL-TIME", age_s=1.0, iv=0.22,
         greeks=True, now_s=NOW_NS, include_oi=True):
    item = {
        "details": {
            "expiration_date": expiry,
            "strike_price": strike,
            "contract_type": "call",
            "ticker": f"O:TEST{expiry[2:4]}{expiry[5:7]}{expiry[8:10]}C{int(round(strike * 1000)):08d}",
        },
        "last_quote": _quote(now_s, age_s=age_s, timeframe=timeframe, bid=bid, ask=ask),
        "day": {"volume": volume} if volume is not None else {},
        "implied_volatility": iv,
        "underlying_asset": {"price": 500.0, "ticker": "TEST"},
    }
    if include_oi:
        item["open_interest"] = oi
    if greeks and delta is not None:
        item["greeks"] = {"delta": delta, "theta": theta, "gamma": 0.02, "vega": 0.08}
    elif greeks:
        item["greeks"] = {"theta": theta}
    return item


def _row(strike, expiry, delta, **kwargs):
    """One already-translated row, for the pure scorer."""
    oi = kwargs.pop("oi", 500)
    include_oi = kwargs.pop("include_oi", True)
    return {
        "expiry": expiry,
        "strike": strike,
        "right": "CALL",
        "bid": kwargs.get("bid", 5.0),
        "ask": kwargs.get("ask", 5.1),
        "theta": kwargs.get("theta", -0.04),
        "delta": delta,
        "gamma": 0.02,
        "vega": 0.08,
        "iv": kwargs.get("iv", 0.22),
        "open_interest": None if not include_oi else oi,
        "volume": kwargs.get("volume", 80),
        "quote_age_s": kwargs.get("quote_age_s", 1.0),
        "timeframe": kwargs.get("timeframe", "REAL-TIME"),
        "delayed": kwargs.get("timeframe", "REAL-TIME") == "DELAYED",
        "stale": kwargs.get("stale", False),
        "occ": None,
    }


class _Resp:
    def __init__(self, payload, status=200):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


class _ChainHttp:
    """httpx stand-in. Headers are accepted and never stored."""

    calls = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, params=None, headers=None):
        _ChainHttp.calls.append((str(url), dict(params or {})))
        n = len(_ChainHttp.calls)
        body = {"results": [_leg(100 + n, FRIDAY.isoformat(), 0.61)], "status": "OK"}
        if n < 4:
            body["next_url"] = f"https://api.massive.com/v3/snapshot/options/SPY?cursor={n}"
        return _Resp(body)


def test_chain_shadow_defaults_off(monkeypatch):
    monkeypatch.delenv("CHAIN_SHADOW_ENABLED", raising=False)
    assert main_bot.chain_shadow_enabled() is False
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "false")
    assert main_bot.chain_shadow_enabled() is False
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    assert main_bot.chain_shadow_enabled() is True
    spec = main_bot.chain_shadow_comparison_spec()
    assert spec["min_pairs"] == 40
    assert spec["min_pairs_is_flip_rule"] is False
    assert spec["live_flip_permitted"] is False
    assert spec["signoff_required"] == [
        "TSIT Risk & Ops Monitor", "Chief of Staff", "Tommie",
    ]
    assert "same-contract" in spec["note"]
    assert "confidence interval" in spec["note"]


def test_hook_sits_after_the_armed_stop_and_skips_expression_refusal():
    src = inspect.getsource(main_bot.execute_live_order)
    arm = src.index("await _arm_stop_guard(")
    stamp = src.index("await state.set_position(str(symbol).upper(), pos_rec)")
    spawn = src.index("_spawn_chain_shadow(")
    assert arm < stamp < spawn
    assert src.index("_place_entry_with_funds_clamp") < spawn
    assert src.index("_record_open(") < spawn
    refused = src.index("entry_expression_refused")
    window = src[refused:src.index("return {", refused)]
    assert "_spawn_chain_shadow" not in window
    assert "chain_shadow" not in window
    body = inspect.getsource(main_bot._chain_shadow_body)
    run = inspect.getsource(main_bot._run_chain_shadow)
    assert body.index("await asyncio.sleep(0)") < body.index("MassiveClient(")
    assert "asyncio.wait_for" in run
    assert "_chain_shadow_body" in run
    joined = "\n".join(inspect.getsource(fn) for fn in (
        main_bot._chain_shadow_body,
        main_bot.select_chain_shadow,
        main_bot._run_chain_shadow_marks,
        main_bot._spawn_chain_shadow,
        main_bot.chain_shadow_worker,
    ))
    for name in ("_best_expression_contract", "_snap_option_contract", "_sync_etrade_call", "position_lock"):
        assert name not in joined


def test_parse_drops_missing_greeks_and_flags_delayed_and_stale():
    now_s = float(NOW_NS)
    payload = {"results": [
        _leg(100, FRIDAY.isoformat(), None, greeks=False),
        _leg(101, FRIDAY.isoformat(), 0.61, timeframe="DELAYED"),
        _leg(102, FRIDAY.isoformat(), 0.60, age_s=500, timeframe="REAL-TIME"),
        _leg(103, FRIDAY.isoformat(), None, greeks=True),
        _leg(104, FRIDAY.isoformat(), 0.62, include_oi=False, timeframe="REAL-TIME"),
    ]}
    translated = main_bot.massive.chain_snapshot_translate(payload, now_s)
    rows = main_bot.massive.chain_snapshot_to_rows(payload, now_s)
    strikes = [row["strike"] for row in rows]
    assert 100 not in strikes
    assert 103 not in strikes
    assert translated["dropped_greeks"] == 2
    delayed = next(row for row in rows if row["strike"] == 101)
    assert delayed["delayed"] is True
    assert delayed["timeframe"] == "DELAYED"
    stale = next(row for row in rows if row["strike"] == 102)
    assert stale["stale"] is True
    assert stale["delayed"] is False
    assert stale["timeframe"] == "REAL-TIME"
    missing_oi = next(row for row in rows if row["strike"] == 104)
    assert missing_oi["open_interest"] is None
    assert missing_oi["open_interest"] != 0
    assert "underlying" not in missing_oi
    zero = main_bot.massive.chain_snapshot_to_rows(
        {"results": [_leg(105, FRIDAY.isoformat(), 0.61, oi=0)]}, now_s,
    )
    assert zero[0]["open_interest"] == 0.0


def test_pick_parity_with_pick_best_expiry():
    rows = [
        _row(150, TODAY.isoformat(), 0.61, theta=-0.08),
        _row(148, FRIDAY.isoformat(), 0.60, theta=-0.02),
    ]
    selected = main_bot.select_chain_shadow(
        rows, spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    direct = main_bot.core_book.pick_best_expiry(rows, TODAY, 10 * 60)
    assert selected["shadow"]["expiry"] == direct["expiry"]
    assert selected["shadow"]["strike"] == direct["strike"]
    assert selected["shadow"]["cost"] == direct["cost"]
    assert selected["shadow_same_window"]["expiry"] == direct["expiry"]


def test_refuse_greeks_spread_oi_delta_and_late_0dte():
    missing = main_bot.select_chain_shadow(
        [_row(150, FRIDAY.isoformat(), None)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert missing["shadow"] is None
    assert missing["reasons"].get("greeks_missing", 0) >= 1

    wide = main_bot.select_chain_shadow(
        [_row(150, FRIDAY.isoformat(), 0.60, bid=1.0, ask=1.4)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert wide["shadow"] is None
    assert wide["reasons"].get("spread_wide", 0) >= 1

    thin = main_bot.select_chain_shadow(
        [_row(150, FRIDAY.isoformat(), 0.60, oi=40)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert thin["shadow"] is None
    assert thin["reasons"].get("oi_low", 0) >= 1

    outside = main_bot.select_chain_shadow(
        [_row(150, FRIDAY.isoformat(), 0.40)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert outside["shadow"] is None
    assert outside["reasons"].get("delta_outside", 0) >= 1

    late = main_bot.select_chain_shadow(
        [_row(150, TODAY.isoformat(), 0.60, theta=-0.01)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=15 * 60,
    )
    assert late["shadow"] is None
    assert late["reasons"].get("zero_dte_cutoff", 0) >= 1

    unmeasured = _row(150, FRIDAY.isoformat(), 0.60, include_oi=False)
    assert unmeasured["open_interest"] is None
    kept = main_bot.select_chain_shadow(
        [unmeasured],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert kept["shadow"] is not None
    assert kept["shadow"]["open_interest"] is None
    assert kept["shadow"]["open_interest"] != 0


def test_flags_do_not_drop_a_scored_leg_and_same_window_is_logged():
    flagged = main_bot.select_chain_shadow(
        [_row(150, FRIDAY.isoformat(), 0.60, volume=1, timeframe="DELAYED", iv=2.9,
              quote_age_s=1.0)],
        spot=140, vol_annual=0.25, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert flagged["shadow"]["strike"] == 150
    assert flagged["shadow"]["timeframe_ok"] is False
    assert flagged["shadow"]["delayed"] is True
    assert flagged["shadow"]["volume_ok"] is False

    model = main_bot.core_book.strike_for_delta(100.0, 0.20, 3, "CALL")
    assert model is not None
    rows = []
    for step in range(-5, 6):
        rows.append(_row(round(model + step, 2), FRIDAY.isoformat(), 0.40 if step else 0.62))
    rows.append(_row(round(model + 40, 2), FRIDAY.isoformat(), 0.60))
    selected = main_bot.select_chain_shadow(
        rows, spot=100.0, vol_annual=0.20, right="CALL", today=TODAY, now_minutes_et=10 * 60,
    )
    assert selected["shadow"]["strike"] == round(model + 40, 2)
    assert selected["shadow_same_window"]["strike"] == round(model, 2)
    assert selected["shadow"]["strike"] != selected["shadow_same_window"]["strike"]
    live = {"strike": round(model, 2), "expiry": FRIDAY.isoformat(), "right": "CALL",
            "delta": 0.62, "bid": 5.0, "ask": 5.1}
    compared = main_bot.chain_shadow_compare(live, selected)
    assert compared["same_contract"] is False
    assert compared["same_contract_same_window"] is True


def test_strike_band_uses_the_live_spot_not_a_massive_underlying():
    live = {
        "spot": 100.0,
        "vol_annual": 0.20,
        "right": "CALL",
        "underlying_asset": {"price": 500.0},
        "underlying_price": 500.0,
    }
    params = main_bot.chain_shadow_query_params(live, TODAY)
    own = main_bot.chain_shadow_strike_band(100.0, 0.20, "CALL")
    decoy = main_bot.chain_shadow_strike_band(500.0, 0.20, "CALL")
    assert params["strike_price.gte"] == pytest.approx(own[0])
    assert params["strike_price.lte"] == pytest.approx(own[1])
    assert params["strike_price.gte"] != pytest.approx(decoy[0])
    assert params["expiration_date.gte"] == TODAY.isoformat()
    assert params["expiration_date.lte"] == (TODAY + timedelta(days=7)).isoformat()
    assert params["contract_type"] == "call"
    assert params["limit"] == 250
    assert "500" not in json.dumps({k: params[k] for k in (
        "strike_price.gte", "strike_price.lte")})
    assert main_bot.chain_shadow_query_params({"right": "CALL"}, TODAY) is None


def test_pagination_follows_next_url_and_stops_at_three_pages(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    _ChainHttp.calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _ChainHttp)

    async def scenario():
        client = main_bot.massive.MassiveClient()
        client.on_health = None
        return await client.fetch_option_chain(
            "SPY",
            contract_type="call",
            expiration_gte="2026-11-24",
            expiration_lte="2026-12-01",
            strike_gte=90.0,
            strike_lte=110.0,
            limit=250,
            max_pages=10,
        )

    out = _run(scenario())
    assert len(_ChainHttp.calls) == 3
    assert out["pages"] == 3
    url, params = _ChainHttp.calls[0]
    assert "/v3/snapshot/options/SPY" in url
    assert params["expiration_date.gte"] == "2026-11-24"
    assert params["expiration_date.lte"] == "2026-12-01"
    assert params["strike_price.gte"] == 90.0
    assert params["strike_price.lte"] == 110.0
    assert params["contract_type"] == "call"
    assert params["limit"] == 250
    assert params["sort"] == "strike_price"
    assert _ChainHttp.calls[1][1] == {}
    assert _ChainHttp.calls[2][1] == {}


def test_shared_client_is_unchanged_and_band_comes_from_live_spot(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    main_bot._CHAIN_SHADOW_CACHE.clear()
    shared = main_bot.massive.MassiveClient()
    shared.calls = 4
    shared.failures = 1
    shared.last_ok_at = 111.0
    shared.last_error = "seed"
    health = []
    shared.on_health = lambda: health.append("shared")
    before = shared.export_counters()
    seen = {}

    async def fetch(self, underlying, **kwargs):
        seen["kwargs"] = kwargs
        seen["on_health"] = self.on_health
        seen["is_shared"] = self is shared
        return {"results": [_leg(150, FRIDAY.isoformat(), 0.61)], "pages": 1}

    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_chain", fetch)
    saved_client = main_bot.massive_client
    saved_state = main_bot.state
    main_bot.massive_client = shared

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store
        await store.set(main_bot.FEED_HEALTH_KEY, "sentinel-feed")
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            await main_bot._run_chain_shadow("NVDA", {
                "spot": 100.0,
                "vol_annual": 0.20,
                "right": "CALL",
                "strike": 150,
                "expiry": FRIDAY.isoformat(),
                "signal_id": "sig-band",
                "bid": 5.0,
                "ask": 5.1,
                "delta": 0.61,
                "basis": "best",
            })
            assert await store.get(main_bot.FEED_HEALTH_KEY) == "sentinel-feed"
            tail = main_bot.trade_ledger._ledger.recent(20)
        return tail

    try:
        tail = _run(scenario())
    finally:
        main_bot.massive_client = saved_client
        main_bot.state = saved_state
        main_bot._CHAIN_SHADOW_CACHE.clear()

    assert seen["is_shared"] is False
    assert seen["on_health"] is None
    own = main_bot.chain_shadow_strike_band(100.0, 0.20, "CALL")
    decoy = main_bot.chain_shadow_strike_band(500.0, 0.20, "CALL")
    assert seen["kwargs"]["strike_gte"] == pytest.approx(own[0])
    assert seen["kwargs"]["strike_lte"] == pytest.approx(own[1])
    assert seen["kwargs"]["strike_gte"] != pytest.approx(decoy[0])
    assert shared.export_counters() == before
    assert health == []
    assert shared.last_ok_at == 111.0
    assert shared.calls == 4
    picks = [rec for rec in tail if rec.get("event") == "chain_shadow_pick"]
    assert picks
    assert "realized_pnl_pct" not in json.dumps(picks)
    assert "account_pnl_pct" not in json.dumps(picks)


def test_timeout_and_spawn_exception_are_contained(monkeypatch):
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")

    async def hang(ticker, live):
        await asyncio.sleep(5)

    monkeypatch.setattr(main_bot, "_chain_shadow_body", hang)
    monkeypatch.setattr(main_bot, "CHAIN_SHADOW_TIMEOUT_S", 0.05)

    async def scenario():
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            await main_bot._run_chain_shadow("NVDA", {"signal_id": "sig-timeout"})
            errors = [
                rec for rec in main_bot.trade_ledger._ledger.recent(10)
                if rec.get("event") == "chain_shadow_error"
            ]
            assert errors
            assert errors[-1]["data"]["error"] == "timeout"
            assert "realized_pnl_pct" not in errors[-1]["data"]

            async def boom(ticker, live):
                await asyncio.sleep(0)
                raise RuntimeError("chain broke apiKey=should-not-leak")

            monkeypatch.setattr(main_bot, "_chain_shadow_body", boom)
            monkeypatch.setattr(main_bot, "CHAIN_SHADOW_TIMEOUT_S", 8.0)
            await main_bot._run_chain_shadow("NVDA", {"signal_id": "sig-boom"})
            errors = [
                rec for rec in main_bot.trade_ledger._ledger.recent(10)
                if rec.get("event") == "chain_shadow_error"
            ]
            assert errors[-1]["data"]["error"] == "RuntimeError"
            assert "should-not-leak" not in json.dumps(errors[-1]["data"])

    _run(scenario())

    def explode(*args, **kwargs):
        raise RuntimeError("create_task failed")

    monkeypatch.setattr(asyncio, "create_task", explode)
    main_bot._spawn_chain_shadow("NVDA", {"spot": 100, "signal_id": "sig"})


def test_yield_happens_inside_the_timeout_before_the_client(monkeypatch):
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    main_bot._CHAIN_SHADOW_CACHE.clear()
    order = []
    real_sleep = asyncio.sleep

    async def sleep(seconds=0, *args, **kwargs):
        order.append(("sleep", seconds))
        if seconds == 0:
            return None
        return await real_sleep(seconds, *args, **kwargs)

    orig_init = main_bot.massive.MassiveClient.__init__

    def init(self):
        order.append("client")
        orig_init(self)

    async def fetch(self, underlying, **kwargs):
        order.append("fetch")
        assert self.on_health is None
        return {"results": [], "pages": 1}

    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(main_bot.massive.MassiveClient, "__init__", init)
    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_chain", fetch)
    saved_state = main_bot.state

    async def scenario():
        main_bot.state = main_bot.StateStore(None)
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            await main_bot._run_chain_shadow("SPY", {
                "spot": 100, "vol_annual": 0.2, "right": "CALL", "signal_id": "sig-yield",
            })

    try:
        _run(scenario())
    finally:
        main_bot.state = saved_state
        main_bot._CHAIN_SHADOW_CACHE.clear()
    assert order[0] == ("sleep", 0)
    assert order.index("client") > 0
    assert order.index("fetch") > order.index("client")


def _job(signal_id="sig-mark", ask=5.0):
    return {
        "id": f"{signal_id}:O:NVDA",
        "ticker": "NVDA",
        "signal_id": signal_id,
        "occ": "O:NVDA261218C00150000",
        "right": "CALL",
        "expiry": "2026-12-18",
        "strike": 150.0,
        "entry_ask": ask,
        "entry_delta": 0.60,
        "opened_at": "2026-11-24T15:00:00+00:00",
        "closed_at": None,
        "exit_done": False,
        "plus15_done": False,
        "plus30_done": False,
        "mfe_mid": None,
        "mae_mid": None,
    }


def test_worker_marks_exit_15_and_30_and_ignores_exit_followups(monkeypatch):
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")
    quotes = {"mid_bid": 5.2}

    async def fetch_quote(self, *args, **kwargs):
        assert self.on_health is None
        return {
            "live": True, "bid": quotes["mid_bid"], "ask": quotes["mid_bid"] + 0.1,
            "age_s": 1.0, "timeframe": "REAL-TIME", "reason": "ok",
        }

    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_quote", fetch_quote)
    saved_state = main_bot.state
    base = datetime(2026, 11, 24, 16, 0, tzinfo=timezone.utc)

    async def scenario():
        store = main_bot.StateStore(None)
        main_bot.state = store

        def _no_position_lock(*args, **kwargs):
            raise AssertionError("chain shadow took position_lock")

        store.position_lock = _no_position_lock
        await store.queue_push(main_bot.EXIT_FOLLOWUPS_KEY, "sentinel-do-not-touch")
        followups_before = await store.list_all(main_bot.EXIT_FOLLOWUPS_KEY)
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            assert await main_bot._chain_shadow_enqueue(_job()) is True
            monkeypatch.setattr(main_bot, "CHAIN_SHADOW_MAX_OPEN", 1)
            assert await main_bot._chain_shadow_enqueue(_job("sig-extra")) is False
            monkeypatch.setattr(main_bot, "CHAIN_SHADOW_MAX_OPEN", 200)
            await store.set_position("NVDA", {"qty": 1, "contract": {"right": "CALL"}})
            raw, exp = store._memory._kv[main_bot.CHAIN_SHADOW_OPEN_KEY]
            store._memory._kv[main_bot.CHAIN_SHADOW_OPEN_KEY] = (raw, time.time() + 5)
            assert await main_bot._run_chain_shadow_marks(base) == 0
            refreshed = store._memory._kv[main_bot.CHAIN_SHADOW_OPEN_KEY][1]
            assert refreshed > time.time() + 3600
            await store.delete_position("NVDA")
            # Restart resume: the queue is the only memory of the job.
            assert await main_bot._run_chain_shadow_marks(base) == 1
            assert await main_bot._run_chain_shadow_marks(base + timedelta(minutes=15)) == 1
            assert await main_bot._run_chain_shadow_marks(base + timedelta(minutes=30)) == 1
            assert await store.list_read_live(main_bot.CHAIN_SHADOW_OPEN_KEY) == []
            assert await main_bot._run_chain_shadow_marks(base + timedelta(minutes=40)) == 0
            marks = [
                rec["data"] for rec in main_bot.trade_ledger._ledger.recent(20)
                if rec.get("event") == "chain_shadow_mark"
            ]
            assert [m["horizon"] for m in marks] == ["exit", "15", "30"]
            assert all("realized_pnl_pct" not in m and "account_pnl_pct" not in m for m in marks)
            assert marks[0]["measured"] is True
            assert marks[0]["premium_pct"] is not None
            followups_after = await store.list_all(main_bot.EXIT_FOLLOWUPS_KEY)
            assert followups_after == followups_before
            monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "false")
            await main_bot._chain_shadow_enqueue(_job("sig-held"))
            # The kill switch is re-read. A due job stays queued and unmarked.
            assert await main_bot._run_chain_shadow_marks(base + timedelta(minutes=30)) == 0
            held = await store.list_read_live(main_bot.CHAIN_SHADOW_OPEN_KEY)
            assert len(held) == 1
            assert await store.list_all(main_bot.EXIT_FOLLOWUPS_KEY) == followups_before

    try:
        _run(scenario())
    finally:
        main_bot.state = saved_state


def test_worker_counts_a_non_live_quote_as_unmeasured(monkeypatch):
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")

    async def fetch_quote(self, *args, **kwargs):
        return {"live": False, "bid": None, "ask": None, "timeframe": "DELAYED", "reason": "delayed"}

    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_quote", fetch_quote)
    saved_state = main_bot.state

    async def scenario():
        main_bot.state = main_bot.StateStore(None)
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            await main_bot._chain_shadow_enqueue(_job("sig-delayed"))
            now = datetime(2026, 11, 24, 18, 0, tzinfo=timezone.utc)
            assert await main_bot._run_chain_shadow_marks(now) == 1
            mark = main_bot.trade_ledger._ledger.recent(5)[-1]["data"]
            assert mark["measured"] is False
            assert mark["premium_pct"] is None
            assert mark["horizon"] == "exit"

    try:
        _run(scenario())
    finally:
        main_bot.state = saved_state


def test_pro_trader_report_includes_the_chain_shadow_block():
    saved_state = main_bot.state
    saved_ledger = main_bot.trade_ledger._ledger

    async def scenario():
        main_bot.state = main_bot.StateStore(None)
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            await main_bot.trade_ledger.record("chain_shadow_pick", {
                "signal_id": "s-report",
                "same_contract": True,
                "same_contract_same_window": True,
                "delta_gap": 0.0,
                "spread_gap": 0.0,
                "latency_ms": 12,
                "shadow": {"strike": 150},
                "refused": False,
                "reasons": {},
            })
            report = await main_bot._pro_trader_report_payload()
        return report

    try:
        report = _run(scenario())
    finally:
        main_bot.state = saved_state
        main_bot.trade_ledger._ledger = saved_ledger
    block = report["chain_shadow"]
    assert block["n"] == 1
    assert block["live_flip"] is False
    assert block["comparison"]["min_pairs"] == 40
    assert block["comparison"]["min_pairs_is_flip_rule"] is False
    assert report["version"] == "5.99.16-pro-day-trader"


def test_chain_shadow_report_names_the_live_flag(monkeypatch):
    monkeypatch.delenv("CHAIN_SHADOW_ENABLED", raising=False)
    off = main_bot.chain_shadow_report([])
    assert off["enabled_default"] is False
    assert off["enabled"] is False
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true")
    live = main_bot.chain_shadow_report([])
    assert live["enabled"] is True
    assert live["enabled_default"] is False
    monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "0")
    assert main_bot.chain_shadow_report([])["enabled"] is False


def test_report_zero_partial_and_complete_records():
    assert main_bot.chain_shadow_report([])["n"] == 0
    assert main_bot.chain_shadow_report([])["live_flip"] is False
    assert main_bot.chain_shadow_report([])["same_contract_pct"] is None
    assert main_bot.chain_shadow_report([])["comparison"]["min_pairs_is_flip_rule"] is False
    partial = main_bot.chain_shadow_report([{
        "event": "chain_shadow_pick",
        "data": {
            "signal_id": "s1",
            "same_contract": False,
            "same_contract_same_window": True,
            "delta_gap": 0.02,
            "spread_gap": -0.01,
            "latency_ms": 40,
            "shadow": {"strike": 1},
            "refused": False,
            "reasons": {},
        },
    }])
    assert partial["n"] == 1
    assert partial["exit_minus_live_pct"] is None
    assert partial["mean_delta_gap"] == pytest.approx(0.02)
    assert partial["latency_ms"]["p50"] == pytest.approx(40)
    records = [
        {
            "event": "chain_shadow_pick",
            "data": {
                "signal_id": "s1", "same_contract": True, "same_contract_same_window": True,
                "delta_gap": 0.01, "spread_gap": 0.002, "latency_ms": 20,
                "shadow": {"strike": 150}, "refused": False, "reasons": {},
            },
        },
        {
            "event": "chain_shadow_pick",
            "data": {
                "signal_id": "s2", "same_contract": False, "same_contract_same_window": False,
                "delta_gap": None, "spread_gap": None, "latency_ms": 80,
                "shadow": None, "refused": True, "reasons": {"greeks_missing": 4},
            },
        },
        {"event": "position_closed", "data": {"signal_id": "s1", "realized_pnl_pct": 10.0}},
        {"event": "chain_shadow_mark", "data": {
            "signal_id": "s1", "horizon": "exit", "premium_pct": 12.0,
        }},
        {"event": "chain_shadow_mark", "data": {
            "signal_id": "s1", "horizon": "15", "premium_pct": 11.0,
        }},
        {"event": "chain_shadow_mark", "data": {
            "signal_id": "s1", "horizon": "30", "premium_pct": 9.0,
        }},
    ]
    full = main_bot.chain_shadow_report(records)
    assert full["n"] == 2
    assert full["same_contract_pct"] == pytest.approx(50.0)
    assert full["exit_minus_live_pct"] == pytest.approx(2.0)
    assert full["plus15_minus_live_pct"] == pytest.approx(1.0)
    assert full["plus30_minus_live_pct"] == pytest.approx(-1.0)
    assert full["refusal_counts"]["greeks_missing"] == 4
    assert full["refusal_rate"] == pytest.approx(0.5)
    assert full["comparison"]["measured"]["action"] == "report_only"
    assert full["comparison"]["measured"]["floor_met"] is False
    assert full["live_flip"] is False
    dumped = json.dumps(full)
    assert "realized_pnl_pct" not in dumped
    assert "account_pnl_pct" not in dumped
    ci = full["comparison"]["measured"]["exit_premium_pct_minus_live_ci95"]
    assert ci["n"] == 1
    assert ci["mean"] == pytest.approx(2.0)
    assert ci["low"] is None


def _entry_payload(signal_id: str) -> dict:
    return {
        "mode": "live",
        "instrument": "option",
        "ticker": "NVDA",
        "action": "BUY",
        "signal_id": signal_id,
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
        "setup": "vwap_reclaim",
    }


def test_process_signal_hook_order_and_identical_orders(monkeypatch):
    monkeypatch.setattr(main_bot, "LIVE_TRADING", True)
    monkeypatch.setattr(main_bot, "is_sandbox", False)
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "unit-secret")
    monkeypatch.setattr(main_bot, "_is_market_open", lambda: True)
    monkeypatch.setattr(main_bot, "_flatten_window_blocks_entry", lambda *a, **k: False)
    monkeypatch.setenv("MASSIVE_API_KEY", "unit-test-key")
    monkeypatch.setenv("MASSIVE_ENABLED", "true")

    async def filters(p, *, dry_run=False):
        return True, []

    async def full_weight(payload, *args, **kwargs):
        # This test checks the shadow hook, not the under-1-contract skip.
        return {"allowed": True, "weight": 1.0, "notes": [], "reasons": []}

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
            liquidity_out["delta"] = 0.61
            liquidity_out["open_interest"] = 400
            liquidity_out["volume"] = 90
            liquidity_out["bid_size"] = 10
            liquidity_out["ask_size"] = 12
        return ("2026-12-18", 150.0, 4.90, 5.00)

    async def fetch_quote(self, *args, **kwargs):
        if self.on_health is None:
            return {"live": False, "bid": None, "ask": None, "reason": "shadow"}
        return {
            "live": True, "bid": 4.90, "ask": 5.00, "age_s": 1.0,
            "timeframe": "REAL-TIME", "reason": "payoff",
        }

    async def fetch_chain(self, underlying, **kwargs):
        assert self.on_health is None
        assert kwargs["strike_gte"] != pytest.approx(
            main_bot.chain_shadow_strike_band(500.0, 0.2, "CALL")[0]
        )
        return {"results": [], "pages": 1}

    monkeypatch.setattr(main_bot, "_passes_entry_filters", filters)
    monkeypatch.setattr(main_bot, "_assess_portfolio_risk", full_weight)
    monkeypatch.setattr(main_bot, "_resolve_account_id_key", account_key)
    monkeypatch.setattr(main_bot, "_live_equity", equity)
    monkeypatch.setattr(main_bot, "_index_vwap_quotes", quiet)
    monkeypatch.setattr(main_bot, "_observe_series", quiet)
    monkeypatch.setattr(main_bot, "_spawn_guard", lambda ticker: None)
    monkeypatch.setattr(main_bot.alerts, "send", quiet)
    monkeypatch.setattr(main_bot.pyetrade, "ETradeMarket", _Market)
    monkeypatch.setattr(main_bot, "_snap_option_contract", snap)
    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_quote", fetch_quote)
    monkeypatch.setattr(main_bot.massive.MassiveClient, "fetch_option_chain", fetch_chain)
    monkeypatch.setattr(main_bot, "load_tokens", lambda: {
        "oauth_token": "unit-token", "oauth_token_secret": "unit-secret",
    })

    saved = {
        "state": main_bot.state,
        "client": main_bot.massive_client,
        "ledger": main_bot.trade_ledger._ledger,
    }
    placed = []
    opens = []
    arms = []
    order = []
    real_arm = main_bot._arm_stop_guard
    real_open = main_bot._record_open
    real_spawn = main_bot._spawn_chain_shadow

    async def place(kind, common, tokens, unit_cost=None):
        placed.append(json.dumps(
            {k: common[k] for k in sorted(common) if k not in {"clientOrderId", "resp_format"}},
            sort_keys=True, default=str,
        ))
        return {"Order": [{"orderId": "1"}]}, int(common["quantity"]), str(common["clientOrderId"])

    async def arm(*args, **kwargs):
        order.append("arm_enter")
        try:
            await real_arm(*args, **kwargs)
        finally:
            order.append("arm_return")
        arms.append((args[2] if len(args) > 2 else kwargs.get("stop_price"), kwargs.get("contract")))

    async def record_open(*args, **kwargs):
        contract = args[5] if len(args) > 5 else kwargs.get("contract")
        strike = contract.get("strike") if isinstance(contract, dict) else None
        opens.append((args[1], strike, kwargs.get("signal_id")))
        await real_open(*args, **kwargs)

    def spawn(*args, **kwargs):
        order.append("spawn")
        if monkeypatch_raise["on"]:
            raise RuntimeError("spawn exploded")
        return real_spawn(*args, **kwargs)

    monkeypatch_raise = {"on": False}
    monkeypatch.setattr(main_bot, "_place_entry_with_funds_clamp", place)
    monkeypatch.setattr(main_bot, "_arm_stop_guard", arm)
    monkeypatch.setattr(main_bot, "_record_open", record_open)
    monkeypatch.setattr(main_bot, "_spawn_chain_shadow", spawn)
    place_order_calls = []

    async def place_order(*args, **kwargs):
        place_order_calls.append(args)
        raise AssertionError("place_order should stay on the funds-clamp path")

    monkeypatch.setattr(main_bot, "_place_order_smart", place_order)

    async def drive(signal_id, enabled):
        main_bot.portfolio_risk.clear_spy_gap_sit_out()
        main_bot.portfolio_risk.note_spy_gap_session(
            0.4, True, main_bot._session_day_et(),
        )
        monkeypatch.setenv("CHAIN_SHADOW_ENABLED", "true" if enabled else "false")
        store = main_bot.StateStore(None)
        main_bot.state = store
        original_set = store.set_position

        async def set_position(ticker, pos):
            await original_set(ticker, pos)
            if isinstance(pos, dict) and pos.get("stop_premium") is not None:
                order.append("stamp_stop")

        store.set_position = set_position
        shared = main_bot.massive.MassiveClient()
        shared.on_health = lambda: None
        shared.calls = 3
        shared.last_ok_at = 50.0
        before = shared.export_counters()
        main_bot.massive_client = shared
        result = await main_bot.process_signal(_entry_payload(signal_id))
        pending = list(main_bot._CHAIN_SHADOW_TASKS)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        return result, before, shared.export_counters()

    async def scenario():
        with tempfile.TemporaryDirectory() as tmp:
            main_bot.trade_ledger.init(Path(tmp) / "ledger.jsonl")
            monkeypatch_raise["on"] = False
            off, before_off, after_off = await drive("sig-off", False)
            on, before_on, after_on = await drive("sig-on", True)
            monkeypatch_raise["on"] = True
            blown, _, _ = await drive("sig-boom", True)
            events = [rec.get("event") for rec in main_bot.trade_ledger._ledger.recent(50)]
        return off, on, blown, before_off, after_off, before_on, after_on, events

    try:
        off, on, blown, before_off, after_off, before_on, after_on, events = _run(scenario())
    finally:
        main_bot.state = saved["state"]
        main_bot.massive_client = saved["client"]
        main_bot.trade_ledger._ledger = saved["ledger"]
        main_bot._CHAIN_SHADOW_CACHE.clear()
        main_bot.portfolio_risk.clear_spy_gap_sit_out()

    assert off["status"] == "processed_directly", off
    assert off["result"]["status"] == "success", off
    assert on["status"] == "processed_directly", on
    assert on["result"]["status"] == "success", on
    assert blown["status"] == "processed_directly", blown
    assert blown["result"]["status"] == "success", blown
    assert placed[0] == placed[1] == placed[2]
    assert "150" in placed[0]
    assert opens[0][0] == opens[1][0] == opens[2][0]
    assert opens[0][1] == opens[1][1] == 150.0
    assert arms[0][0] == arms[1][0] == arms[2][0]
    assert place_order_calls == []
    assert after_off == before_off
    assert after_on["calls"] == before_on["calls"]
    assert after_on["last_ok_at"] == before_on["last_ok_at"]
    # The enabled run records arm, then the stop stamp, then the spawn.
    assert order.index("arm_return") < order.index("stamp_stop") < order.index("spawn")
    assert "entry_expression_refused" not in events
