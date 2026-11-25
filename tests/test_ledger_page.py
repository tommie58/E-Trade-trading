"""Read-only paging for GET /ledger.

The ledger on disk is append-only JSONL. These tests use the fake key
TESTACCTKEY123 and never a live account. They do not append, trim, or migrate.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
import pytest

import main_bot
import trade_ledger

FAKE_KEY = "TESTACCTKEY123"
AUTH = {"X-Rork-Secret": "s3cret"}
CT = ZoneInfo("America/Chicago")


@pytest.fixture(autouse=True)
def _ledger_env(monkeypatch):
    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", "s3cret")
    saved = {
        "ledger": trade_ledger._ledger,
        "memory": main_bot._ledger_memory,
        "key": main_bot._resolved_account_id_key,
        "crypto": main_bot._resolved_crypto_account_id_key,
    }
    main_bot._ledger_memory = None
    main_bot._resolved_account_id_key = None
    main_bot._resolved_crypto_account_id_key = None
    main_bot._reset_ledger_rate_limit()
    yield
    trade_ledger._ledger = saved["ledger"]
    main_bot._ledger_memory = saved["memory"]
    main_bot._resolved_account_id_key = saved["key"]
    main_bot._resolved_crypto_account_id_key = saved["crypto"]
    main_bot._reset_ledger_rate_limit()


class _AppClient:
    """HTTP against the ASGI app without the trading startup lifespan."""

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        transport = httpx.ASGITransport(app=main_bot.app, raise_app_exceptions=False)
        self._client = httpx.AsyncClient(transport=transport, base_url="http://ledger.test")
        self._loop.run_until_complete(self._client.__aenter__())

    def get(self, url, **kwargs):
        return self._loop.run_until_complete(self._client.get(url, **kwargs))

    def post(self, url, **kwargs):
        return self._loop.run_until_complete(self._client.post(url, **kwargs))

    def close(self):
        self._loop.run_until_complete(self._client.aclose())
        self._loop.close()


@pytest.fixture
def http():
    client = _AppClient()
    try:
        yield client
    finally:
        client.close()


def _record(seq: int, event: str, ts: str, **extra) -> dict:
    data = {"n": seq}
    data.update(extra.pop("data", {}))
    rec = {
        "seq": seq,
        "ts": ts,
        "event": event,
        "data": data,
        "prev_hash": "ab" * 32,
        "hash": "cd" * 32,
    }
    rec.update(extra)
    return rec


def _write(path, records, *, trailing_newline=True, extra_lines=()):
    lines = [json.dumps(rec, separators=(",", ":")) for rec in records]
    lines.extend(extra_lines)
    text = "\n".join(lines)
    if trailing_newline:
        text += "\n"
    path.write_text(text, encoding="utf-8")
    trade_ledger.init(path)
    return path


def _day(seq: int) -> str:
    day = date(2026, 10, 1) + timedelta(days=(seq - 1) % 61)
    return datetime(day.year, day.month, day.day, 18, 0, tzinfo=timezone.utc).isoformat()


def _events_for(n: int, poisoned_every: int = 0):
    names = ("position_opened", "stop_updated", "position_closed", "heartbeat")
    records = []
    for seq in range(1, n + 1):
        extra = {}
        data = {}
        if poisoned_every and seq % poisoned_every == 0:
            extra["accountId"] = FAKE_KEY
            extra["accountIdKey"] = FAKE_KEY
            data["note"] = (
                f"failed /v1/accounts/{FAKE_KEY}/orders?accountIdKey={FAKE_KEY} {FAKE_KEY}"
            )
            data["accountId"] = FAKE_KEY
            data["nested"] = {"accountIdKey": FAKE_KEY, "ok": True}
        records.append(_record(seq, names[(seq - 1) % 4], _day(seq), data=data, **extra))
    return records


def _get(http, query="", **kwargs):
    return http.get("/ledger" + query, headers=AUTH, **kwargs)


def _walk(http, base: dict, cursor_param: str, cursor_field: str):
    """Follow one exclusive cursor until has_more is false."""
    params = dict(base)
    seen = []
    body = None
    for _ in range(50):
        response = http.get("/ledger", params=params, headers=AUTH)
        assert response.status_code == 200, response.text
        body = response.json()
        assert FAKE_KEY not in response.text
        seen.extend(rec["seq"] for rec in body["records"])
        if not body["has_more"]:
            return seen, body
        cursor = body[cursor_field]
        assert isinstance(cursor, int)
        params[cursor_param] = cursor
    raise AssertionError("paging did not finish")


def test_legacy_tail_matches_trade_ledger_tail_without_paging_keys(tmp_path, http):
    records = [_record(seq, "heartbeat", _day(seq)) for seq in range(1, 121)]
    path = _write(
        tmp_path / "ledger.jsonl",
        records,
        extra_lines=("", "{not-json", ""),
        trailing_newline=False,
    )
    direct = main_bot._legacy_tail_records(100)
    assert direct == trade_ledger.tail(100)
    assert main_bot._legacy_tail_records(7) == trade_ledger.tail(7)

    default = _get(http)
    assert default.status_code == 200
    body = default.json()
    assert set(body) == {"records"}
    assert body["records"] == trade_ledger.tail(100)
    assert body["records"][0]["seq"] == 120
    assert "has_more" not in body
    assert "next_before_seq" not in body
    assert "next_since_seq" not in body

    counted = _get(http, "?count=7&verify=true")
    assert counted.status_code == 200
    counted_body = counted.json()
    assert counted_body["records"] == trade_ledger.tail(7)
    ok, checked, err = trade_ledger.verify()
    assert counted_body["chain_ok"] is ok
    assert counted_body["chain_records_checked"] == checked
    assert counted_body["chain_error"] == err
    assert set(counted_body) == {
        "records", "chain_ok", "chain_records_checked", "chain_error",
    }

    memory = {"status": "healthy", "records": 120, "message": "ledger memory intact"}
    main_bot._ledger_memory = memory
    with_memory = _get(http, "?count=1").json()
    assert set(with_memory) == {"records", "memory"}
    assert with_memory["records"] == trade_ledger.tail(1)
    assert with_memory["memory"] == memory
    assert path.read_bytes()  # file still there; bytes checked in the write test


def test_paging_round_trip_over_2500_records_has_no_gaps(tmp_path, http):
    _write(tmp_path / "ledger.jsonl", _events_for(2500, poisoned_every=50))

    newest_first, last = _walk(http, {"limit": 500}, "before_seq", "next_before_seq")
    assert newest_first == list(range(2500, 0, -1))
    assert last["has_more"] is False

    oldest_first, last_asc = _walk(
        http, {"since_seq": 0, "limit": 500}, "since_seq", "next_since_seq",
    )
    assert oldest_first == list(range(1, 2501))
    assert last_asc["has_more"] is False
    assert last_asc["records"][0]["seq"] == 2001

    main_bot._reset_ledger_rate_limit()
    filtered, _ = _walk(
        http,
        {"event_type": "position_opened,stop_updated", "limit": 300},
        "before_seq",
        "next_before_seq",
    )
    assert filtered == [seq for seq in range(2500, 0, -1) if (seq - 1) % 4 in (0, 1)]
    assert len(filtered) == 1250
    assert len(set(filtered)) == len(filtered)


def test_event_type_dates_and_cursors(tmp_path, http):
    inside_early = "2026-11-11T06:00:00+00:00"
    inside_late = "2026-11-19T05:59:59+00:00"
    before_window = "2026-11-11T05:59:59+00:00"
    after_window = "2026-11-19T06:00:00+00:00"
    records = [
        _record(1, "position_opened", before_window),
        _record(2, "position_opened", inside_early),
        _record(3, "stop_updated", "2026-11-12T18:00:00+00:00"),
        _record(4, "position_opened", "2026-11-15T18:00:00+00:00"),
        _record(5, "position_closed", "2026-11-16T18:00:00+00:00"),
        _record(6, "position_opened", inside_late),
        _record(7, "position_opened", after_window),
    ]
    _write(tmp_path / "ledger.jsonl", records)

    first = _get(
        http,
        "?event_type=position_opened&start=2026-11-11&end=2026-11-18&limit=2",
    )
    assert first.status_code == 200, first.text
    body = first.json()
    assert [rec["seq"] for rec in body["records"]] == [6, 4]
    assert body["has_more"] is True
    assert body["next_before_seq"] == 4
    assert body["next_since_seq"] == 6

    second = _get(
        http,
        "?event_type=position_opened&start=2026-11-11&end=2026-11-18&limit=2"
        f"&before_seq={body['next_before_seq']}",
    )
    assert [rec["seq"] for rec in second.json()["records"]] == [2]
    assert second.json()["has_more"] is False

    spanned = _get(http, "?event_type=position_opened,%20stop_updated&start=2026-11-11&end=2026-11-18")
    assert [rec["seq"] for rec in spanned.json()["records"]] == [6, 4, 3, 2]

    window = _get(http, "?since_seq=2&before_seq=6&limit=10")
    assert window.status_code == 200
    assert [rec["seq"] for rec in window.json()["records"]] == [3, 4, 5]
    assert window.json()["has_more"] is False

    older = _get(http, "?before_seq=3&limit=10")
    assert [rec["seq"] for rec in older.json()["records"]] == [2, 1]


def test_ct_date_bounds_are_chicago_calendar_days():
    start = main_bot._parse_ledger_ct_bound("start", "2026-11-11", is_end=False)
    end = main_bot._parse_ledger_ct_bound("end", "2026-11-18", is_end=True)
    assert start == datetime(2026, 11, 11, tzinfo=CT).astimezone(timezone.utc)
    assert end == datetime(2026, 11, 19, tzinfo=CT).astimezone(timezone.utc)
    naive = main_bot._parse_ledger_ct_bound("start", "2026-11-11T00:00:00", is_end=False)
    assert naive == start


def test_limit_is_capped_and_legacy_count_stays(tmp_path, http):
    _write(tmp_path / "ledger.jsonl", [_record(seq, "heartbeat", _day(seq)) for seq in range(1, 601)])

    assert len(_get(http).json()["records"]) == 100
    assert _get(http, "?count=501").status_code == 422

    page = _get(http, "?limit=1000")
    assert page.status_code == 200
    assert len(page.json()["records"]) == 600
    assert page.json()["has_more"] is False

    default_page = _get(http, "?since_seq=0")
    assert len(default_page.json()["records"]) == 500
    assert default_page.json()["records"][0]["seq"] == 1
    assert default_page.json()["records"][-1]["seq"] == 500
    assert default_page.json()["has_more"] is True

    both = _get(http, "?count=3&limit=2")
    assert [rec["seq"] for rec in both.json()["records"]] == [601 - 1, 599]
    assert len(both.json()["records"]) == 2

    for query in (
        "?limit=0",
        "?limit=1001",
        "?limit=-1",
        "?limit=abc",
        "?limit=1.5",
        "?since_seq=-1",
        "?since_seq=nope",
        "?since_seq=1.2",
        "?before_seq=-3",
        "?since_seq=10&before_seq=10",
        "?since_seq=10&before_seq=4",
        "?event_type=",
        "?event_type=position_opened,",
        "?event_type=bad name",
        "?start=2026-13-01",
        "?start=not-a-date",
        "?end=nope",
        "?start=2026-11-20&end=2026-11-11",
    ):
        response = _get(http, query)
        assert response.status_code == 400, query
        assert FAKE_KEY not in response.text


def test_scan_cap_still_pages_without_reading_the_whole_file(tmp_path, http, monkeypatch):
    monkeypatch.setattr(main_bot, "LEDGER_PAGE_SCAN_CAP", 25)
    _write(tmp_path / "ledger.jsonl", [_record(seq, "heartbeat", _day(seq)) for seq in range(1, 81)])
    calls = {"n": 0}
    real = main_bot._line_at

    def counted(handle, pos, size):
        calls["n"] += 1
        return real(handle, pos, size)

    monkeypatch.setattr(main_bot, "_line_at", counted)
    first = _get(http, "?limit=100")
    assert first.status_code == 200
    body = first.json()
    assert [rec["seq"] for rec in body["records"]] == list(range(80, 55, -1))
    assert body["has_more"] is True
    assert calls["n"] < 50

    seen, _last = _walk(http, {"limit": 100}, "before_seq", "next_before_seq")
    assert seen == list(range(80, 0, -1))


def test_auth_is_fail_closed_and_get_only(tmp_path, http, monkeypatch):
    _write(tmp_path / "ledger.jsonl", [_record(1, "heartbeat", _day(1))])

    missing = http.get("/ledger")
    wrong = http.get("/ledger", headers={"X-Rork-Secret": "nope"})
    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert FAKE_KEY not in missing.text
    assert FAKE_KEY not in wrong.text

    posted = http.post("/ledger", headers=AUTH)
    assert posted.status_code == 405
    assert http.post("/ledger").status_code == 401

    monkeypatch.setattr(main_bot, "WEBHOOK_SECRET", None)
    closed = http.get("/ledger", headers=AUTH)
    assert closed.status_code == 401
    assert http.get("/ledger").status_code == 401


def test_rate_limit_returns_429_after_thirty(tmp_path, http, monkeypatch):
    _write(tmp_path / "ledger.jsonl", [_record(1, "heartbeat", _day(1), accountId=FAKE_KEY)])
    codes = [_get(http, "?limit=1").status_code for _ in range(LEDGER_BUDGET)]
    assert codes == [200] * LEDGER_BUDGET
    blocked = _get(http, "?limit=1")
    assert blocked.status_code == 429
    assert blocked.json()["detail"] == "ledger rate limit exceeded"
    assert FAKE_KEY not in blocked.text

    # Unauthenticated calls never reach the limiter, so they do not consume it.
    main_bot._reset_ledger_rate_limit()
    for _ in range(LEDGER_BUDGET + 5):
        assert http.get("/ledger").status_code == 401
    assert _get(http, "?limit=1").status_code == 200

    now = {"t": 1_000.0}

    def clock():
        return now["t"]

    monkeypatch.setattr(main_bot.time, "monotonic", clock)
    main_bot._reset_ledger_rate_limit()
    assert _get(http).status_code == 200
    for _ in range(LEDGER_BUDGET - 1):
        assert _get(http).status_code == 200
    assert _get(http).status_code == 429
    now["t"] += main_bot.LEDGER_RATE_WINDOW_SECONDS + 1
    assert _get(http).status_code == 200


# The name is bound after import of main_bot so the test above can read it.
LEDGER_BUDGET = main_bot.LEDGER_RATE_LIMIT


def test_responses_and_logs_strip_the_account_key(tmp_path, http, monkeypatch, caplog):
    note = f"boom /v1/accounts/{FAKE_KEY}/orders?accountIdKey={FAKE_KEY} {FAKE_KEY}"
    _write(tmp_path / "ledger.jsonl", [
        _record(
            1,
            "position_opened",
            "2026-11-12T18:00:00+00:00",
            accountId=FAKE_KEY,
            accountIdKey=FAKE_KEY,
            data={
                "note": note,
                "accountId": FAKE_KEY,
                "nested": {"accountIdKey": FAKE_KEY, "ok": True},
            },
        ),
    ])
    main_bot._resolved_account_id_key = FAKE_KEY
    caplog.set_level(logging.DEBUG)

    default = _get(http)
    paged = _get(http, "?limit=10")
    for response in (default, paged):
        assert response.status_code == 200, response.text
        assert FAKE_KEY not in response.text
        record = response.json()["records"][0]
        assert "accountId" not in record
        assert "accountIdKey" not in record
        assert "accountId" not in record["data"]
        assert record["data"]["nested"] == {"ok": True}
        assert record["data"]["note"] == (
            "boom /v1/accounts/{acct}/orders?accountIdKey={acct} {acct}"
        )
        assert record["seq"] == 1
        assert record["event"] == "position_opened"

    def explode(_query):
        raise OSError(
            f"read /v1/accounts/{FAKE_KEY}/orders accountIdKey={FAKE_KEY} {FAKE_KEY}"
        )

    monkeypatch.setattr(main_bot, "_read_ledger_page", explode)
    with caplog.at_level(logging.DEBUG, logger="etrade-bot"):
        failed = _get(http, "?limit=1")
    assert failed.status_code == 500
    assert FAKE_KEY not in failed.text
    assert "/v1/accounts/{acct}/" in failed.text
    assert FAKE_KEY not in caplog.text
    assert "accountIdKey={acct}" in caplog.text


def test_ledger_read_does_not_write(tmp_path, http, monkeypatch):
    path = _write(tmp_path / "ledger.jsonl", _events_for(12))
    before = path.read_bytes()
    writes = []

    def refuse_append(self, line):
        writes.append(("append", line))
        raise AssertionError("append")

    async def refuse_record(*args, **kwargs):
        writes.append(("record", args))
        raise AssertionError("record")

    real_open = open

    def spy_open(file, mode="r", *args, **kwargs):
        if str(file) == str(path) and any(flag in str(mode) for flag in ("w", "a", "x", "+")):
            writes.append(("open", mode))
            raise AssertionError(mode)
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(trade_ledger.TradeLedger, "_append_line", refuse_append)
    monkeypatch.setattr(trade_ledger, "record", refuse_record)
    monkeypatch.setattr("builtins.open", spy_open)

    assert _get(http).status_code == 200
    assert _get(http, "?limit=4&event_type=position_opened").status_code == 200
    assert _get(http, "?since_seq=3&verify=true").status_code == 200
    assert writes == []
    assert path.read_bytes() == before
