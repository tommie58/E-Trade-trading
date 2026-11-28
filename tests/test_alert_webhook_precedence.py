"""Env webhook precedence and the error delivery floor.

The webhook strings below are fixtures. They are not real endpoints, and
the tests assert they never appear in logs or in GET /alerts/config.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time

import pytest

import main_bot


# Obviously fake. Not a configured receiver. The path tail is the token
# fragment the boot log must not keep.
TOKEN_FRAGMENT = "tokfixture"
ENV_URL = "https://example.invalid/hook/" + TOKEN_FRAGMENT
SAVED_URL = "https://example.invalid/saved/" + TOKEN_FRAGMENT


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _restore_alert_manager():
    saved = main_bot.alerts._manager
    yield
    main_bot.alerts._manager = saved


class _Response:
    status_code = 204


class _Client:
    posts: list

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):
        severity = json.get("severity") if isinstance(json, dict) else None
        _Client.posts.append({"url": url, "severity": severity})
        return _Response()


def _install_http(monkeypatch):
    import httpx

    _Client.posts = []
    monkeypatch.setattr(httpx, "AsyncClient", _Client)


async def _boot(env, saved, min_severity="critical"):
    store = main_bot.StateStore(None)
    if saved is not None:
        body = {"webhook_url": saved, "min_severity": min_severity}
        await store.set(
            main_bot.alerts.ALERTS_CONFIG_KEY,
            json.dumps(body),
        )
    main_bot.alerts.init(store, env, dedupe_seconds=300)
    await main_bot.alerts.load_persisted()
    return store


def _assert_url_absent(text: str):
    assert ENV_URL not in text
    assert SAVED_URL not in text
    assert TOKEN_FRAGMENT not in text


def test_send_keeps_dedupe_seconds_parameter():
    for fn in (main_bot.alerts.AlertManager.send, main_bot.alerts.send):
        params = inspect.signature(fn).parameters
        assert "dedupe_seconds" in params
        assert params["dedupe_seconds"].kind is inspect.Parameter.KEYWORD_ONLY or (
            params["dedupe_seconds"].default is None
        )


def test_env_beats_a_different_saved_url(caplog):
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")

    async def scenario():
        await _boot(ENV_URL, SAVED_URL, min_severity="warning")
        return await main_bot.alerts_config()

    cfg = _run(scenario())
    assert cfg["webhook_source"] == "env"
    assert cfg["webhook_configured"] is True
    assert cfg["effective_min_severity"] == "warning"
    assert cfg["webhook_url_masked"] != ENV_URL
    assert cfg["webhook_url_masked"] != SAVED_URL
    _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)
    assert "webhook_source=env" in caplog.text
    assert "effective_min_severity=warning" in caplog.text


def test_empty_saved_url_does_not_clear_env(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")
    _install_http(monkeypatch)

    async def scenario():
        await _boot(ENV_URL, "", min_severity="critical")
        await main_bot.alerts.send(
            "error", "etrade_http_error", "read failed",
            dedupe_key="etrade:GET /quotes", dedupe_seconds=900,
        )
        return await main_bot.alerts_config()

    cfg = _run(scenario())
    assert cfg["webhook_source"] == "env"
    assert cfg["effective_min_severity"] == "error"
    assert [row["url"] for row in _Client.posts] == [ENV_URL]
    _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)


def test_saved_url_used_only_when_env_is_empty(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")
    _install_http(monkeypatch)

    async def scenario():
        seen = []
        for env in (None, "", "   "):
            caplog.clear()
            _Client.posts = []
            await _boot(env, SAVED_URL, min_severity="warning")
            cfg = await main_bot.alerts.get_config()
            await main_bot.alerts.send("error", "etrade_http_error", "read failed")
            seen.append((cfg, list(_Client.posts)))
        return seen

    seen = _run(scenario())
    assert len(seen) == 3
    for cfg, posts in seen:
        assert cfg["webhook_source"] == "persisted"
        assert cfg["webhook_configured"] is True
        assert cfg["effective_min_severity"] == "warning"
        assert [row["url"] for row in posts] == [SAVED_URL]
        _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)
    assert "webhook_source=persisted" in caplog.text


def test_both_empty_is_source_none(caplog):
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")

    async def scenario():
        await _boot(None, None)
        return await main_bot.alerts_config()

    cfg = _run(scenario())
    assert cfg["webhook_source"] == "none"
    assert cfg["webhook_configured"] is False
    assert cfg["webhook_url_masked"] is None
    assert cfg["effective_min_severity"] == "warning"
    assert "note" not in cfg
    _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)
    assert "webhook_source=none" in caplog.text
    assert "effective_min_severity=warning" in caplog.text


def test_critical_floor_still_delivers_error_once(caplog, monkeypatch):
    """A saved critical floor delivers error (and critical) once per key."""
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")
    _install_http(monkeypatch)

    async def scenario():
        store = await _boot(ENV_URL, SAVED_URL, min_severity="critical")
        await main_bot.alerts.send(
            "warning", "etrade_http_error", "below the floor",
            dedupe_key="etrade:GET /below", dedupe_seconds=900,
        )
        await main_bot.alerts.send(
            "error", "etrade_http_error", "read failed",
            dedupe_key="etrade:GET /quotes", dedupe_seconds=900,
        )
        await main_bot.alerts.send(
            "error", "etrade_http_error", "read failed again",
            dedupe_key="etrade:GET /quotes", dedupe_seconds=900,
        )
        await main_bot.alerts.send(
            "critical", "kill_switch", "halted",
            dedupe_key="kill-once", dedupe_seconds=900,
        )
        cfg = await main_bot.alerts_config()
        ttl = store._memory._kv["alert:dedupe:etrade:GET /quotes"][1]
        return cfg, ttl

    cfg, ttl = _run(scenario())
    assert cfg["min_severity"] == "critical"
    assert cfg["effective_min_severity"] == "error"
    assert cfg["webhook_source"] == "env"
    assert "does not override" in cfg["note"]
    assert [row["severity"] for row in _Client.posts] == ["error", "critical"]
    assert [row["url"] for row in _Client.posts] == [ENV_URL, ENV_URL]
    # The passed window is 15 minutes, not the manager default of 300s.
    remaining = ttl - time.time()
    assert 800 < remaining < 950
    _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)
    assert "webhook_source=env" in caplog.text
    assert "effective_min_severity=error" in caplog.text
    assert "[ALERT][dup suppressed]" in caplog.text


def test_saved_url_does_not_override_env_on_update(caplog, monkeypatch):
    """POST /alerts/config stores the saved URL and leaves a set env URL in effect."""
    caplog.set_level(logging.DEBUG, logger="etrade-bot.alerts")
    _install_http(monkeypatch)

    async def scenario():
        store = await _boot(ENV_URL, "", min_severity="warning")
        updated = await main_bot.alerts.set_config(SAVED_URL, "critical")
        stored = json.loads(await store.get(main_bot.alerts.ALERTS_CONFIG_KEY))
        # Env unset on the next boot: the URL saved while env was set is used.
        main_bot.alerts.init(store, None, dedupe_seconds=300)
        await main_bot.alerts.load_persisted()
        await main_bot.alerts.send("error", "etrade_http_error", "from saved")
        from_saved = await main_bot.alerts.get_config()
        saved_posts = list(_Client.posts)
        # Env set again: the same saved URL does not take over, and clearing
        # the saved value does not clear the env URL.
        _Client.posts = []
        main_bot.alerts.init(store, ENV_URL, dedupe_seconds=300)
        await main_bot.alerts.load_persisted()
        await main_bot.alerts.send("error", "etrade_http_error", "from env")
        still_env = await main_bot.alerts.get_config()
        env_posts = list(_Client.posts)
        cleared = await main_bot.alerts.set_config("", None)
        _Client.posts = []
        await main_bot.alerts.send("critical", "kill_switch", "still env")
        after_clear = list(_Client.posts)
        return (
            updated, stored, from_saved, saved_posts,
            still_env, env_posts, cleared, after_clear,
        )

    (
        updated, stored, from_saved, saved_posts,
        still_env, env_posts, cleared, after_clear,
    ) = _run(scenario())
    assert updated["webhook_source"] == "env"
    assert updated["effective_min_severity"] == "error"
    assert "does not override" in updated["note"]
    assert stored["webhook_url"] == SAVED_URL
    assert stored["min_severity"] == "critical"
    assert from_saved["webhook_source"] == "persisted"
    assert [row["url"] for row in saved_posts] == [SAVED_URL]
    assert still_env["webhook_source"] == "env"
    assert [row["url"] for row in env_posts] == [ENV_URL]
    assert cleared["webhook_source"] == "env"
    assert cleared["webhook_configured"] is True
    assert [row["url"] for row in after_clear] == [ENV_URL]
    for cfg in (updated, from_saved, still_env, cleared):
        _assert_url_absent(json.dumps(cfg))
    _assert_url_absent(caplog.text)


def test_saved_url_survives_a_severity_only_update():
    async def scenario():
        store = await _boot(None, SAVED_URL, min_severity="warning")
        cfg = await main_bot.alerts.set_config(None, "error")
        stored = json.loads(await store.get(main_bot.alerts.ALERTS_CONFIG_KEY))
        return cfg, stored

    cfg, stored = _run(scenario())
    assert cfg["webhook_source"] == "persisted"
    assert cfg["effective_min_severity"] == "error"
    assert stored["webhook_url"] == SAVED_URL
    assert stored["min_severity"] == "error"
    _assert_url_absent(json.dumps(cfg))


def test_webhook_source_log_is_scrubber_output(caplog, monkeypatch):
    """Source lines are _scrub_alert_text output. The new mask hides the token."""
    caplog.set_level(logging.DEBUG)
    real_scrub = main_bot.alerts._scrub_alert_text
    real_mask = main_bot.alerts.mask_webhook_url
    produced = []
    masked = []

    def _recording(message):
        cleaned = real_scrub(message)
        produced.append(cleaned)
        return cleaned

    def _recording_mask(url):
        hidden = real_mask(url)
        masked.append(hidden)
        return hidden

    monkeypatch.setattr(main_bot.alerts, "_scrub_alert_text", _recording)
    monkeypatch.setattr(main_bot.alerts, "mask_webhook_url", _recording_mask)

    async def scenario():
        await _boot(ENV_URL, SAVED_URL, min_severity="critical")
        leaked = "webhook_source=env " + ENV_URL + " token " + TOKEN_FRAGMENT
        safe = main_bot.alerts._scrub_webhook_source_line(leaked, [ENV_URL, SAVED_URL])
        main_bot.alerts.logger.info("%s", safe)
        main_bot.logger.info(
            "%s", main_bot.alerts._manager.webhook_source_log_line("update"),
        )
        return await main_bot.alerts_config()

    cfg = _run(scenario())

    assert produced, "webhook source line did not reach _scrub_alert_text"
    assert masked, "mask_webhook_url was not used"
    source_logs = [
        rec.getMessage() for rec in caplog.records if "webhook_source=" in rec.getMessage()
    ]
    assert source_logs
    assert set(source_logs).issubset(set(produced))
    _assert_url_absent(caplog.text)
    _assert_url_absent(json.dumps(cfg))
    for line in produced:
        _assert_url_absent(line)
    for hidden in masked:
        assert hidden is None or TOKEN_FRAGMENT not in hidden
        assert hidden is None or ENV_URL not in hidden
        assert hidden is None or SAVED_URL not in hidden
    # New mask is scheme + host + ellipsis + a 4-character tail, not the path token.
    assert cfg["webhook_url_masked"].startswith("https://example.invalid/")
    assert cfg["webhook_url_masked"].endswith("…ture")
    assert "webhook_source=env" in caplog.text
    assert "effective_min_severity=error" in caplog.text


def test_send_nowait_uses_the_effective_error_floor(monkeypatch):
    """A saved critical floor does not drop error alerts on the nowait path."""
    _install_http(monkeypatch)

    async def scenario():
        await _boot(ENV_URL, SAVED_URL, min_severity="critical")
        manager = main_bot.alerts._manager
        assert manager.effective_min_severity == "error"
        await main_bot.alerts.send_nowait("warning", "routine_notice", "below")
        await main_bot.alerts.send_nowait("error", "etrade_http_error", "read failed")
        await main_bot.alerts.send_nowait("critical", "feed_down", "halted")
        pending = list(main_bot.alerts._delivery_tasks)
        if pending:
            await asyncio.gather(*pending)
        return [row["severity"] for row in _Client.posts]

    assert _run(scenario()) == ["error", "critical"]


def test_failure_text_logs_the_exception_type_only(caplog):
    caplog.set_level(logging.DEBUG)
    header = "Authorization: Bearer-fixture"

    class _Boom(Exception):
        pass

    async def scenario():
        await _boot(ENV_URL, SAVED_URL, min_severity="critical")
        exc = _Boom(ENV_URL + " " + header + " " + TOKEN_FRAGMENT)
        text = main_bot.alerts._manager._failure_text(exc)
        main_bot.alerts.logger.warning("alert config load failed: %s", text)
        return text

    text = _run(scenario())
    assert text == "_Boom"
    assert header not in caplog.text
    assert "Bearer-fixture" not in caplog.text
    _assert_url_absent(caplog.text)
