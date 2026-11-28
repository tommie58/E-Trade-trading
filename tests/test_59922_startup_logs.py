"""Startup logs say whether Postgres and Redis are configured.

They do not print the pooler user or the Redis host. A redis-py
connection error keeps the exception type only.
"""
from __future__ import annotations

import asyncio
import logging

import main_bot


def _run(coro):
    return asyncio.run(coro)


def test_redis_startup_log_omits_the_host(monkeypatch, caplog):
    url = "redis://pooleruser:s3cret-redis@redis.example.internal:6379/0"

    class _Client:
        async def ping(self):
            return True

    import redis.asyncio as redis_asyncio

    monkeypatch.setattr(redis_asyncio, "from_url", lambda *args, **kwargs: _Client())
    with caplog.at_level(logging.INFO):
        store = _run(main_bot.StateStore.create(url))
    assert store.redis is not None
    assert "redis: configured" in caplog.text
    assert "redis.example.internal" not in caplog.text
    assert "pooleruser" not in caplog.text
    assert "s3cret-redis" not in caplog.text


def test_redis_unreachable_log_is_the_exception_type(monkeypatch, caplog):
    url = "redis://pooleruser:s3cret-redis@redis.example.internal:6379/0"

    def boom(*args, **kwargs):
        raise ConnectionError("Error 111 connecting to redis.example.internal:6379")

    import redis.asyncio as redis_asyncio

    monkeypatch.setattr(redis_asyncio, "from_url", boom)
    with caplog.at_level(logging.ERROR):
        store = _run(main_bot.StateStore.create(url))
    assert store.redis is None
    assert "ConnectionError" in caplog.text
    assert "redis.example.internal" not in caplog.text
    assert "s3cret-redis" not in caplog.text


def test_redis_library_connection_log_drops_the_host(caplog):
    main_bot._install_etrade_log_redactor()
    library = logging.getLogger("redis.connection")
    with caplog.at_level(logging.ERROR, logger="redis.connection"):
        library.error(
            "Error 111 connecting to redis.example.internal:6379. ConnectionError.",
        )
    assert "redis.example.internal" not in caplog.text
    assert "ConnectionError" in caplog.text


def test_postgres_startup_log_omits_the_pooler_user(monkeypatch, caplog):
    url = (
        "postgresql://postgres.projref123:s3cret-pg@"
        "aws-0-us-east-1.pooler.supabase.com:6543/postgres"
    )
    saved = {
        "url": main_bot.DATABASE_URL,
        "engine": main_bot.engine,
        "session": main_bot.async_session,
        "backend": main_bot._db_backend,
        "error": main_bot._db_error,
    }

    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, *args, **kwargs):
            return None

        async def commit(self):
            return None

        async def run_sync(self, fn):
            return None

    class _Engine:
        def connect(self):
            return _Conn()

        def begin(self):
            return _Conn()

        async def dispose(self):
            return None

    monkeypatch.setattr(main_bot, "DATABASE_URL", url)
    monkeypatch.setattr(main_bot, "create_async_engine", lambda *args, **kwargs: _Engine())
    try:
        with caplog.at_level(logging.INFO):
            _run(main_bot.init_db())
    finally:
        main_bot.DATABASE_URL = saved["url"]
        main_bot.engine = saved["engine"]
        main_bot.async_session = saved["session"]
        main_bot._db_backend = saved["backend"]
        main_bot._db_error = saved["error"]
    assert "postgres: configured" in caplog.text
    assert "postgres.projref123" not in caplog.text
    assert "s3cret-pg" not in caplog.text
    assert "pooler.supabase.com" not in caplog.text
