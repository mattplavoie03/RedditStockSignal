"""Tests for async database session helpers."""

from __future__ import annotations

from config import Settings
from db.session import create_engine, create_session_factory


def test_create_engine_uses_database_url() -> None:
    settings = Settings(
        reddit_client_id="id",
        reddit_client_secret="secret",
        reddit_user_agent="ua",
        database_url="postgresql+asyncpg://user:pass@localhost:5432/db",
    )
    engine = create_engine(settings)
    assert str(engine.url) == "postgresql+asyncpg://user:***@localhost:5432/db"
    engine.sync_engine.dispose()


def test_create_session_factory() -> None:
    settings = Settings(
        reddit_client_id="id",
        reddit_client_secret="secret",
        reddit_user_agent="ua",
        database_url="postgresql+asyncpg://user:pass@localhost:5432/db",
    )
    engine = create_engine(settings)
    factory = create_session_factory(engine)
    assert factory.kw["expire_on_commit"] is False
    engine.sync_engine.dispose()
