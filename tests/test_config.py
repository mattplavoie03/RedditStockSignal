"""Tests for the config module."""

from __future__ import annotations

import os

import pytest

from config import Settings, get_settings


def test_get_settings_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDDIT_CLIENT_ID", "test-id")
    monkeypatch.setenv("REDDIT_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("REDDIT_USER_AGENT", "platform:reddit-signal:v0.1 (by /u/test)")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://reddit_signal:reddit_signal@localhost:5432/reddit_signal",
    )

    settings = get_settings()

    assert settings == Settings(
        reddit_client_id="test-id",
        reddit_client_secret="test-secret",
        reddit_user_agent="platform:reddit-signal:v0.1 (by /u/test)",
        database_url="postgresql+asyncpg://reddit_signal:reddit_signal@localhost:5432/reddit_signal",
    )


def test_get_settings_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "REDDIT_CLIENT_ID",
        "REDDIT_CLIENT_SECRET",
        "REDDIT_USER_AGENT",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(ValueError, match="Missing required environment variable"):
        get_settings()
