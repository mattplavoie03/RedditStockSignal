"""Tests for WSB daily rollover and draining behavior."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from ingest.constants import WSB_DAILY_MAX_TRUNCATION_STREAK, wsb_daily_cursor_key, wsb_daily_truncation_streak_key
from ingest.wsb_daily import WsbDailyState, _handle_thread_rollover, _threads_to_poll, _update_cursor_for_thread

THREAD_ID = "t3_daily"
MAX_SEEN = datetime(2026, 7, 13, 14, 30, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_rollover_moves_previous_thread_to_draining() -> None:
    session = AsyncMock()
    state = WsbDailyState()
    state.thread_id = "t3_old"

    stored: dict[str, str | None] = {
        "wsb_daily:thread_id": "t3_old",
        "wsb_daily:draining": json.dumps([]),
    }

    async def fake_get(_session, key: str) -> str | None:
        return stored.get(key)

    async def fake_set(_session, key: str, value: str | None) -> None:
        stored[key] = value

    with patch("ingest.wsb_daily.get_poller_state", side_effect=fake_get):
        with patch("ingest.wsb_daily.set_poller_state", side_effect=fake_set):
            await _handle_thread_rollover(session, state, "t3_new")

    assert state.thread_id == "t3_new"
    assert stored["wsb_daily:thread_id"] == "t3_new"
    draining = json.loads(stored["wsb_daily:draining"] or "[]")
    assert "t3_old" in draining
    assert "wsb_daily:drain_started:t3_old" in stored


def test_threads_to_poll_includes_draining_predecessor() -> None:
    state = WsbDailyState()
    state.thread_id = "t3_new"
    state.draining_thread_ids = ["t3_old"]

    threads = _threads_to_poll(state, "t3_new")
    assert threads == ["t3_new", "t3_old"]


def test_threads_to_poll_deduplicates_primary() -> None:
    state = WsbDailyState()
    state.draining_thread_ids = ["t3_new", "t3_old"]

    threads = _threads_to_poll(state, "t3_new")
    assert threads == ["t3_new", "t3_old"]


@pytest.mark.asyncio
async def test_force_advances_cursor_after_three_truncations(caplog: pytest.LogCaptureFixture) -> None:
    session = AsyncMock()
    stored: dict[str, str | None] = {
        wsb_daily_cursor_key(THREAD_ID): "2026-07-13T14:00:00+00:00",
        wsb_daily_truncation_streak_key(THREAD_ID): "0",
    }

    async def fake_get(_session, key: str) -> str | None:
        return stored.get(key)

    async def fake_set(_session, key: str, value: str | None) -> None:
        stored[key] = value

    with caplog.at_level(logging.ERROR):
        with patch("ingest.wsb_daily.get_poller_state", side_effect=fake_get):
            with patch("ingest.wsb_daily.set_poller_state", side_effect=fake_set):
                for expected_streak in range(1, WSB_DAILY_MAX_TRUNCATION_STREAK):
                    advanced = await _update_cursor_for_thread(
                        session,
                        THREAD_ID,
                        truncated=True,
                        max_seen_utc=MAX_SEEN,
                        num_comments=5000,
                        fetched_count=2048,
                        ingested=2048,
                    )
                    assert not advanced
                    assert stored[wsb_daily_truncation_streak_key(THREAD_ID)] == str(expected_streak)

                advanced = await _update_cursor_for_thread(
                    session,
                    THREAD_ID,
                    truncated=True,
                    max_seen_utc=MAX_SEEN,
                    num_comments=5000,
                    fetched_count=2048,
                    ingested=2048,
                )

    assert advanced
    assert stored[wsb_daily_cursor_key(THREAD_ID)] == MAX_SEEN.isoformat()
    assert stored[wsb_daily_truncation_streak_key(THREAD_ID)] == "0"
    assert "forcing cursor forward" in caplog.text
    assert THREAD_ID in caplog.text
    assert "estimated_skipped=2952" in caplog.text
