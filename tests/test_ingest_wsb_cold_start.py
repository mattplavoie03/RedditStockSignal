"""Tests for WSB daily cold-start cursor initialization."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest.wsb_daily import _initialize_cold_start_cursor


@pytest.mark.asyncio
async def test_cold_start_skips_backlog_and_sets_cursor_to_now() -> None:
    session = AsyncMock()
    submission = MagicMock()
    submission.load = AsyncMock()
    submission.num_comments = 7709
    submission.name = "t3_daily"

    stored: dict[str, str | None] = {}

    async def fake_set(_session, key: str, value: str | None) -> None:
        stored[key] = value

    with patch("ingest.wsb_daily.with_backoff", side_effect=lambda op, **kw: op()):
        with patch("ingest.wsb_daily.set_poller_state", side_effect=fake_set):
            with patch("ingest.wsb_daily.count_comments_for_post", return_value=0):
                with patch("ingest.wsb_daily.record_capture_stat", new=AsyncMock()) as record:
                    ingested = await _initialize_cold_start_cursor(session, submission, "t3_daily")

    assert ingested == 0
    assert "wsb_daily:cursor_utc:t3_daily" in stored
    cursor = datetime.fromisoformat(stored["wsb_daily:cursor_utc:t3_daily"])
    assert cursor.tzinfo is not None
    record.assert_awaited_once()
    assert record.await_args.kwargs["fetched_this_cycle"] == 0
    assert record.await_args.kwargs["was_truncated"] is False
    assert record.await_args.kwargs["num_comments_reported"] == 7709
