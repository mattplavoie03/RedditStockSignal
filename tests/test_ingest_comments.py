"""Tests for comment checkpoint boundary and early-stop fetch behavior."""

from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ingest import comments as comments_mod
from ingest.comments import (
    CommentFetchResult,
    _collect_comments_until_cursor,
    _replace_more_batches_until_cursor,
    _top_level_reached_cursor,
    reply_grace_floor,
    should_skip_comment,
)
from ingest.constants import REPLACE_MORE_MAX_BATCHES, WSB_REPLY_GRACE_HOURS


def test_should_skip_only_strictly_older_comments() -> None:
    cursor = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    same_second = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    older = datetime(2026, 7, 13, 13, 59, 59, tzinfo=timezone.utc)
    newer = datetime(2026, 7, 13, 14, 0, 1, tzinfo=timezone.utc)

    assert not should_skip_comment(same_second, cursor)
    assert should_skip_comment(older, cursor)
    assert not should_skip_comment(newer, cursor)


def test_should_not_skip_when_cursor_missing() -> None:
    created = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    assert not should_skip_comment(created, None)


def test_replace_more_never_awaits_list() -> None:
    """Regression: exhausted-batch path used to `await forest.list()` and crash."""
    source = inspect.getsource(comments_mod._replace_more_batches_until_cursor)
    assert "await forest.list()" not in source
    assert "forest.list()" in source


def test_top_level_reached_cursor() -> None:
    cursor = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    forest = [
        SimpleNamespace(created_utc=datetime(2026, 7, 13, 14, 5, tzinfo=timezone.utc).timestamp()),
        SimpleNamespace(created_utc=datetime(2026, 7, 13, 13, 50, tzinfo=timezone.utc).timestamp()),
    ]
    assert _top_level_reached_cursor(forest, cursor)


@pytest.mark.asyncio
async def test_new_reply_under_old_comment_within_grace() -> None:
    """sort=new does not order replies — new replies under old parents must be kept."""
    cursor = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    # Parent is 3h older than cursor (within 12h grace)
    old_parent = SimpleNamespace(
        body="old_parent",
        created_utc=datetime(2026, 7, 13, 11, 0, tzinfo=timezone.utc).timestamp(),
        replies=[
            SimpleNamespace(
                body="new_reply",
                created_utc=datetime(2026, 7, 13, 14, 5, tzinfo=timezone.utc).timestamp(),
                replies=[],
            ),
            SimpleNamespace(
                body="old_reply",
                created_utc=datetime(2026, 7, 13, 11, 30, tzinfo=timezone.utc).timestamp(),
                replies=[],
            ),
        ],
    )
    newer_top = SimpleNamespace(
        body="new_top",
        created_utc=datetime(2026, 7, 13, 14, 10, tzinfo=timezone.utc).timestamp(),
        replies=[],
    )
    # sort=new: newest top-level first
    forest = [newer_top, old_parent]

    with patch("ingest.comments._replace_more_batches", new=AsyncMock(return_value=(False, 0))):
        collected = await _collect_comments_until_cursor(forest, cursor, depth=0, max_depth=1)

    bodies = [c.body for c in collected]
    assert "new_top" in bodies
    assert "new_reply" in bodies
    assert "old_parent" not in bodies  # older than cursor — not eligible
    assert "old_reply" not in bodies


@pytest.mark.asyncio
async def test_top_level_past_grace_stops_without_expanding() -> None:
    cursor = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)
    floor = reply_grace_floor(cursor)
    assert WSB_REPLY_GRACE_HOURS == 12

    too_old_parent = SimpleNamespace(
        body="too_old",
        created_utc=(floor.timestamp() - 3600),  # 1h past grace
        replies=[
            SimpleNamespace(
                body="would_be_missed_new_reply",
                created_utc=datetime(2026, 7, 13, 14, 5, tzinfo=timezone.utc).timestamp(),
                replies=[],
            )
        ],
    )
    forest = [too_old_parent]
    replace = AsyncMock(return_value=(False, 0))

    with patch("ingest.comments._replace_more_batches", new=replace):
        collected = await _collect_comments_until_cursor(forest, cursor, depth=0, max_depth=1)

    assert collected == []
    replace.assert_not_called()


@pytest.mark.asyncio
async def test_intentional_early_stop_is_not_truncated() -> None:
    """Stopping because we passed the cursor must not set truncated=True."""
    cursor = datetime(2026, 7, 13, 14, 0, 0, tzinfo=timezone.utc)

    class StubMore:
        pass

    class FakeForest:
        def __init__(self) -> None:
            self.replace_calls = 0
            self._items = [
                SimpleNamespace(
                    created_utc=datetime(2026, 7, 13, 13, 50, tzinfo=timezone.utc).timestamp()
                ),
                StubMore(),
            ]

        def __iter__(self):
            return iter(self._items)

        def list(self) -> list:
            return list(self._items)

        async def replace_more(self, *, limit: int) -> list:
            self.replace_calls += 1
            return ["expanded"]

    forest = FakeForest()
    original = comments_mod.MoreComments
    comments_mod.MoreComments = StubMore  # type: ignore[misc, assignment]
    try:
        truncated, pending = await _replace_more_batches_until_cursor(forest, cursor)
    finally:
        comments_mod.MoreComments = original

    assert truncated is False
    assert pending == 0
    assert forest.replace_calls == 0


@pytest.mark.asyncio
async def test_exhausted_batches_with_pending_is_truncated() -> None:
    """Hitting REPLACE_MORE_MAX_BATCHES with stubs remaining is genuine truncation."""

    class StubMore:
        pass

    class FakeForest:
        def list(self) -> list:
            return [StubMore()]

        async def replace_more(self, *, limit: int) -> list:
            return ["expanded"]

    forest = FakeForest()
    original = comments_mod.MoreComments
    comments_mod.MoreComments = StubMore  # type: ignore[misc, assignment]

    async def run_op(op, **kw):
        return await op()

    try:
        with patch("ingest.comments.with_backoff", side_effect=run_op):
            truncated, pending = await _replace_more_batches_until_cursor(forest, cursor_utc=None)
    finally:
        comments_mod.MoreComments = original

    assert truncated is True
    assert pending == 1


def test_comment_fetch_result_distinguishes_truncation_flag() -> None:
    ok = CommentFetchResult(comments=[], truncated=False, pending_more=0, raw_fetched_count=10)
    bad = CommentFetchResult(comments=[], truncated=True, pending_more=3, raw_fetched_count=2048)
    assert ok.truncated is False
    assert bad.truncated is True
    assert REPLACE_MORE_MAX_BATCHES == 50
