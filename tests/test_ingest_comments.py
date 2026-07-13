"""Tests for comment checkpoint boundary behavior."""

from __future__ import annotations

from datetime import datetime, timezone

from ingest.comments import should_skip_comment


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
