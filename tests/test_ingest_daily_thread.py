"""Tests for daily thread title detection."""

from __future__ import annotations

from ingest.daily_thread import is_daily_thread_title


def test_matches_daily_discussion() -> None:
    assert is_daily_thread_title("Daily Discussion Thread for July 13, 2026")


def test_matches_moves_thread() -> None:
    assert is_daily_thread_title("What Are Your Moves Tomorrow, July 14, 2026")


def test_rejects_regular_post() -> None:
    assert not is_daily_thread_title("I bought RKLB at 110. Did I screw up?")
