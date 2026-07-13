"""Tests for poller_state key helpers."""

from __future__ import annotations

from ingest.constants import (
    COMMENT_PASSES,
    DEFAULT_SUBREDDITS,
    WSB_DAILY_DRAINING_KEY,
    WSB_DAILY_THREAD_KEY,
    wsb_daily_cursor_key,
    wsb_daily_truncation_streak_key,
)
from ingest.post_poller import poll_interval_sec


def test_default_subreddits_include_wsb() -> None:
    assert "wallstreetbets" in DEFAULT_SUBREDDITS


def test_comment_pass_windows_do_not_overlap_at_boundaries() -> None:
    one_hour, twenty_four_hour = COMMENT_PASSES
    assert one_hour.max_age_minutes < twenty_four_hour.min_age_minutes


def test_wsb_poll_state_keys() -> None:
    assert WSB_DAILY_THREAD_KEY.startswith("wsb_daily:")
    assert WSB_DAILY_DRAINING_KEY.startswith("wsb_daily:")
    assert wsb_daily_cursor_key("t3_abc").startswith("wsb_daily:cursor_utc:")
    assert wsb_daily_truncation_streak_key("t3_abc").startswith("wsb_daily:truncation_streak:")


def test_wsb_polls_faster_than_other_subs() -> None:
    assert poll_interval_sec("wallstreetbets") < poll_interval_sec("stocks")
