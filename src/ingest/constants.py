"""Ingestion configuration constants."""

from __future__ import annotations

from dataclasses import dataclass

FAST_POLL_SUBREDDITS = frozenset({"wallstreetbets"})
DEFAULT_SUBREDDITS = (
    "wallstreetbets",
    "stocks",
    "pennystocks",
    "smallstreetbets",
)

FAST_POLL_INTERVAL_SEC = 600  # ~10 min for WSB
SLOW_POLL_INTERVAL_SEC = 900  # ~15 min for other subs
POLL_JITTER_SEC = 60  # ±0–60s so requests aren't metronomic
POST_FETCH_LIMIT = 100

COMMENT_POLL_INTERVAL_SEC = 300  # scan for due 1h/24h passes
COMMENT_PASS_1H_MIN = 55
COMMENT_PASS_1H_MAX = 70
COMMENT_PASS_24H_MIN = 23 * 60 + 30  # 23.5 h
COMMENT_PASS_24H_MAX = 24 * 60 + 30  # 24.5 h

WSB_DAILY_POLL_INTERVAL_SEC = 600  # ~10 min
REPLACE_MORE_LIMIT = 32
REPLACE_MORE_MAX_BATCHES = 50
WSB_COMMENT_LIMIT = 2048  # asyncpraw maximum per fetch
# Expand replies under top-level comments this far older than the cursor.
# sort=new only orders top-level; fresh replies sit under old parents all day.
WSB_REPLY_GRACE_HOURS = 12

WSB_DAILY_THREAD_KEY = "wsb_daily:thread_id"
WSB_DAILY_DRAINING_KEY = "wsb_daily:draining"
WSB_DAILY_DRAIN_IDLE_POLLS = 6  # retire draining thread after 6 empty polls (~30 min)
WSB_DAILY_MAX_DRAIN_HOURS = 18
WSB_DAILY_MAX_TRUNCATION_STREAK = 3


def wsb_daily_cursor_key(thread_id: str) -> str:
    return f"wsb_daily:cursor_utc:{thread_id}"


def wsb_daily_drain_started_key(thread_id: str) -> str:
    return f"wsb_daily:drain_started:{thread_id}"


def wsb_daily_idle_polls_key(thread_id: str) -> str:
    return f"wsb_daily:idle_polls:{thread_id}"


def wsb_daily_truncation_streak_key(thread_id: str) -> str:
    return f"wsb_daily:truncation_streak:{thread_id}"


@dataclass(frozen=True, slots=True)
class CommentPass:
    name: str
    poller_key_suffix: str
    min_age_minutes: int
    max_age_minutes: int


COMMENT_PASSES = (
    CommentPass("1h", "1h", COMMENT_PASS_1H_MIN, COMMENT_PASS_1H_MAX),
    CommentPass("24h", "24h", COMMENT_PASS_24H_MIN, COMMENT_PASS_24H_MAX),
)
