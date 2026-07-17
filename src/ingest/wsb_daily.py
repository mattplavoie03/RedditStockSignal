"""Incremental poller for the WSB daily discussion thread.

Deprecated: live Reddit API access was revoked July 2026. Prefer Arctic Shift
archive dumps via ``archive.loader`` / ``scripts/load_archive.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpraw
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingest.backoff import with_backoff
from ingest.comments import fetch_comments_since, should_skip_comment
from ingest.constants import (
    POLL_JITTER_SEC,
    WSB_DAILY_DRAIN_IDLE_POLLS,
    WSB_DAILY_DRAINING_KEY,
    WSB_DAILY_MAX_DRAIN_HOURS,
    WSB_DAILY_MAX_TRUNCATION_STREAK,
    WSB_DAILY_POLL_INTERVAL_SEC,
    WSB_DAILY_THREAD_KEY,
    wsb_daily_cursor_key,
    wsb_daily_drain_started_key,
    wsb_daily_idle_polls_key,
    wsb_daily_truncation_streak_key,
)
from ingest.daily_thread import is_daily_thread_title
from ingest.metrics import IngestMetrics
from ingest.repository import (
    count_comments_for_post,
    get_poller_state,
    record_capture_stat,
    set_poller_state,
    upsert_comment,
)
from ingest.serialize import comment_to_row, subreddit_name, utc_from_reddit

logger = logging.getLogger(__name__)


class WsbDailyState:
    """Tracks the active daily thread and threads still being drained."""

    def __init__(self) -> None:
        self.thread_id: str | None = None
        self.draining_thread_ids: list[str] = []

    async def load(self, session: AsyncSession) -> None:
        self.thread_id = await get_poller_state(session, WSB_DAILY_THREAD_KEY)
        self.draining_thread_ids = await _get_draining_threads(session)


async def run_wsb_daily_poller(
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
    state: WsbDailyState,
) -> None:
    """Incrementally poll comments from the WSB daily thread and draining predecessors."""
    logger.info("starting WSB daily thread poller")
    while True:
        ingested = await _poll_once(reddit, session_factory, metrics, state)
        logger.info("WSB daily poll complete: %s new comments", ingested)
        await asyncio.sleep(WSB_DAILY_POLL_INTERVAL_SEC + random.uniform(0, POLL_JITTER_SEC))


async def _poll_once(
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
    state: WsbDailyState,
) -> int:
    primary = await with_backoff(
        lambda: find_daily_thread_submission(reddit),
        operation_name="find WSB daily thread",
    )
    if primary is None:
        logger.warning("no WSB daily thread found")
        return 0

    async with session_factory() as session:
        await _handle_thread_rollover(session, state, primary.name)

        thread_ids = _threads_to_poll(state, primary.name)
        total_ingested = 0
        for thread_id in thread_ids:
            count = await _poll_thread_comments(reddit, session, thread_id, metrics)
            total_ingested += count
            if thread_id != primary.name:
                await _update_draining_thread(session, state, thread_id, count)

        await session.commit()

    return total_ingested


def _threads_to_poll(state: WsbDailyState, primary_thread_id: str) -> list[str]:
    threads = [primary_thread_id]
    for thread_id in state.draining_thread_ids:
        if thread_id != primary_thread_id and thread_id not in threads:
            threads.append(thread_id)
    return threads


async def _poll_thread_comments(
    reddit: asyncpraw.Reddit,
    session: AsyncSession,
    thread_id: str,
    metrics: IngestMetrics,
) -> int:
    cursor_utc = _parse_ts(await get_poller_state(session, wsb_daily_cursor_key(thread_id)))

    async def _load_submission():
        submission = await reddit.submission(id=thread_id.removeprefix("t3_"), fetch=False)
        return submission

    submission = await with_backoff(_load_submission, operation_name=f"load WSB thread {thread_id}")

    if cursor_utc is None:
        return await _initialize_cold_start_cursor(session, submission, thread_id)

    fetch_result = await with_backoff(
        lambda: fetch_comments_since(submission, cursor_utc, max_depth=2),
        operation_name=f"fetch WSB daily comments {thread_id}",
    )

    subreddit = subreddit_name(submission.subreddit)
    ingested = 0
    max_seen_utc: datetime | None = None

    for comment in fetch_result.comments:
        created = utc_from_reddit(comment.created_utc)
        if max_seen_utc is None or created > max_seen_utc:
            max_seen_utc = created
        if should_skip_comment(created, cursor_utc):
            continue
        row = comment_to_row(comment, post_id=submission.name, subreddit=subreddit)
        await upsert_comment(session, row)
        ingested += 1

    cursor_advanced = await _update_cursor_for_thread(
        session,
        thread_id,
        truncated=fetch_result.truncated,
        max_seen_utc=max_seen_utc,
        num_comments=submission.num_comments,
        fetched_count=fetch_result.raw_fetched_count,
        ingested=ingested,
    )

    comments_in_db = await count_comments_for_post(session, thread_id)
    await record_capture_stat(
        session,
        thread_id=thread_id,
        num_comments_reported=submission.num_comments,
        comments_in_db_for_thread=comments_in_db,
        fetched_this_cycle=fetch_result.raw_fetched_count,
        was_truncated=fetch_result.truncated,
    )

    metrics.record_comments(ingested)
    logger.info(
        "WSB thread %s: ingested=%s truncated=%s cursor_advanced=%s cursor=%s",
        thread_id,
        ingested,
        fetch_result.truncated,
        cursor_advanced,
        max_seen_utc if cursor_advanced else cursor_utc,
    )
    return ingested


async def _initialize_cold_start_cursor(
    session: AsyncSession,
    submission: Any,
    thread_id: str,
) -> int:
    """Skip pre-start backlog: set cursor to now and only collect new comments."""
    await with_backoff(
        lambda: submission.load(),
        operation_name=f"load WSB thread metadata {thread_id}",
    )
    now = datetime.now(timezone.utc)
    num_existing = submission.num_comments or 0
    logger.info(
        "WSB daily cold start for %s: skipping %s pre-start comments, cursor initialized to now",
        thread_id,
        num_existing,
    )
    await set_poller_state(session, wsb_daily_cursor_key(thread_id), now.isoformat())
    await set_poller_state(session, wsb_daily_truncation_streak_key(thread_id), "0")

    comments_in_db = await count_comments_for_post(session, thread_id)
    await record_capture_stat(
        session,
        thread_id=thread_id,
        num_comments_reported=num_existing,
        comments_in_db_for_thread=comments_in_db,
        fetched_this_cycle=0,
        was_truncated=False,
        polled_at=now,
    )
    return 0


async def _update_cursor_for_thread(
    session: AsyncSession,
    thread_id: str,
    *,
    truncated: bool,
    max_seen_utc: datetime | None,
    num_comments: int | None,
    fetched_count: int,
    ingested: int,
) -> bool:
    """Update per-thread cursor and truncation streak. Returns True if cursor advanced."""
    streak_key = wsb_daily_truncation_streak_key(thread_id)
    cursor_key = wsb_daily_cursor_key(thread_id)

    if not truncated:
        await set_poller_state(session, streak_key, "0")
        if max_seen_utc is not None:
            await set_poller_state(session, cursor_key, max_seen_utc.isoformat())
            return True
        return False

    streak = int(await get_poller_state(session, streak_key) or "0") + 1
    await set_poller_state(session, streak_key, str(streak))

    if streak < WSB_DAILY_MAX_TRUNCATION_STREAK:
        logger.error(
            "WSB daily cursor not advanced for %s due to truncated fetch "
            "(ingested=%s streak=%s/%s)",
            thread_id,
            ingested,
            streak,
            WSB_DAILY_MAX_TRUNCATION_STREAK,
        )
        return False

    if max_seen_utc is None:
        logger.error(
            "WSB daily truncation streak exhausted for %s but no comments were fetched; resetting streak",
            thread_id,
        )
        await set_poller_state(session, streak_key, "0")
        return False

    estimated_skipped = max(0, (num_comments or 0) - fetched_count)
    logger.error(
        "WSB daily forcing cursor forward for %s after %s consecutive truncated fetches; "
        "estimated_skipped=%s (num_comments=%s fetched=%s ingested=%s)",
        thread_id,
        streak,
        estimated_skipped,
        num_comments,
        fetched_count,
        ingested,
    )
    await set_poller_state(session, cursor_key, max_seen_utc.isoformat())
    await set_poller_state(session, streak_key, "0")
    return True


async def _handle_thread_rollover(session: AsyncSession, state: WsbDailyState, thread_id: str) -> None:
    previous = state.thread_id or await get_poller_state(session, WSB_DAILY_THREAD_KEY)
    if previous and previous != thread_id:
        logger.info("WSB daily thread rollover: %s -> %s (draining predecessor)", previous, thread_id)
        draining = await _get_draining_threads(session)
        if previous not in draining:
            draining.append(previous)
            await _set_draining_threads(session, draining)
            await set_poller_state(
                session,
                wsb_daily_drain_started_key(previous),
                datetime.now(timezone.utc).isoformat(),
            )
            await set_poller_state(session, wsb_daily_idle_polls_key(previous), "0")
        state.draining_thread_ids = draining

    if previous != thread_id:
        await set_poller_state(session, WSB_DAILY_THREAD_KEY, thread_id)
        state.thread_id = thread_id


async def _update_draining_thread(
    session: AsyncSession,
    state: WsbDailyState,
    thread_id: str,
    ingested: int,
) -> None:
    if ingested > 0:
        await set_poller_state(session, wsb_daily_idle_polls_key(thread_id), "0")
        return

    idle = int(await get_poller_state(session, wsb_daily_idle_polls_key(thread_id)) or "0") + 1
    await set_poller_state(session, wsb_daily_idle_polls_key(thread_id), str(idle))

    drain_started = _parse_ts(await get_poller_state(session, wsb_daily_drain_started_key(thread_id)))
    drain_age = datetime.now(timezone.utc) - drain_started if drain_started else timedelta(0)
    should_retire = idle >= WSB_DAILY_DRAIN_IDLE_POLLS or drain_age >= timedelta(hours=WSB_DAILY_MAX_DRAIN_HOURS)

    if should_retire:
        draining = [tid for tid in state.draining_thread_ids if tid != thread_id]
        await _set_draining_threads(session, draining)
        state.draining_thread_ids = draining
        logger.info(
            "retired draining WSB thread %s (idle_polls=%s drain_age=%s)",
            thread_id,
            idle,
            drain_age,
        )


async def _get_draining_threads(session: AsyncSession) -> list[str]:
    raw = await get_poller_state(session, WSB_DAILY_DRAINING_KEY)
    if not raw:
        return []
    return json.loads(raw)


async def _set_draining_threads(session: AsyncSession, thread_ids: list[str]) -> None:
    await set_poller_state(session, WSB_DAILY_DRAINING_KEY, json.dumps(thread_ids))


async def find_daily_thread_submission(reddit: asyncpraw.Reddit):
    subreddit = await reddit.subreddit("wallstreetbets")
    async for submission in subreddit.hot(limit=15):
        if submission.stickied and is_daily_thread_title(submission.title):
            return submission
    return None


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
