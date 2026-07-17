"""Subreddit /new post poller.

Deprecated: live Reddit API access was revoked July 2026. Prefer Arctic Shift
archive dumps via ``archive.loader`` / ``scripts/load_archive.py``.
"""

from __future__ import annotations

import asyncio
import logging
import random

import asyncpraw
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingest.backoff import with_backoff
from ingest.constants import (
    DEFAULT_SUBREDDITS,
    FAST_POLL_INTERVAL_SEC,
    FAST_POLL_SUBREDDITS,
    POLL_JITTER_SEC,
    POST_FETCH_LIMIT,
    SLOW_POLL_INTERVAL_SEC,
)
from ingest.metrics import IngestMetrics
from ingest.repository import upsert_post
from ingest.serialize import submission_to_row

logger = logging.getLogger(__name__)


def poll_interval_sec(subreddit: str) -> float:
    base = FAST_POLL_INTERVAL_SEC if subreddit in FAST_POLL_SUBREDDITS else SLOW_POLL_INTERVAL_SEC
    return base + random.uniform(0, POLL_JITTER_SEC)


async def poll_subreddit_posts(
    subreddit_name: str,
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
) -> None:
    """Continuously poll /new for a single subreddit."""
    logger.info("starting post poller for r/%s", subreddit_name)
    while True:
        ingested = await _poll_once(subreddit_name, reddit, session_factory, metrics)
        logger.info("r/%s post poll complete: %s submissions processed", subreddit_name, ingested)
        await asyncio.sleep(poll_interval_sec(subreddit_name))


async def _poll_once(
    subreddit_name: str,
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
) -> int:
    async def _fetch():
        subreddit = await reddit.subreddit(subreddit_name)
        submissions = []
        async for submission in subreddit.new(limit=POST_FETCH_LIMIT):
            submissions.append(submission)
        return submissions

    submissions = await with_backoff(_fetch, operation_name=f"r/{subreddit_name} /new")

    count = 0
    async with session_factory() as session:
        for submission in submissions:
            row = submission_to_row(submission)
            await upsert_post(session, row)
            count += 1
        await session.commit()

    metrics.record_posts(count)
    return count


async def run_post_pollers(
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
    subreddits: tuple[str, ...] = DEFAULT_SUBREDDITS,
) -> None:
    async with asyncio.TaskGroup() as group:
        for name in subreddits:
            group.create_task(poll_subreddit_posts(name, reddit, session_factory, metrics))
