"""Comment ingestion for regular (non-daily-thread) posts."""

from __future__ import annotations

import asyncio
import logging

import asyncpraw
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.models import RawPost
from ingest.backoff import with_backoff
from ingest.comments import expand_comment_forest
from ingest.constants import COMMENT_PASSES, COMMENT_POLL_INTERVAL_SEC
from ingest.daily_thread import is_daily_thread_title
from ingest.metrics import IngestMetrics
from ingest.repository import mark_comment_pass_done, posts_needing_comment_pass, upsert_comment
from ingest.serialize import comment_to_row, subreddit_name
from ingest.wsb_daily import WsbDailyState


logger = logging.getLogger(__name__)


async def run_comment_poller(
    reddit: asyncpraw.Reddit,
    session_factory: async_sessionmaker[AsyncSession],
    metrics: IngestMetrics,
    *,
    wsb_state: WsbDailyState,
) -> None:
    """Poll for posts needing 1h and 24h comment fetches."""
    logger.info("starting comment poller")
    while True:
        processed = 0
        async with session_factory() as session:
            for comment_pass in COMMENT_PASSES:
                posts = await posts_needing_comment_pass(session, comment_pass)
                for post in posts:
                    if _skip_post(post, wsb_state.thread_id):
                        await mark_comment_pass_done(session, post.id, comment_pass.poller_key_suffix)
                        continue
                    count = await _fetch_comments_for_post(
                        reddit,
                        session,
                        post,
                        metrics,
                    )
                    await mark_comment_pass_done(session, post.id, comment_pass.poller_key_suffix)
                    processed += count
            await session.commit()

        logger.info("comment poll complete: %s comments ingested", processed)
        await asyncio.sleep(COMMENT_POLL_INTERVAL_SEC)


def _skip_post(post: RawPost, wsb_daily_thread_id: str | None) -> bool:
    if wsb_daily_thread_id and post.id == wsb_daily_thread_id:
        return True
    return post.subreddit == "wallstreetbets" and is_daily_thread_title(post.title or "")


async def _fetch_comments_for_post(
    reddit: asyncpraw.Reddit,
    session: AsyncSession,
    post: RawPost,
    metrics: IngestMetrics,
) -> int:
    async def _load_submission():
        return await reddit.submission(id=post.id.removeprefix("t3_"), fetch=False)

    submission = await with_backoff(_load_submission, operation_name=f"load submission {post.id}")

    async def _expand():
        if not submission._fetched:
            await submission.load()
        return await expand_comment_forest(submission.comments, max_depth=1)

    comments = await with_backoff(_expand, operation_name=f"expand comments {post.id}")

    subreddit = subreddit_name(submission.subreddit)
    count = 0
    for comment in comments:
        row = comment_to_row(comment, post_id=submission.name, subreddit=subreddit)
        await upsert_comment(session, row)
        count += 1

    metrics.record_comments(count)
    logger.info("ingested %s comments for post %s", count, post.id)
    return count
