"""Database persistence for ingested Reddit data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import PollerState, RawComment, RawPost, WsbCaptureStats
from ingest.constants import CommentPass


async def upsert_post(session: AsyncSession, row: dict) -> None:
    """Insert a post or update mutable fields on conflict."""
    stmt = insert(RawPost).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[RawPost.id, RawPost.created_utc],
        set_={
            "score": stmt.excluded.score,
            "num_comments": stmt.excluded.num_comments,
        },
    )
    await session.execute(stmt)


async def upsert_comment(session: AsyncSession, row: dict) -> None:
    """Insert a comment or update score on conflict."""
    stmt = insert(RawComment).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[RawComment.id, RawComment.created_utc],
        set_={"score": stmt.excluded.score},
    )
    await session.execute(stmt)


async def get_poller_state(session: AsyncSession, key: str) -> str | None:
    result = await session.execute(select(PollerState.value).where(PollerState.key == key))
    return result.scalar_one_or_none()


async def set_poller_state(session: AsyncSession, key: str, value: str | None) -> None:
    stmt = insert(PollerState).values(key=key, value=value, updated_at=datetime.now(timezone.utc))
    stmt = stmt.on_conflict_do_update(
        index_elements=[PollerState.key],
        set_={"value": stmt.excluded.value, "updated_at": stmt.excluded.updated_at},
    )
    await session.execute(stmt)


async def comment_pass_done(session: AsyncSession, post_id: str, pass_suffix: str) -> bool:
    key = f"comment:{post_id}:{pass_suffix}"
    value = await get_poller_state(session, key)
    return value == "done"


async def mark_comment_pass_done(session: AsyncSession, post_id: str, pass_suffix: str) -> None:
    await set_poller_state(session, f"comment:{post_id}:{pass_suffix}", "done")


async def posts_needing_comment_pass(
    session: AsyncSession,
    comment_pass: CommentPass,
    *,
    now: datetime | None = None,
) -> list[RawPost]:
    """Return posts in the age window for a comment pass that are not yet fetched."""
    current = now or datetime.now(timezone.utc)
    min_created = current - timedelta(minutes=comment_pass.max_age_minutes)
    max_created = current - timedelta(minutes=comment_pass.min_age_minutes)

    result = await session.execute(
        select(RawPost)
        .where(RawPost.created_utc >= min_created)
        .where(RawPost.created_utc <= max_created)
        .order_by(RawPost.created_utc.asc())
    )
    posts = list(result.scalars().all())

    pending: list[RawPost] = []
    for post in posts:
        if await comment_pass_done(session, post.id, comment_pass.poller_key_suffix):
            continue
        pending.append(post)
    return pending


async def count_comments_for_post(session: AsyncSession, post_id: str) -> int:
    result = await session.execute(
        select(func.count()).select_from(RawComment).where(RawComment.post_id == post_id)
    )
    return int(result.scalar_one())


async def record_capture_stat(
    session: AsyncSession,
    *,
    thread_id: str,
    num_comments_reported: int | None,
    comments_in_db_for_thread: int,
    fetched_this_cycle: int,
    was_truncated: bool,
    polled_at: datetime | None = None,
) -> None:
    session.add(
        WsbCaptureStats(
            thread_id=thread_id,
            polled_at=polled_at or datetime.now(timezone.utc),
            num_comments_reported=num_comments_reported,
            comments_in_db_for_thread=comments_in_db_for_thread,
            fetched_this_cycle=fetched_this_cycle,
            was_truncated=was_truncated,
        )
    )
