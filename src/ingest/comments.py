"""Comment tree expansion helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from asyncpraw.models import MoreComments

from ingest.backoff import with_backoff
from ingest.constants import (
    REPLACE_MORE_LIMIT,
    REPLACE_MORE_MAX_BATCHES,
    WSB_COMMENT_LIMIT,
    WSB_REPLY_GRACE_HOURS,
)
from ingest.serialize import utc_from_reddit

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CommentFetchResult:
    comments: list[Any]
    truncated: bool
    pending_more: int
    raw_fetched_count: int


def should_skip_comment(created: datetime, cursor_utc: datetime | None) -> bool:
    """Return True only for comments strictly older than the cursor.

    Comments at the exact cursor timestamp are re-fetched (upserts are idempotent).
    """
    return cursor_utc is not None and created < cursor_utc


def reply_grace_floor(cursor_utc: datetime) -> datetime:
    """Oldest top-level parent whose replies we still expand."""
    return cursor_utc - timedelta(hours=WSB_REPLY_GRACE_HOURS)


async def expand_comment_forest(forest: Any, *, max_depth: int = 1) -> list[Any]:
    """Expand a comment forest to top-level and nested replies up to max_depth."""
    truncated, _ = await _replace_more_batches(forest)
    if truncated:
        logger.warning("comment expansion hit replace_more batch limit")
    return await _collect_comments(forest, depth=0, max_depth=max_depth)


async def fetch_comments_since(
    submission: Any,
    cursor_utc: datetime | None,
    *,
    max_depth: int = 2,
) -> CommentFetchResult:
    """Fetch comments at/after cursor_utc using sort=new, stopping once past the cursor.

    Top-level pagination early-stops (sort=new is ordered). Reply expansion uses a
    grace window so new replies under older parents are still collected.
    Intentional early-stop sets truncated=False; hitting batch/comment limits does not.
    """
    submission.comment_sort = "new"
    submission.comment_limit = WSB_COMMENT_LIMIT
    await submission.load()

    truncated, pending_more = await _replace_more_batches_until_cursor(
        submission.comments,
        cursor_utc,
    )
    comments = await _collect_comments_until_cursor(
        submission.comments,
        cursor_utc,
        depth=0,
        max_depth=max_depth,
    )

    # pending_more after intentional early-stop is 0 (we return False, 0).
    # Genuine truncation: exhausted batches with stubs still pending.
    if pending_more > 0:
        truncated = True
        logger.error(
            "WSB daily comment fetch truncated: %s MoreComments remain after %s batches for %s",
            pending_more,
            REPLACE_MORE_MAX_BATCHES,
            submission.name,
        )

    if len(comments) >= WSB_COMMENT_LIMIT:
        truncated = True
        logger.error(
            "WSB daily comment fetch hit comment_limit=%s for %s; burst may exceed window",
            WSB_COMMENT_LIMIT,
            submission.name,
        )

    if cursor_utc is not None:
        eligible = [
            c for c in comments if not should_skip_comment(utc_from_reddit(c.created_utc), cursor_utc)
        ]
    else:
        eligible = comments

    return CommentFetchResult(
        comments=eligible,
        truncated=truncated,
        pending_more=pending_more,
        raw_fetched_count=len(comments),
    )


async def _replace_more_batches(forest: Any) -> tuple[bool, int]:
    """Expand MoreComments buckets. Returns (truncated, pending_more_count)."""
    return await _replace_more_batches_until_cursor(forest, cursor_utc=None)


async def _replace_more_batches_until_cursor(
    forest: Any,
    cursor_utc: datetime | None,
) -> tuple[bool, int]:
    """Expand MoreComments until cleared, batch limit, or past cursor (sort=new).

    Intentional early-stop returns (False, 0) — not truncated.
    Exhausting REPLACE_MORE_MAX_BATCHES with stubs remaining returns (True, n).
    """
    for batch in range(REPLACE_MORE_MAX_BATCHES):
        pending = [item for item in forest.list() if isinstance(item, MoreComments)]
        if not pending:
            return False, 0

        if cursor_utc is not None and _top_level_reached_cursor(forest, cursor_utc):
            # Newest-first: remaining MoreComments only yield older top-level comments.
            # Replies under grace-window parents are expanded separately during collect.
            logger.debug("stopping replace_more early — top-level comments past cursor")
            return False, 0

        async def _expand() -> list[Any]:
            return await forest.replace_more(limit=REPLACE_MORE_LIMIT)

        expanded = await with_backoff(_expand, operation_name="replace_more")
        if not expanded:
            return True, len(pending)

        logger.debug("replace_more batch %s expanded %s comments", batch + 1, len(expanded))

    pending = [item for item in forest.list() if isinstance(item, MoreComments)]
    return bool(pending), len(pending)


def _top_level_reached_cursor(forest: Any, cursor_utc: datetime) -> bool:
    """True if any already-loaded top-level comment is older than the cursor."""
    for item in forest:
        if isinstance(item, MoreComments):
            continue
        if not hasattr(item, "created_utc"):
            continue
        if utc_from_reddit(item.created_utc) < cursor_utc:
            return True
    return False


async def _collect_comments(forest: Any, *, depth: int, max_depth: int) -> list[Any]:
    return await _collect_comments_until_cursor(
        forest, cursor_utc=None, depth=depth, max_depth=max_depth
    )


async def _collect_comments_until_cursor(
    forest: Any,
    cursor_utc: datetime | None,
    *,
    depth: int,
    max_depth: int,
) -> list[Any]:
    """Collect comments; early-stop top-level past grace, but still expand replies in-window.

    sort=new orders top-level only. A new reply can sit under an old parent, so we keep
    expanding replies for top-level comments within WSB_REPLY_GRACE_HOURS of the cursor.
    """
    comments: list[Any] = []
    grace_floor = reply_grace_floor(cursor_utc) if cursor_utc is not None else None

    for item in list(forest):
        if isinstance(item, MoreComments):
            continue
        if not hasattr(item, "body"):
            continue

        created = utc_from_reddit(item.created_utc) if hasattr(item, "created_utc") else None

        if depth == 0 and grace_floor is not None and created is not None and created < grace_floor:
            # Past grace window: remaining top-level siblings are older still — stop.
            break

        past_cursor = (
            depth == 0
            and cursor_utc is not None
            and created is not None
            and should_skip_comment(created, cursor_utc)
        )

        if not past_cursor:
            comments.append(item)

        if depth < max_depth:
            # Expand replies for in-cursor parents AND grace-window parents (old top-level
            # with possibly-new replies). Stop expanding only past the grace floor.
            within_grace = (
                cursor_utc is None
                or created is None
                or grace_floor is None
                or created >= grace_floor
            )
            if depth > 0 or within_grace:
                child_truncated, _ = await _replace_more_batches(item.replies)
                if child_truncated:
                    logger.warning("child comment expansion truncated at depth %s", depth + 1)
                child_comments = await _collect_comments_until_cursor(
                    item.replies,
                    cursor_utc,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
                if cursor_utc is not None:
                    child_comments = [
                        c
                        for c in child_comments
                        if not should_skip_comment(utc_from_reddit(c.created_utc), cursor_utc)
                    ]
                comments.extend(child_comments)

    return comments
