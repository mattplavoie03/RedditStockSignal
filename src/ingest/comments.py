"""Comment tree expansion helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from asyncpraw.models import MoreComments

from ingest.backoff import with_backoff
from ingest.constants import REPLACE_MORE_LIMIT, REPLACE_MORE_MAX_BATCHES, WSB_COMMENT_LIMIT
from ingest.serialize import utc_from_reddit

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CommentFetchResult:
    comments: list[Any]
    truncated: bool
    pending_more: int


def should_skip_comment(created: datetime, cursor_utc: datetime | None) -> bool:
    """Return True only for comments strictly older than the cursor.

  Comments at the exact cursor timestamp are re-fetched (upserts are idempotent).
    """
    return cursor_utc is not None and created < cursor_utc


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
    """Fetch comments newer than cursor_utc using sort=new."""
    submission.comment_sort = "new"
    submission.comment_limit = WSB_COMMENT_LIMIT
    await submission.load()

    truncated, pending_more = await _replace_more_batches(submission.comments)
    comments = await _collect_comments(submission.comments, depth=0, max_depth=max_depth)

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
        eligible = [c for c in comments if not should_skip_comment(utc_from_reddit(c.created_utc), cursor_utc)]
    else:
        eligible = comments

    return CommentFetchResult(comments=eligible, truncated=truncated, pending_more=pending_more)


async def _replace_more_batches(forest: Any) -> tuple[bool, int]:
    """Expand MoreComments buckets. Returns (truncated, pending_more_count)."""
    for batch in range(REPLACE_MORE_MAX_BATCHES):
        pending = [item for item in forest.list() if isinstance(item, MoreComments)]
        if not pending:
            return False, 0

        async def _expand() -> list[Any]:
            return await forest.replace_more(limit=REPLACE_MORE_LIMIT)

        expanded = await with_backoff(_expand, operation_name="replace_more")
        if not expanded:
            return True, len(pending)

        logger.debug("replace_more batch %s expanded %s comments", batch + 1, len(expanded))

    pending = [item for item in await forest.list() if isinstance(item, MoreComments)]
    return bool(pending), len(pending)


async def _collect_comments(forest: Any, *, depth: int, max_depth: int) -> list[Any]:
    comments: list[Any] = []
    for item in forest.list():
        if isinstance(item, MoreComments):
            continue
        if not hasattr(item, "body"):
            continue
        comments.append(item)
        if depth < max_depth:
            child_truncated, _ = await _replace_more_batches(item.replies)
            if child_truncated:
                logger.warning("child comment expansion truncated at depth %s", depth + 1)
            comments.extend(await _collect_comments(item.replies, depth=depth + 1, max_depth=max_depth))
    return comments
