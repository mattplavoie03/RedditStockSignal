"""Load Arctic Shift Reddit archive dumps into raw_* tables.

Archive-first ingestion replaces the live Reddit API pollers (API access revoked
July 2026). Dumps are JSONL (optionally zstandard-compressed); each line is one
raw Reddit API payload object.

Caveats for downstream phases:
- ``score`` is as-of ``retrieved_on``, not posting time.
- ``[removed]``/``[deleted]`` bodies produce no extractable mentions.
- ``author == "[deleted]"`` is stored as NULL and cannot count toward unique authors.
"""

from __future__ import annotations

import io
import json
import logging
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

import zstandard as zstd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.models import RawComment, RawPost
from ingest.constants import DEFAULT_SUBREDDITS
from ingest.serialize import utc_from_reddit

logger = logging.getLogger(__name__)

Kind = Literal["posts", "comments", "auto"]
ResolvedKind = Literal["posts", "comments"]

DEFAULT_BATCH_SIZE = 5_000
PROGRESS_EVERY = 50_000
ZSTD_MAX_WINDOW = 2**31
# asyncpg caps bind parameters at 32_767 per statement; stay under that.
ASYNCPG_MAX_QUERY_PARAMS = 32_000


@dataclass
class LoadStats:
    read: int = 0
    matched: int = 0
    inserted: int = 0
    skipped_malformed: int = 0
    skipped_filter: int = 0
    files: int = 0

    def merge(self, other: LoadStats) -> None:
        self.read += other.read
        self.matched += other.matched
        self.inserted += other.inserted
        self.skipped_malformed += other.skipped_malformed
        self.skipped_filter += other.skipped_filter
        self.files += other.files


@dataclass
class LoadFilters:
    subreddits: frozenset[str] = field(
        default_factory=lambda: frozenset(s.lower() for s in DEFAULT_SUBREDDITS)
    )
    start_date: date | None = None
    end_date: date | None = None


def detect_kind(obj: dict[str, Any]) -> ResolvedKind:
    """Auto-detect posts vs comments via presence of ``title`` (verified on samples)."""
    return "posts" if "title" in obj else "comments"


def archive_author(value: Any) -> str | None:
    if value is None or value == "[deleted]":
        return None
    return strip_null_bytes(str(value))


def strip_null_bytes(value: str) -> str:
    """Postgres text/JSONB reject U+0000; Arctic Shift dumps occasionally contain it."""
    return value.replace("\x00", "")


def sanitize_for_postgres(value: Any) -> Any:
    """Recursively strip null bytes from strings (mapped fields and ``raw`` JSON)."""
    if isinstance(value, str):
        return strip_null_bytes(value)
    if isinstance(value, dict):
        return {str(k): sanitize_for_postgres(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_postgres(v) for v in value]
    return value


def fullname_id(obj: dict[str, Any], kind: ResolvedKind) -> str:
    """Prefer dump ``name`` (already fullname-prefixed); else build from ``id``."""
    name = obj.get("name")
    if isinstance(name, str) and name:
        return name
    raw_id = obj.get("id")
    if raw_id is None:
        raise ValueError("archive object missing both name and id")
    prefix = "t3_" if kind == "posts" else "t1_"
    text = str(raw_id)
    if text.startswith(("t1_", "t3_")):
        return text
    return f"{prefix}{text}"


def map_comment(obj: dict[str, Any]) -> dict[str, Any]:
    link_id = obj.get("link_id")
    if not link_id:
        raise ValueError("comment missing link_id")
    body = obj.get("body")
    return {
        "id": fullname_id(obj, "comments"),
        "post_id": strip_null_bytes(str(link_id)),
        "subreddit": strip_null_bytes(str(obj.get("subreddit", "")).lower()),
        "author": archive_author(obj.get("author")),
        "body": strip_null_bytes(body) if isinstance(body, str) else body,
        "score": obj.get("score"),
        "created_utc": utc_from_reddit(float(obj["created_utc"])),
        "raw": sanitize_for_postgres(obj),
    }


def map_post(obj: dict[str, Any]) -> dict[str, Any]:
    title = obj.get("title")
    selftext = obj.get("selftext")
    return {
        "id": fullname_id(obj, "posts"),
        "subreddit": strip_null_bytes(str(obj.get("subreddit", "")).lower()),
        "author": archive_author(obj.get("author")),
        "title": strip_null_bytes(title) if isinstance(title, str) else title,
        "selftext": strip_null_bytes(selftext) if isinstance(selftext, str) else selftext,
        "score": obj.get("score"),
        "num_comments": obj.get("num_comments"),
        "created_utc": utc_from_reddit(float(obj["created_utc"])),
        "raw": sanitize_for_postgres(obj),
    }


def map_object(obj: dict[str, Any], kind: ResolvedKind) -> dict[str, Any]:
    return map_post(obj) if kind == "posts" else map_comment(obj)


def matches_filters(obj: dict[str, Any], filters: LoadFilters) -> bool:
    subreddit = str(obj.get("subreddit", "")).lower()
    if filters.subreddits and subreddit not in filters.subreddits:
        return False
    created = utc_from_reddit(float(obj["created_utc"])).date()
    if filters.start_date is not None and created < filters.start_date:
        return False
    if filters.end_date is not None and created > filters.end_date:
        return False
    return True


def iter_archive_lines(path: Path) -> Iterator[str]:
    """Yield text lines from plain JSONL or zstandard-compressed JSONL."""
    compressed = path.suffix == ".zst" or path.name.endswith(".jsonl.zst")
    if compressed:
        with path.open("rb") as fh:
            dctx = zstd.ZstdDecompressor(max_window_size=ZSTD_MAX_WINDOW)
            with dctx.stream_reader(fh) as reader:
                text = io.TextIOWrapper(reader, encoding="utf-8")
                for line in text:
                    yield line
    else:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                yield line


def resolve_kind(path: Path, kind: Kind) -> ResolvedKind:
    if kind != "auto":
        return kind
    for line in iter_archive_lines(path):
        stripped = line.strip()
        if not stripped:
            continue
        return detect_kind(json.loads(stripped))
    raise ValueError(f"cannot auto-detect kind: empty file {path}")


def max_rows_per_insert(num_columns: int, *, param_limit: int = ASYNCPG_MAX_QUERY_PARAMS) -> int:
    """Largest multi-row INSERT that stays under asyncpg's bind-parameter cap."""
    if num_columns < 1:
        raise ValueError("num_columns must be >= 1")
    return max(param_limit // num_columns, 1)


async def insert_batch(
    session: AsyncSession,
    kind: ResolvedKind,
    rows: Sequence[dict[str, Any]],
) -> int:
    """Multi-row INSERT ... ON CONFLICT DO NOTHING. Returns rows actually inserted.

    Chunks when ``len(rows) * num_columns`` would exceed asyncpg's parameter limit.
    """
    if not rows:
        return 0
    model = RawPost if kind == "posts" else RawComment
    chunk_size = max_rows_per_insert(len(rows[0]))
    inserted = 0
    for start in range(0, len(rows), chunk_size):
        chunk = list(rows[start : start + chunk_size])
        stmt = insert(model).values(chunk).on_conflict_do_nothing(
            index_elements=["id", "created_utc"]
        )
        result = await session.execute(stmt)
        inserted += max(int(result.rowcount or 0), 0)
    await session.commit()
    return inserted


async def load_archive_file(
    path: Path,
    session_factory: async_sessionmaker[AsyncSession],
    *,
    filters: LoadFilters | None = None,
    kind: Kind = "auto",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> LoadStats:
    """Stream one archive file into raw_posts or raw_comments."""
    filters = filters or LoadFilters()
    resolved = resolve_kind(path, kind)
    stats = LoadStats(files=1)
    batch: list[dict[str, Any]] = []
    started = time.monotonic()

    async with session_factory() as session:
        for line in iter_archive_lines(path):
            stats.read += 1
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError:
                stats.skipped_malformed += 1
                logger.warning("malformed JSON in %s line ~%s", path.name, stats.read)
                continue

            try:
                if not matches_filters(obj, filters):
                    stats.skipped_filter += 1
                    continue
                row = map_object(obj, resolved)
            except (KeyError, TypeError, ValueError) as exc:
                stats.skipped_malformed += 1
                logger.warning("skip unmappable object in %s: %s", path.name, exc)
                continue

            stats.matched += 1
            batch.append(row)
            if len(batch) >= batch_size:
                stats.inserted += await insert_batch(session, resolved, batch)
                batch.clear()

            if stats.read % PROGRESS_EVERY == 0:
                elapsed = max(time.monotonic() - started, 1e-9)
                logger.info(
                    "progress %s: read=%s matched=%s inserted=%s skipped_malformed=%s "
                    "skipped_filter=%s (%.0f lines/sec)",
                    path.name,
                    stats.read,
                    stats.matched,
                    stats.inserted,
                    stats.skipped_malformed,
                    stats.skipped_filter,
                    stats.read / elapsed,
                )

        if batch:
            stats.inserted += await insert_batch(session, resolved, batch)

    elapsed = max(time.monotonic() - started, 1e-9)
    logger.info(
        "finished %s (%s): read=%s matched=%s inserted=%s skipped_malformed=%s "
        "skipped_filter=%s (%.0f lines/sec)",
        path.name,
        resolved,
        stats.read,
        stats.matched,
        stats.inserted,
        stats.skipped_malformed,
        stats.skipped_filter,
        stats.read / elapsed,
    )
    return stats


async def load_archive_files(
    paths: Iterable[Path],
    session_factory: async_sessionmaker[AsyncSession],
    *,
    filters: LoadFilters | None = None,
    kind: Kind = "auto",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> LoadStats:
    total = LoadStats()
    for path in paths:
        file_stats = await load_archive_file(
            path,
            session_factory,
            filters=filters,
            kind=kind,
            batch_size=batch_size,
        )
        total.merge(file_stats)
    return total
