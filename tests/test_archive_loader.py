"""Tests for Arctic Shift archive loader."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
import zstandard as zstd
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from archive.loader import (
    ASYNCPG_MAX_QUERY_PARAMS,
    LoadFilters,
    detect_kind,
    fullname_id,
    insert_batch,
    iter_archive_lines,
    load_archive_file,
    map_comment,
    map_post,
    matches_filters,
    max_rows_per_insert,
)
from ingest.serialize import utc_from_reddit

FIXTURE = Path(__file__).parent / "fixtures" / "archive_comments_sample.jsonl"


def _filters() -> LoadFilters:
    return LoadFilters(
        subreddits=frozenset({"smallstreetbets"}),
        start_date=date(2024, 12, 1),
        end_date=date(2024, 12, 31),
    )


def test_detect_kind_comments_vs_posts() -> None:
    assert detect_kind({"body": "hi", "id": "x"}) == "comments"
    assert detect_kind({"title": "Hello", "id": "y"}) == "posts"


def test_fullname_id_prefers_name_and_falls_back() -> None:
    assert fullname_id({"name": "t1_abc", "id": "abc"}, "comments") == "t1_abc"
    assert fullname_id({"id": "xyz"}, "comments") == "t1_xyz"
    assert fullname_id({"id": "xyz"}, "posts") == "t3_xyz"
    assert fullname_id({"id": "t3_already"}, "posts") == "t3_already"


def test_map_comment_from_fixture_line() -> None:
    obj = json.loads(FIXTURE.read_text().splitlines()[0])
    row = map_comment(obj)
    assert row["id"] == "t1_m0ajn7n"
    assert row["post_id"] == "t3_1h5qbgt"
    assert row["subreddit"] == "smallstreetbets"
    assert row["author"] == "anon_user_0"
    assert row["body"].startswith("Opening reads")
    assert row["score"] == 1
    assert row["created_utc"] == utc_from_reddit(1733273084)
    assert row["raw"]["retrieved_on"] == 1733273106
    assert row["raw"]["parent_id"] == "t3_1h5qbgt"


def test_map_comment_deleted_author_is_null() -> None:
    obj = json.loads(FIXTURE.read_text().splitlines()[7])
    assert obj["author"] == "[deleted]"
    row = map_comment(obj)
    assert row["author"] is None
    assert row["id"] == "t1_deletedauth"


def test_map_comment_missing_name_fallback() -> None:
    obj = json.loads(FIXTURE.read_text().splitlines()[9])
    assert "name" not in obj
    row = map_comment(obj)
    assert row["id"] == "t1_nonamefallbk"


def test_map_comment_removed_body_preserved() -> None:
    obj = json.loads(FIXTURE.read_text().splitlines()[8])
    row = map_comment(obj)
    assert row["body"] == "[removed]"


def test_map_strips_null_bytes_for_postgres() -> None:
    obj = {
        "name": "t1_nul",
        "id": "nul",
        "link_id": "t3_x",
        "subreddit": "pennystocks",
        "author": "trader",
        "body": "bad\x00null",
        "score": 1,
        "created_utc": 1733273084,
        "extra": {"note": "also\x00here"},
    }
    row = map_comment(obj)
    assert row["body"] == "badnull"
    assert "\x00" not in row["raw"]["body"]
    assert row["raw"]["extra"]["note"] == "alsohere"


def test_map_post_fields() -> None:
    obj = {
        "name": "t3_post1",
        "id": "post1",
        "subreddit": "Stocks",
        "author": "trader",
        "title": "DD on XYZ",
        "selftext": "long form",
        "score": 42,
        "num_comments": 7,
        "created_utc": 1733277389,
    }
    row = map_post(obj)
    assert row["id"] == "t3_post1"
    assert row["subreddit"] == "stocks"
    assert row["title"] == "DD on XYZ"
    assert row["selftext"] == "long form"
    assert row["num_comments"] == 7
    assert row["score"] == 42
    assert row["raw"]["title"] == "DD on XYZ"
    assert row["raw"] is not obj


def test_matches_filters_subreddit_and_date() -> None:
    filters = _filters()
    good = {"subreddit": "smallstreetbets", "created_utc": 1733273084}
    wrong_sub = {"subreddit": "cryptocurrency", "created_utc": 1733273084}
    old = {"subreddit": "smallstreetbets", "created_utc": 1_000_000_000}
    assert matches_filters(good, filters) is True
    assert matches_filters(wrong_sub, filters) is False
    assert matches_filters(old, filters) is False


def test_max_rows_per_insert_respects_asyncpg_param_cap() -> None:
    # 8 comment columns × 5000 rows = 40_000 params — must chunk under 32_000.
    assert max_rows_per_insert(8) == ASYNCPG_MAX_QUERY_PARAMS // 8
    assert max_rows_per_insert(8) * 8 <= ASYNCPG_MAX_QUERY_PARAMS
    assert max_rows_per_insert(8) < 5_000
    # 9 post columns stay under the default batch size.
    assert max_rows_per_insert(9) == ASYNCPG_MAX_QUERY_PARAMS // 9
    assert max_rows_per_insert(9) * 9 <= ASYNCPG_MAX_QUERY_PARAMS


@pytest.mark.asyncio
async def test_insert_batch_chunks_when_params_would_exceed_limit() -> None:
    """A 5_000-row comment batch must issue multiple INSERTs (8 cols → 4k max)."""
    chunk_size = max_rows_per_insert(8)
    assert chunk_size < 5_000
    rows = [
        {
            "id": f"t1_{i}",
            "post_id": "t3_x",
            "subreddit": "stocks",
            "author": "a",
            "body": "b",
            "score": 1,
            "created_utc": utc_from_reddit(1_700_000_000 + i),
            "raw": {},
        }
        for i in range(5_000)
    ]
    execute_sizes: list[int] = []

    class _Result:
        rowcount = 10

    class _Session:
        async def execute(self, stmt: Any) -> _Result:
            values = stmt.compile().params
            execute_sizes.append(len(values) // 8)
            return _Result()

        async def commit(self) -> None:
            return None

    inserted = await insert_batch(_Session(), "comments", rows)  # type: ignore[arg-type]
    assert inserted == 10 * len(execute_sizes)
    assert len(execute_sizes) == (5_000 + chunk_size - 1) // chunk_size
    assert max(execute_sizes) <= chunk_size
    assert sum(execute_sizes) == 5_000


def _session_factory() -> async_sessionmaker[AsyncSession]:
    class _Ctx:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *args: object) -> None:
            return None

    class _Factory:
        def __call__(self) -> _Ctx:
            return _Ctx()

    return _Factory()  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_load_fixture_filters_skips_and_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[tuple[str, datetime], dict[str, Any]] = {}

    async def fake_insert(
        session: AsyncSession,
        kind: str,
        rows: list[dict[str, Any]],
    ) -> int:
        assert kind == "comments"
        inserted = 0
        for row in rows:
            key = (row["id"], row["created_utc"])
            if key not in store:
                store[key] = row
                inserted += 1
        return inserted

    import archive.loader as loader_mod

    monkeypatch.setattr(loader_mod, "insert_batch", fake_insert)
    stats = await load_archive_file(
        FIXTURE,
        _session_factory(),
        filters=_filters(),
        kind="comments",
        batch_size=3,
    )

    # 11 lines: 5 real + wrong sub + old date + deleted + removed + no name + malformed
    assert stats.read == 11
    assert stats.skipped_malformed == 1
    assert stats.skipped_filter == 2  # wrong sub + old date
    assert stats.matched == 8
    assert stats.inserted == 8
    assert len(store) == 8

    deleted = store[("t1_deletedauth", utc_from_reddit(1733279439))]
    assert deleted["author"] is None

    fallback = store[("t1_nonamefallbk", utc_from_reddit(1733280342))]
    assert fallback["id"] == "t1_nonamefallbk"


@pytest.mark.asyncio
async def test_load_idempotent_on_second_run(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[tuple[str, datetime], dict[str, Any]] = {}

    async def fake_insert(
        session: AsyncSession,
        kind: str,
        rows: list[dict[str, Any]],
    ) -> int:
        inserted = 0
        for row in rows:
            key = (row["id"], row["created_utc"])
            if key not in store:
                store[key] = row
                inserted += 1
        return inserted

    import archive.loader as loader_mod

    monkeypatch.setattr(loader_mod, "insert_batch", fake_insert)
    factory = _session_factory()
    first = await load_archive_file(FIXTURE, factory, filters=_filters(), kind="comments")
    second = await load_archive_file(FIXTURE, factory, filters=_filters(), kind="comments")

    assert first.inserted == 8
    assert second.inserted == 0
    assert len(store) == 8


@pytest.mark.asyncio
async def test_zst_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zst_path = tmp_path / "sample.jsonl.zst"
    raw = FIXTURE.read_bytes()
    cctx = zstd.ZstdCompressor()
    zst_path.write_bytes(cctx.compress(raw))

    lines = list(iter_archive_lines(zst_path))
    assert len(lines) == 11
    assert json.loads(lines[0])["name"] == "t1_m0ajn7n"

    store: dict[tuple[str, datetime], dict[str, Any]] = {}

    async def fake_insert(
        session: AsyncSession,
        kind: str,
        rows: list[dict[str, Any]],
    ) -> int:
        inserted = 0
        for row in rows:
            key = (row["id"], row["created_utc"])
            if key not in store:
                store[key] = row
                inserted += 1
        return inserted

    import archive.loader as loader_mod

    monkeypatch.setattr(loader_mod, "insert_batch", fake_insert)
    stats = await load_archive_file(
        zst_path, _session_factory(), filters=_filters(), kind="auto"
    )

    assert stats.matched == 8
    assert stats.inserted == 8
    assert len(store) == 8
