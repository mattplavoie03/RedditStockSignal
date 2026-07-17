#!/usr/bin/env python3
"""Load Arctic Shift Reddit archive dumps into Postgres."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from archive.loader import DEFAULT_BATCH_SIZE, LoadFilters, load_archive_files
from config import get_settings
from db.session import create_engine, create_session_factory
from ingest.constants import DEFAULT_SUBREDDITS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Arctic Shift JSONL/.zst dumps into raw_posts / raw_comments"
    )
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        type=Path,
        help="One or more .jsonl or .jsonl.zst / .zst archive files",
    )
    parser.add_argument(
        "--subreddits",
        nargs="+",
        default=list(DEFAULT_SUBREDDITS),
        help=f"Subreddits to keep (default: {' '.join(DEFAULT_SUBREDDITS)})",
    )
    parser.add_argument("--start-date", type=date.fromisoformat, default=None, help="UTC inclusive YYYY-MM-DD")
    parser.add_argument("--end-date", type=date.fromisoformat, default=None, help="UTC inclusive YYYY-MM-DD")
    parser.add_argument(
        "--kind",
        choices=("auto", "posts", "comments"),
        default="auto",
        help="Force posts/comments or auto-detect via title field (default: auto)",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    for path in args.files:
        if not path.exists():
            logging.error("file not found: %s", path)
            return 1

    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    filters = LoadFilters(
        subreddits=frozenset(s.lower() for s in args.subreddits),
        start_date=args.start_date,
        end_date=args.end_date,
    )
    try:
        stats = await load_archive_files(
            args.files,
            session_factory,
            filters=filters,
            kind=args.kind,
            batch_size=args.batch_size,
        )
    finally:
        await engine.dispose()

    print(
        f"done: files={stats.files} read={stats.read} matched={stats.matched} "
        f"inserted={stats.inserted} skipped_malformed={stats.skipped_malformed} "
        f"skipped_filter={stats.skipped_filter}"
    )
    return 0


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
