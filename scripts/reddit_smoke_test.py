#!/usr/bin/env python3
"""Smoke test: fetch one post from r/stocks via asyncpraw."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import asyncpraw

from config import get_settings


async def main() -> int:
    settings = get_settings()
    reddit = asyncpraw.Reddit(
        client_id=settings.reddit_client_id,
        client_secret=settings.reddit_client_secret,
        user_agent=settings.reddit_user_agent,
    )
    try:
        subreddit = await reddit.subreddit("stocks")
        async for submission in subreddit.new(limit=1):
            print(f"OK: fetched post {submission.id!r} — {submission.title!r}")
            return 0
        print("ERROR: no posts returned from r/stocks", file=sys.stderr)
        return 1
    finally:
        await reddit.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
