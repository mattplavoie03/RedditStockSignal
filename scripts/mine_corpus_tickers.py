#!/usr/bin/env python3
"""Mine corpus for candidate tickers missing from the current universe.

Scans raw_comments / raw_posts for $CASHTAG and all-caps tokens above a count
threshold, then writes any symbols NOT already in ``tickers`` to a review CSV.

Does NOT insert into the database — human review first (survivorship / delistings).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from config import get_settings
from db.session import create_engine, create_session_factory

logger = logging.getLogger(__name__)

DEFAULT_MIN_COUNT = 50
DEFAULT_OUT = Path("data/corpus_ticker_candidates.csv")

# $GME / $AAPL — 1–5 letters after dollar sign
CASHTAG_SQL = r"\$([A-Za-z]{1,5})\y"
# Standalone all-caps tokens 2–5 letters (Postgres word boundary)
ALLCAPS_SQL = r"\y([A-Z]{2,5})\y"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-count", type=int, default=DEFAULT_MIN_COUNT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2020, 1, 1),
        help="Only scan posts/comments on/after this UTC date (YYYY-MM-DD)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


async def mine_tokens(
    session: AsyncSession,
    *,
    min_count: int,
    start_date: date,
) -> list[tuple[str, int, int, str]]:
    """Return (symbol, cashtag_count, allcaps_count, evidence) for missing symbols."""
    start_ts = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    query = text(
        """
        WITH comment_cash AS (
            SELECT upper(m[1]) AS sym, count(*)::bigint AS n
            FROM raw_comments c
            CROSS JOIN LATERAL regexp_matches(c.body, :cashtag, 'g') AS m
            WHERE c.created_utc >= :start_date
              AND c.body IS NOT NULL
              AND c.body NOT IN ('[deleted]', '[removed]')
            GROUP BY 1
        ),
        comment_caps AS (
            SELECT m[1] AS sym, count(*)::bigint AS n
            FROM raw_comments c
            CROSS JOIN LATERAL regexp_matches(c.body, :allcaps, 'g') AS m
            WHERE c.created_utc >= :start_date
              AND c.body IS NOT NULL
              AND c.body NOT IN ('[deleted]', '[removed]')
            GROUP BY 1
        ),
        post_cash AS (
            SELECT upper(m[1]) AS sym, count(*)::bigint AS n
            FROM raw_posts p
            CROSS JOIN LATERAL regexp_matches(
                coalesce(p.title, '') || ' ' || coalesce(p.selftext, ''),
                :cashtag,
                'g'
            ) AS m
            WHERE p.created_utc >= :start_date
            GROUP BY 1
        ),
        post_caps AS (
            SELECT m[1] AS sym, count(*)::bigint AS n
            FROM raw_posts p
            CROSS JOIN LATERAL regexp_matches(
                coalesce(p.title, '') || ' ' || coalesce(p.selftext, ''),
                :allcaps,
                'g'
            ) AS m
            WHERE p.created_utc >= :start_date
            GROUP BY 1
        ),
        cash AS (
            SELECT sym, sum(n)::bigint AS n FROM (
                SELECT * FROM comment_cash
                UNION ALL
                SELECT * FROM post_cash
            ) u GROUP BY sym
        ),
        caps AS (
            SELECT sym, sum(n)::bigint AS n FROM (
                SELECT * FROM comment_caps
                UNION ALL
                SELECT * FROM post_caps
            ) u GROUP BY sym
        ),
        combined AS (
            SELECT
                coalesce(cash.sym, caps.sym) AS symbol,
                coalesce(cash.n, 0)::bigint AS cashtag_count,
                coalesce(caps.n, 0)::bigint AS allcaps_count
            FROM cash
            FULL OUTER JOIN caps ON cash.sym = caps.sym
        )
        SELECT
            c.symbol,
            c.cashtag_count,
            c.allcaps_count,
            CASE
                WHEN c.cashtag_count > 0 AND c.allcaps_count > 0 THEN 'cashtag+allcaps'
                WHEN c.cashtag_count > 0 THEN 'cashtag'
                ELSE 'allcaps'
            END AS evidence
        FROM combined c
        LEFT JOIN tickers t ON t.symbol = c.symbol
        WHERE t.symbol IS NULL
          AND (c.cashtag_count + c.allcaps_count) >= :min_count
          AND c.symbol ~ '^[A-Z]{1,5}$'
        ORDER BY (c.cashtag_count + c.allcaps_count) DESC, c.symbol
        """
    )
    await session.execute(text("SET statement_timeout = 0"))
    result = await session.execute(
        query,
        {
            "cashtag": CASHTAG_SQL,
            "allcaps": ALLCAPS_SQL,
            "start_date": start_ts,
            "min_count": min_count,
        },
    )
    return [(r[0], int(r[1]), int(r[2]), str(r[3])) for r in result.fetchall()]


def write_review_csv(path: Path, rows: list[tuple[str, int, int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ["symbol", "cashtag_count", "allcaps_count", "total_count", "evidence", "action"]
        )
        for symbol, cash_n, caps_n, evidence in rows:
            writer.writerow(
                [symbol, cash_n, caps_n, cash_n + caps_n, evidence, "review"]
            )


async def main_async(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        async with session_factory() as session:
            logger.info(
                "mining corpus (min_count=%s, start_date=%s) — this can take a while",
                args.min_count,
                args.start_date,
            )
            rows = await mine_tokens(
                session, min_count=args.min_count, start_date=args.start_date
            )
    finally:
        await engine.dispose()

    write_review_csv(args.out, rows)
    print(f"done: candidates={len(rows)} written_to={args.out}")
    print("NOTE: nothing was inserted into tickers — review the CSV first.")
    return 0


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
