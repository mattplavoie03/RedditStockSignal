#!/usr/bin/env python3
"""Print WSB daily thread capture-rate scorecard for a given date."""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import DateTime, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from db.models import RawComment, WsbCaptureStats
from db.session import create_engine, create_session_factory


@dataclass(frozen=True, slots=True)
class DaySummary:
    total_volume: int
    total_truncations: int
    market_open_truncations: int
    market_open_hours_with_truncations: int
    avg_capture_pct: float | None
    min_capture_pct: float | None


# US cash session roughly 9:30–16:00 ET → 13:30–20:00 UTC (EDT); use 13–20 for the scorecard window.
MARKET_OPEN_UTC_HOURS = range(13, 21)


@dataclass(frozen=True, slots=True)
class HourlyRow:
    hour: datetime
    comment_volume: int
    comments_in_db: int
    num_comments_reported: int
    capture_rate_pct: float | None
    truncations: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WSB daily thread capture-rate report")
    parser.add_argument(
        "date",
        nargs="?",
        default=date.today().isoformat(),
        help="UTC date to report on (YYYY-MM-DD, default: today)",
    )
    return parser.parse_args()


def parse_utc_date(value: str) -> date:
    return date.fromisoformat(value)


def day_bounds_utc(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


async def build_report(session: AsyncSession, day: date) -> list[HourlyRow]:
    day_start, day_end = day_bounds_utc(day)
    thread_ids = await _thread_ids_for_day(session, day_start, day_end)
    if not thread_ids:
        return []

    volume_by_hour = await _hourly_comment_volume(session, thread_ids, day_start, day_end)
    capture_by_hour = await _hourly_capture_snapshots(session, day_start, day_end)
    truncations_by_hour = await _hourly_truncations(session, day_start, day_end)

    hours = sorted(set(volume_by_hour) | set(capture_by_hour) | set(truncations_by_hour))
    rows: list[HourlyRow] = []
    for hour in hours:
        in_db, reported = capture_by_hour.get(hour, (0, 0))
        rate = (in_db / reported * 100) if reported > 0 else None
        rows.append(
            HourlyRow(
                hour=hour,
                comment_volume=volume_by_hour.get(hour, 0),
                comments_in_db=in_db,
                num_comments_reported=reported,
                capture_rate_pct=rate,
                truncations=truncations_by_hour.get(hour, 0),
            )
        )
    return rows


async def _thread_ids_for_day(
    session: AsyncSession,
    day_start: datetime,
    day_end: datetime,
) -> list[str]:
    result = await session.execute(
        select(WsbCaptureStats.thread_id)
        .where(WsbCaptureStats.polled_at >= day_start)
        .where(WsbCaptureStats.polled_at < day_end)
        .distinct()
    )
    return list(result.scalars().all())


async def _hourly_comment_volume(
    session: AsyncSession,
    thread_ids: list[str],
    day_start: datetime,
    day_end: datetime,
) -> dict[datetime, int]:
    hour_bucket = func.date_trunc("hour", RawComment.created_utc).label("hour")
    result = await session.execute(
        select(hour_bucket, func.count())
        .where(RawComment.post_id.in_(thread_ids))
        .where(RawComment.created_utc >= day_start)
        .where(RawComment.created_utc < day_end)
        .group_by(hour_bucket)
        .order_by(hour_bucket)
    )
    return {hour: int(count) for hour, count in result.all()}


async def _hourly_capture_snapshots(
    session: AsyncSession,
    day_start: datetime,
    day_end: datetime,
) -> dict[datetime, tuple[int, int]]:
    hour_bucket = func.date_trunc("hour", WsbCaptureStats.polled_at).label("hour")
    ranked = (
        select(
            hour_bucket,
            WsbCaptureStats.thread_id,
            WsbCaptureStats.comments_in_db_for_thread,
            WsbCaptureStats.num_comments_reported,
            func.row_number()
            .over(
                partition_by=(hour_bucket, WsbCaptureStats.thread_id),
                order_by=WsbCaptureStats.polled_at.desc(),
            )
            .label("rn"),
        )
        .where(WsbCaptureStats.polled_at >= day_start)
        .where(WsbCaptureStats.polled_at < day_end)
        .subquery()
    )

    result = await session.execute(
        select(
            ranked.c.hour,
            func.sum(ranked.c.comments_in_db_for_thread),
            func.sum(ranked.c.num_comments_reported),
        )
        .where(ranked.c.rn == 1)
        .group_by(ranked.c.hour)
    )

    snapshots: dict[datetime, tuple[int, int]] = {}
    for hour, in_db, reported in result.all():
        snapshots[hour] = (int(in_db or 0), int(reported or 0))
    return snapshots


async def _hourly_truncations(
    session: AsyncSession,
    day_start: datetime,
    day_end: datetime,
) -> dict[datetime, int]:
    hour_bucket = cast(func.date_trunc("hour", WsbCaptureStats.polled_at), DateTime(timezone=True)).label(
        "hour"
    )
    result = await session.execute(
        select(hour_bucket, func.count())
        .where(WsbCaptureStats.polled_at >= day_start)
        .where(WsbCaptureStats.polled_at < day_end)
        .where(WsbCaptureStats.was_truncated.is_(True))
        .group_by(hour_bucket)
    )
    return {hour: int(count) for hour, count in result.all()}


def summarize_day(rows: list[HourlyRow]) -> DaySummary:
    rates = [row.capture_rate_pct for row in rows if row.capture_rate_pct is not None]
    market_rows = [row for row in rows if row.hour.hour in MARKET_OPEN_UTC_HOURS]
    return DaySummary(
        total_volume=sum(row.comment_volume for row in rows),
        total_truncations=sum(row.truncations for row in rows),
        market_open_truncations=sum(row.truncations for row in market_rows),
        market_open_hours_with_truncations=sum(1 for row in market_rows if row.truncations > 0),
        avg_capture_pct=(sum(rates) / len(rates)) if rates else None,
        min_capture_pct=min(rates) if rates else None,
    )


def build_verdict(summary: DaySummary) -> str:
    """Truncations are the signal; capture % is context only."""
    if summary.total_truncations == 0:
        return "Verdict: zero truncations — poller is capturing everything fetchable."

    if summary.market_open_truncations == 0:
        return (
            "Verdict: no truncations during market hours (UTC 13:00–20:59) — healthy; "
            f"off-hours truncations only ({summary.total_truncations} total)."
        )

    if summary.market_open_hours_with_truncations >= 2 or summary.market_open_truncations >= 3:
        return (
            "Verdict: repeated truncations during market hours — need /api/morechildren paginator."
        )

    return (
        "Verdict: isolated truncation(s) during market hours — watch the next open; "
        "capture % alone is not actionable."
    )


def format_report(day: date, rows: list[HourlyRow]) -> str:
    lines = [
        f"WSB Capture Report — {day.isoformat()} (UTC)",
        "",
        "Hour (UTC) | Volume | In DB | Reddit | Capture % | Truncations",
        "-----------+--------+-------+--------+-------------+------------",
    ]

    if not rows:
        lines.append("(no data — is the ingest poller running?)")
        return "\n".join(lines)

    for row in rows:
        rate_str = f"{row.capture_rate_pct:5.1f}" if row.capture_rate_pct is not None else "  n/a"
        trunc_flag = " !" if row.truncations > 0 and row.hour.hour in MARKET_OPEN_UTC_HOURS else "  "
        lines.append(
            f"{row.hour:%H:%M}      | {row.comment_volume:6d} | {row.comments_in_db:5d} | "
            f"{row.num_comments_reported:6d} | {rate_str}%      | {row.truncations:9d}{trunc_flag}"
        )

    summary = summarize_day(rows)
    lines.append("")
    lines.append(
        f"Day summary: {summary.total_volume} comments created | "
        f"{summary.total_truncations} truncations "
        f"({summary.market_open_truncations} during market hours)"
    )
    if summary.avg_capture_pct is not None:
        lines.append(
            "Capture sanity check: "
            f"avg {summary.avg_capture_pct:.1f}% | min {summary.min_capture_pct:.1f}% "
            "(expect ~90–97%; shortfall vs Reddit is mostly deleted/removed comments)"
        )
    lines.append(build_verdict(summary))

    return "\n".join(lines)


async def main_async(day: date) -> int:
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        async with session_factory() as session:
            rows = await build_report(session, day)
        print(format_report(day, rows))
        return 0
    finally:
        await engine.dispose()


def main() -> int:
    args = parse_args()
    day = parse_utc_date(args.date)
    return asyncio.run(main_async(day))


if __name__ == "__main__":
    raise SystemExit(main())
