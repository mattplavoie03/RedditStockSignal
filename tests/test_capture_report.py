"""Tests for capture report formatting."""

from __future__ import annotations

from datetime import date, datetime, timezone

from scripts.capture_report import HourlyRow, build_verdict, format_report, summarize_day


def _row(hour: int, *, capture: float, truncations: int, volume: int = 100) -> HourlyRow:
    return HourlyRow(
        hour=datetime(2026, 7, 13, hour, tzinfo=timezone.utc),
        comment_volume=volume,
        comments_in_db=4600,
        num_comments_reported=5000,
        capture_rate_pct=capture,
        truncations=truncations,
    )


def test_verdict_healthy_when_zero_truncations() -> None:
    summary = summarize_day([_row(14, capture=93.0, truncations=0)])
    assert build_verdict(summary) == "Verdict: zero truncations — poller is capturing everything fetchable."


def test_verdict_healthy_at_93pct_with_zero_truncations() -> None:
    output = format_report(date(2026, 7, 13), [_row(14, capture=93.0, truncations=0)])
    assert "93.0%" in output
    assert "zero truncations" in output
    assert "≥95%" not in output
    assert "monitor" not in output.lower()


def test_verdict_paginator_when_repeated_market_open_truncations() -> None:
    rows = [
        _row(13, capture=60.0, truncations=2),
        _row(14, capture=58.0, truncations=1),
    ]
    summary = summarize_day(rows)
    assert "repeated truncations during market hours" in build_verdict(summary)


def test_verdict_off_hours_truncations_only() -> None:
    summary = summarize_day([_row(3, capture=92.0, truncations=2)])
    assert "no truncations during market hours" in build_verdict(summary)
