"""Tests for ingestion metrics and rate tracking."""

from __future__ import annotations

from ingest.metrics import IngestMetrics


def test_metrics_track_api_requests() -> None:
    metrics = IngestMetrics()
    metrics.record_api_request()
    metrics.record_api_request()
    metrics.record_posts(5)
    metrics.record_comments(10)

    posts, comments, api_requests, elapsed = metrics.snapshot_and_reset()
    assert posts == 5
    assert comments == 10
    assert api_requests == 2
    assert elapsed >= 0

    posts2, comments2, api_requests2, _ = metrics.snapshot_and_reset()
    assert posts2 == 0
    assert comments2 == 0
    assert api_requests2 == 0
