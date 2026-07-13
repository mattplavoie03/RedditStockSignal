"""Ingestion throughput metrics and heartbeat logging."""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL_SEC = 3600


class IngestMetrics:
    """Counters for ingestion heartbeat and API rate tracking."""

    def __init__(self) -> None:
        self._posts = 0
        self._comments = 0
        self._api_requests = 0
        self._window_start = time.monotonic()

    def record_posts(self, count: int) -> None:
        self._posts += count

    def record_comments(self, count: int) -> None:
        self._comments += count

    def record_api_request(self) -> None:
        self._api_requests += 1

    def snapshot_and_reset(self) -> tuple[int, int, int, float]:
        posts = self._posts
        comments = self._comments
        api_requests = self._api_requests
        elapsed = time.monotonic() - self._window_start
        self._posts = 0
        self._comments = 0
        self._api_requests = 0
        self._window_start = time.monotonic()
        return posts, comments, api_requests, elapsed


async def run_heartbeat(metrics: IngestMetrics) -> None:
    """Log rows ingested per hour and actual Reddit API request rate."""
    logger.info("starting ingestion heartbeat")
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        posts, comments, api_requests, elapsed = metrics.snapshot_and_reset()
        hours = max(elapsed / 3600, 1e-9)
        minutes = max(elapsed / 60, 1e-9)
        qpm = api_requests / minutes
        logger.info(
            "heartbeat: posts/hr=%.1f comments/hr=%.1f api_requests=%s qpm=%.1f (window %.0fs)",
            posts / hours,
            comments / hours,
            api_requests,
            qpm,
            elapsed,
        )
        if qpm > 80:
            logger.error("heartbeat alert: Reddit API rate %.1f QPM exceeds safe headroom (target <100)", qpm)
        if posts == 0 and comments == 0:
            logger.error("heartbeat alert: zero rows ingested in the last hour")
