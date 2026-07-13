"""Async Reddit client factory with rate-limit header logging."""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import asyncpraw
from asyncprawcore.requestor import Requestor

from config import Settings, get_settings
from ingest.metrics import IngestMetrics

logger = logging.getLogger(__name__)


def create_reddit(settings: Settings | None = None, metrics: IngestMetrics | None = None) -> asyncpraw.Reddit:
    cfg = settings or get_settings()

    class RateLimitLoggingRequestor(Requestor):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._metrics = metrics

        @asynccontextmanager
        async def request(
            self, *args: Any, timeout: float | None = None, **kwargs: Any
        ) -> AsyncGenerator[Any]:
            async with super().request(*args, timeout=timeout, **kwargs) as response:
                if self._metrics is not None:
                    self._metrics.record_api_request()
                logger.info(
                    "reddit rate-limit remaining=%s used=%s reset=%s status=%s",
                    response.headers.get("x-ratelimit-remaining"),
                    response.headers.get("x-ratelimit-used"),
                    response.headers.get("x-ratelimit-reset"),
                    response.status,
                )
                yield response

    return asyncpraw.Reddit(
        client_id=cfg.reddit_client_id,
        client_secret=cfg.reddit_client_secret,
        user_agent=cfg.reddit_user_agent,
        requestor_class=RateLimitLoggingRequestor,
    )
