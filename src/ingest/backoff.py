"""Exponential backoff with jitter for transient Reddit API errors."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from asyncprawcore.exceptions import RequestException, ResponseException, ServerError, TooManyRequests

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (TooManyRequests, ServerError, RequestException)

T = TypeVar("T")


async def with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 6,
    base_delay_sec: float = 1.0,
    max_delay_sec: float = 120.0,
    operation_name: str = "reddit request",
) -> T:
    """Run an async operation with exponential backoff on transient failures."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return await operation()
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt >= max_attempts:
                logger.error("%s failed after %s attempts", operation_name, attempt)
                raise

            delay = _compute_delay(exc, attempt, base_delay_sec, max_delay_sec)
            logger.warning(
                "%s attempt %s/%s failed (%s); retrying in %.1fs",
                operation_name,
                attempt,
                max_attempts,
                exc.__class__.__name__,
                delay,
            )
            await asyncio.sleep(delay)


def _compute_delay(
    exc: BaseException,
    attempt: int,
    base_delay_sec: float,
    max_delay_sec: float,
) -> float:
    if isinstance(exc, TooManyRequests) and isinstance(exc, ResponseException):
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            try:
                return min(max(float(retry_after), base_delay_sec), max_delay_sec)
            except (TypeError, ValueError):
                pass

    exponential = min(base_delay_sec * (2 ** (attempt - 1)), max_delay_sec)
    jitter = random.uniform(0, exponential * 0.25)
    return exponential + jitter
