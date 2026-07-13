"""Exponential backoff with jitter for transient Reddit API errors."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from asyncprawcore.exceptions import Forbidden, RequestException, ResponseException, ServerError, TooManyRequests

logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (TooManyRequests, ServerError, RequestException)
FORBIDDEN_BACKOFF_SEC = (300, 900, 3600)  # 5 min → 15 min → 60 min (cap)

T = TypeVar("T")


async def with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 6,
    base_delay_sec: float = 1.0,
    max_delay_sec: float = 120.0,
    operation_name: str = "reddit request",
) -> T:
    """Run an async operation with backoff on transient failures.

    HTTP 403 (Forbidden) is treated as a temporary Reddit block: sleep with escalating
    delays and retry indefinitely rather than crashing the process.
    """
    attempt = 0
    forbidden_blocks = 0
    while True:
        attempt += 1
        try:
            return await operation()
        except Forbidden:
            delay = FORBIDDEN_BACKOFF_SEC[min(forbidden_blocks, len(FORBIDDEN_BACKOFF_SEC) - 1)]
            forbidden_blocks += 1
            logger.error(
                "Reddit 403 block on %s; sleeping %.0fs before retry (block #%s)",
                operation_name,
                delay,
                forbidden_blocks,
            )
            await asyncio.sleep(delay)
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


def forbidden_backoff_delay(block_number: int) -> float:
    """Return sleep duration for the Nth consecutive 403 block (1-indexed)."""
    index = min(max(block_number, 1) - 1, len(FORBIDDEN_BACKOFF_SEC) - 1)
    return float(FORBIDDEN_BACKOFF_SEC[index])


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
