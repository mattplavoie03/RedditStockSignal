"""Tests for exponential backoff."""

from __future__ import annotations

import pytest
from asyncprawcore.exceptions import ServerError, TooManyRequests

from ingest.backoff import _compute_delay, with_backoff


class FakeResponse:
    def __init__(self, status: int = 429, retry_after: str | None = None) -> None:
        self.status = status
        self.headers = {"retry-after": retry_after} if retry_after else {}
        self.text = ""


@pytest.mark.asyncio
async def test_with_backoff_retries_then_succeeds() -> None:
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ServerError(FakeResponse(status=503))
        return "ok"

    result = await with_backoff(flaky, max_attempts=5, base_delay_sec=0.01, operation_name="test")
    assert result == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_with_backoff_raises_after_max_attempts() -> None:
    async def always_fail() -> None:
        raise ServerError(FakeResponse(status=503))

    with pytest.raises(ServerError):
        await with_backoff(always_fail, max_attempts=2, base_delay_sec=0.01)


def test_compute_delay_honors_retry_after() -> None:
    exc = TooManyRequests(FakeResponse(retry_after="10"))
    assert _compute_delay(exc, attempt=1, base_delay_sec=1.0, max_delay_sec=120.0) == 10.0
