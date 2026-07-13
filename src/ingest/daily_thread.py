"""WSB daily discussion thread detection."""

from __future__ import annotations

import re

DAILY_THREAD_PATTERN = re.compile(
    r"(daily discussion|what are your moves|moves for)",
    re.IGNORECASE,
)


def is_daily_thread_title(title: str) -> bool:
    return bool(DAILY_THREAD_PATTERN.search(title))
