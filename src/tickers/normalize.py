"""Normalize issuer names for ticker_names lookup keys."""

from __future__ import annotations

import re

# Longer phrases first so "common stock" wins over lone tokens.
_NAME_SUFFIX_PATTERN = re.compile(
    r"""
    [,\s]+
    (
        class\s+[a-z0-9]+
      | series\s+[a-z0-9]+
      | ordinary\s+shares?
      | common\s+stock
      | common\s+shares?
      | preferred\s+stock
      | american\s+depositary\s+shares?
      | ads
      | adr
      | warrants?
      | rights?
      | units?
      | holdings?
      | holding\s+company
      | incorporated
      | corporation
      | company
      | limited
      | corp\.?
      | inc\.?
      | ltd\.?
      | llc\.?
      | plc\.?
      | co\.?
      | lp\.?
      | \&\s*co\.?
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MULTISPACE = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9\s&]")


def normalize_company_name(name: str) -> str:
    """Lowercase, strip corporate suffixes / share-class noise, collapse whitespace."""
    text = name.strip().lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    prev = None
    while prev != text:
        prev = text
        text = _NAME_SUFFIX_PATTERN.sub(" ", text).strip(" ,.-")
    text = _NON_ALNUM.sub(" ", text)
    text = _MULTISPACE.sub(" ", text).strip()
    return text
