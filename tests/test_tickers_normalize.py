"""Tests for company-name normalization."""

from __future__ import annotations

from tickers.normalize import normalize_company_name


def test_normalize_strips_common_suffixes() -> None:
    assert normalize_company_name("Apple Inc. Common Stock") == "apple"
    assert normalize_company_name("Alcoa Corporation Common Stock") == "alcoa"
    assert normalize_company_name("Agilent Technologies, Inc. Common Stock") == (
        "agilent technologies"
    )


def test_normalize_strips_class_shares() -> None:
    assert "class" not in normalize_company_name(
        "Alphabet Inc. - Class A Common Stock"
    )
    assert normalize_company_name("Foo Holdings Ltd.") == "foo"
