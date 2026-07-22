"""Tests for NASDAQ listing parsers (no network)."""

from __future__ import annotations

from tickers.loader import merge_listings, parse_nasdaq_listed, parse_other_listed

NASDAQ_SAMPLE = """\
Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N
TEST|Test Company|Q|Y|N|100|N|N
QQQ|Invesco QQQ Trust|G|N|N|100|Y|N
File Creation Time: 0716202617:02
"""

OTHER_SAMPLE = """\
ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
GME|GameStop Corporation Common Stock|N|GME|N|100|N|GME
ZZZ|Fake Test|N|ZZZ|N|100|Y|ZZZ
File Creation Time: 0716202617:02
"""


def test_parse_skips_test_issues_and_footer() -> None:
    nasdaq = parse_nasdaq_listed(NASDAQ_SAMPLE)
    other = parse_other_listed(OTHER_SAMPLE)
    symbols = {r.symbol for r in nasdaq}
    assert symbols == {"AAPL", "QQQ"}
    assert {r.symbol for r in other} == {"GME"}
    qqq = next(r for r in nasdaq if r.symbol == "QQQ")
    assert qqq.is_etf is True


def test_merge_prefers_first_source() -> None:
    a = parse_nasdaq_listed(NASDAQ_SAMPLE)
    # duplicate AAPL on other side should not override NASDAQ row
    other = parse_other_listed(
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
        "AAPL|Apple Other|N|AAPL|N|100|N|AAPL\n"
    )
    merged = merge_listings(a, other)
    aapl = next(r for r in merged if r.symbol == "AAPL")
    assert aapl.exchange == "NASDAQ"
    assert "Other" not in aapl.name
