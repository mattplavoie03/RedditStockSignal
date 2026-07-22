"""Alias detection and curated equity allowlists for candidate review."""

from __future__ import annotations

# Common Reddit misspellings / shorthand → canonical listed ticker.
KNOWN_ALIASES: dict[str, str] = {
    "APPL": "AAPL",
    "TESLA": "TSLA",
    "AMZ": "AMZN",
    "AMZNQ": "AMZN",
    "NOKIA": "NOK",
    "LUCID": "LCID",
    "NIKE": "NKE",
    "MFST": "MSFT",
    "MSFTQ": "MSFT",
    "TSMC": "TSM",
    "BOFA": "BAC",
    "BRKB": "BRK.B",
    "BRKA": "BRK.A",
    "BRK": "BRK.B",
    "GOOG": "GOOGL",
    "ALPH": "GOOGL",
    "FACEBK": "META",
    "FB": "META",
}

# Human-confirmed delisted / SPAC / ADR / OTC names from corpus review.
CONFIDENT_EQUITY: frozenset[str] = frozenset(
    {
        # high-cashtag confident
        "SEARS",
        "CCIV",
        "RDBX",
        "DEAC",
        "NGA",
        "CCCX",
        "IPOE",
        "IPOC",
        "IPOB",
        "VGAC",
        "LGVW",
        "GIK",
        "STPK",
        "AABB",
        "ENZC",
        "RDS",
        "MMTLP",
        "EHANG",
        "DRYS",
        # penny / OTC reals
        "ABML",
        "LTNC",
        "NOVC",
        "TSNP",
        "CLIS",
        "INND",
        "SGMD",
        "ALYI",
        "HMBL",
        "SIRC",
        "NSAV",
        "ILUS",
        "GTEH",
        "BLSP",
        "DECN",
        "CYBL",
        "SANP",
    }
)


def levenshtein_one(a: str, b: str) -> bool:
    """True if edit distance is exactly 1 (insert/delete/substitute)."""
    if a == b:
        return False
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la > lb:
        a, b = b, a
        la, lb = lb, la
    # a shorter or equal
    if la == lb:
        return sum(x != y for x, y in zip(a, b, strict=True)) == 1
    # one insert into a to make b
    i = j = diffs = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            diffs += 1
            if diffs > 1:
                return False
            j += 1
    return True


def find_alias_target(symbol: str, universe: set[str]) -> str | None:
    """Return canonical ticker from the curated alias map only.

    Automatic 1-edit matching is too noisy on OTC-heavy corpora (BANG→LBGJ,
    JUMP→PUMP, FUCKU→PUCKU). Misspellings belong in ``KNOWN_ALIASES``.
    """
    _ = universe  # reserved for future constrained matching
    return KNOWN_ALIASES.get(symbol)


def company_name_alias(symbol: str, name_index: dict[str, str]) -> str | None:
    """If symbol equals a normalized single-token company name, return its ticker."""
    return name_index.get(symbol.lower())
