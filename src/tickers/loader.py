"""Download and upsert NASDAQ Trader symbol directories into tickers / ticker_names."""

from __future__ import annotations

import csv
import io
import logging
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.models import Ticker, TickerName
from tickers.normalize import normalize_company_name

logger = logging.getLogger(__name__)

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# otherlisted Exchange column → display name
_EXCHANGE_MAP = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Z": "BATS",
    "V": "IEX",
}


@dataclass(frozen=True)
class ListingRow:
    symbol: str
    name: str
    exchange: str
    is_etf: bool


def download_text(url: str, timeout: float = 60.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "reddit-signal/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("latin-1")


def _is_footer(line: str) -> bool:
    return line.upper().startswith("FILE CREATION TIME")


def parse_nasdaq_listed(text: str) -> list[ListingRow]:
    """Parse nasdaqlisted.txt (pipe-delimited). Skip Test Issue=Y and footer."""
    rows: list[ListingRow] = []
    reader = csv.DictReader(
        io.StringIO(_strip_footer(text)),
        delimiter="|",
    )
    for raw in reader:
        if (raw.get("Test Issue") or "").strip().upper() == "Y":
            continue
        symbol = (raw.get("Symbol") or "").strip().upper()
        name = (raw.get("Security Name") or "").strip()
        if not symbol or not name:
            continue
        is_etf = (raw.get("ETF") or "").strip().upper() == "Y"
        rows.append(ListingRow(symbol=symbol, name=name, exchange="NASDAQ", is_etf=is_etf))
    return rows


def parse_other_listed(text: str) -> list[ListingRow]:
    """Parse otherlisted.txt (pipe-delimited). Skip Test Issue=Y and footer."""
    rows: list[ListingRow] = []
    reader = csv.DictReader(
        io.StringIO(_strip_footer(text)),
        delimiter="|",
    )
    for raw in reader:
        if (raw.get("Test Issue") or "").strip().upper() == "Y":
            continue
        symbol = (raw.get("ACT Symbol") or "").strip().upper()
        name = (raw.get("Security Name") or "").strip()
        if not symbol or not name:
            continue
        exch_code = (raw.get("Exchange") or "").strip().upper()
        exchange = _EXCHANGE_MAP.get(exch_code, exch_code or "OTHER")
        is_etf = (raw.get("ETF") or "").strip().upper() == "Y"
        rows.append(ListingRow(symbol=symbol, name=name, exchange=exchange, is_etf=is_etf))
    return rows


def _strip_footer(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip() and not _is_footer(ln.strip())]
    return "\n".join(lines) + "\n"


def merge_listings(*groups: Iterable[ListingRow]) -> list[ListingRow]:
    """Dedupe by symbol; prefer first occurrence (NASDAQ file before otherlisted)."""
    by_symbol: dict[str, ListingRow] = {}
    for group in groups:
        for row in group:
            by_symbol.setdefault(row.symbol, row)
    return list(by_symbol.values())


async def upsert_tickers(
    session: AsyncSession,
    listings: list[ListingRow],
    *,
    ticker_source: str = "nasdaq_current",
    chunk_size: int = 3_000,
) -> int:
    if not listings:
        return 0
    now = datetime.now(timezone.utc)
    values: list[dict[str, Any]] = [
        {
            "symbol": row.symbol,
            "name": row.name,
            "exchange": row.exchange,
            "is_etf": row.is_etf,
            "is_active": True,
            "ticker_source": ticker_source,
            "updated_at": now,
        }
        for row in listings
    ]
    total = 0
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        stmt = insert(Ticker).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Ticker.symbol],
            set_={
                "name": stmt.excluded.name,
                "exchange": stmt.excluded.exchange,
                "is_etf": stmt.excluded.is_etf,
                "is_active": stmt.excluded.is_active,
                "ticker_source": stmt.excluded.ticker_source,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await session.execute(stmt)
        total += len(chunk)
    return total


async def upsert_ticker_names(session: AsyncSession, listings: list[ListingRow]) -> int:
    now = datetime.now(timezone.utc)
    values: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for row in listings:
        normalized = normalize_company_name(row.name)
        if not normalized or normalized in seen_names:
            continue
        seen_names.add(normalized)
        values.append(
            {
                "normalized_name": normalized,
                "symbol": row.symbol,
                "updated_at": now,
            }
        )
    if not values:
        return 0
    # Chunk to stay under asyncpg param limits
    inserted = 0
    chunk_size = 3000
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        stmt = insert(TickerName).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[TickerName.normalized_name],
            set_={
                "symbol": stmt.excluded.symbol,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await session.execute(stmt)
        inserted += len(chunk)
    return inserted


async def load_nasdaq_universe(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[int, int]:
    """Download both symbol directories and upsert tickers + ticker_names. Idempotent."""
    logger.info("downloading %s", NASDAQ_LISTED_URL)
    nasdaq_text = download_text(NASDAQ_LISTED_URL)
    logger.info("downloading %s", OTHER_LISTED_URL)
    other_text = download_text(OTHER_LISTED_URL)

    listings = merge_listings(
        parse_nasdaq_listed(nasdaq_text),
        parse_other_listed(other_text),
    )
    logger.info("parsed %s unique listings", len(listings))

    async with session_factory() as session:
        n_tickers = await upsert_tickers(session, listings)
        n_names = await upsert_ticker_names(session, listings)
        await session.commit()
    return n_tickers, n_names
