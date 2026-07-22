#!/usr/bin/env python3
"""Promote classified corpus candidates into tickers as ticker_source=corpus_mined.

Reads:
  - data/candidates_confirmed.csv
  - data/candidates_review_foreign.csv  (filtered; false positives → stoplist)

Does not overwrite existing nasdaq_current / manual rows (ON CONFLICT DO NOTHING).
Prints foreign-suffix drops for eyeballing, then verifies DB counts.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from db.models import Ticker, TickerName
from db.session import create_engine, create_session_factory
from tickers.loader import ListingRow
from tickers.normalize import normalize_company_name
from tickers.stoplist import SLANG_STOPLIST, should_drop_foreign_suffix

logger = logging.getLogger(__name__)

CONFIRMED_CSV = Path("data/candidates_confirmed.csv")
FOREIGN_CSV = Path("data/candidates_review_foreign.csv")
STOPLIST_CSV = Path("data/candidates_stoplist.csv")
DROPS_CSV = Path("data/candidates_foreign_dropped.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Filter and print only; do not write CSVs or DB",
    )
    return parser.parse_args()


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str] | None = None) -> None:
    if not rows and fieldnames is None:
        path.write_text("", encoding="utf-8")
        return
    fields = fieldnames or list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def filter_foreign(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    keeps: list[dict[str, str]] = []
    drops: list[dict[str, str]] = []
    for row in rows:
        drop, reason = should_drop_foreign_suffix(
            row["symbol"],
            cashtag_count=int(row["cashtag_count"]),
            allcaps_count=int(row["allcaps_count"]),
        )
        if drop:
            drops.append(
                {
                    **row,
                    "bucket": "stoplist",
                    "reason": f"foreign_suffix_false_positive:{reason}",
                    "sources": "foreign_suffix_filter",
                    "matched_names": "",
                    "action": "ignore",
                }
            )
        else:
            keeps.append(row)
    return keeps, drops


def row_to_listing(row: dict[str, str]) -> ListingRow:
    symbol = row["symbol"].strip().upper()
    name = (row.get("matched_names") or "").split("|")[0].strip()
    if not name:
        reason = row.get("reason") or "corpus"
        name = f"Corpus-mined ({reason})"
    exchange = "CORPUS"
    if "bankruptcy_Q" in (row.get("reason") or ""):
        exchange = "OTC-BANKRUPTCY"
    elif "suffix_adr_Y" in (row.get("reason") or "") or "suffix_foreign_F" in (
        row.get("reason") or ""
    ):
        exchange = "OTC-FOREIGN"
    return ListingRow(symbol=symbol, name=name, exchange=exchange, is_etf=False)


async def upsert_corpus_mined(
    session: AsyncSession,
    listings: list[ListingRow],
    *,
    chunk_size: int = 3_000,
) -> int:
    """Insert corpus_mined tickers; never overwrite nasdaq_current / manual."""
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
            "ticker_source": "corpus_mined",
            "updated_at": now,
        }
        for row in listings
    ]
    inserted = 0
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        stmt = insert(Ticker).values(chunk)
        stmt = stmt.on_conflict_do_nothing(index_elements=[Ticker.symbol])
        result = await session.execute(stmt)
        # asyncpg may not expose rowcount reliably for DO NOTHING; recount below
        inserted += result.rowcount or 0
    return inserted


async def upsert_corpus_names(session: AsyncSession, listings: list[ListingRow]) -> int:
    """Insert ticker_names for corpus rows; never steal existing name mappings."""
    now = datetime.now(timezone.utc)
    values: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in listings:
        normalized = normalize_company_name(row.name)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(
            {
                "normalized_name": normalized,
                "symbol": row.symbol,
                "updated_at": now,
            }
        )
    if not values:
        return 0
    inserted = 0
    chunk_size = 3000
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        stmt = insert(TickerName).values(chunk)
        stmt = stmt.on_conflict_do_nothing(index_elements=[TickerName.normalized_name])
        result = await session.execute(stmt)
        inserted += result.rowcount or 0
    return inserted


def merge_stoplist_csv(drops: list[dict[str, str]]) -> int:
    """Append foreign drops into candidates_stoplist.csv (deduped by symbol)."""
    existing = _load_csv(STOPLIST_CSV) if STOPLIST_CSV.exists() else []
    by_sym = {r["symbol"]: r for r in existing}
    added = 0
    for row in drops:
        if row["symbol"] not in by_sym:
            added += 1
        by_sym[row["symbol"]] = row
    merged = sorted(by_sym.values(), key=lambda r: (-int(r["cashtag_count"]), r["symbol"]))
    fields = list(merged[0].keys()) if merged else [
        "symbol",
        "cashtag_count",
        "allcaps_count",
        "total_count",
        "evidence",
        "bucket",
        "reason",
        "sources",
        "matched_names",
        "action",
    ]
    _write_csv(STOPLIST_CSV, merged, fields)
    return added


async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    confirmed = _load_csv(CONFIRMED_CSV)
    foreign_raw = _load_csv(FOREIGN_CSV)
    foreign_keep, foreign_drops = filter_foreign(foreign_raw)

    print(
        f"foreign filter: input={len(foreign_raw)} keep={len(foreign_keep)} "
        f"drop={len(foreign_drops)}"
    )
    print("--- DROPPED (→ stoplist) ---")
    for row in foreign_drops:
        print(
            f"  {row['symbol']:8} cash={row['cashtag_count']:>5} "
            f"caps={row['allcaps_count']:>6}  {row['reason']}"
        )
    if not foreign_drops:
        print("  (none)")

    promote_rows = confirmed + foreign_keep
    # Dedupe by symbol; prefer confirmed over foreign if both appear
    by_sym: dict[str, dict[str, str]] = {}
    for row in promote_rows:
        by_sym.setdefault(row["symbol"].upper(), row)
    listings = [row_to_listing(r) for r in by_sym.values()]

    print(
        f"promote: confirmed={len(confirmed)} foreign_keep={len(foreign_keep)} "
        f"unique={len(listings)} (stoplist_symbols={len(SLANG_STOPLIST)})"
    )

    if args.dry_run:
        print("dry-run: no DB/CSV writes")
        return 0

    _write_csv(DROPS_CSV, foreign_drops)
    _write_csv(FOREIGN_CSV, foreign_keep)
    n_stoplist_added = merge_stoplist_csv(foreign_drops)
    print(f"stoplist CSV: +{n_stoplist_added} foreign drops (file={STOPLIST_CSV})")

    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        async with session_factory() as session:
            before = (
                await session.execute(
                    select(Ticker.ticker_source, func.count())
                    .group_by(Ticker.ticker_source)
                    .order_by(Ticker.ticker_source)
                )
            ).all()
            print("DB before:", {src: n for src, n in before})

            n_attempt = await upsert_corpus_mined(session, listings)
            n_names = await upsert_corpus_names(session, listings)
            await session.commit()

            after = (
                await session.execute(
                    select(Ticker.ticker_source, func.count())
                    .group_by(Ticker.ticker_source)
                    .order_by(Ticker.ticker_source)
                )
            ).all()
            after_map = {src: n for src, n in after}
            print(
                f"done: insert_rowcount={n_attempt} ticker_names_inserted={n_names}"
            )
            print("DB after:", after_map)

            mined = after_map.get("corpus_mined", 0)
            print(
                f"verify: corpus_mined={mined} "
                f"(promote unique={len(listings)}; existing nasdaq_current skipped)"
            )

            symbols = [L.symbol for L in listings]
            present = (
                await session.execute(
                    select(func.count()).select_from(Ticker).where(Ticker.symbol.in_(symbols))
                )
            ).scalar_one()
            print(f"verify: {present}/{len(symbols)} promote symbols present in tickers")

            # Sanity: none of the drops should be corpus_mined
            drop_syms = [r["symbol"] for r in foreign_drops]
            if drop_syms:
                bad = (
                    await session.execute(
                        select(Ticker.symbol).where(
                            Ticker.symbol.in_(drop_syms),
                            Ticker.ticker_source == "corpus_mined",
                        )
                    )
                ).scalars().all()
                print(f"verify: dropped symbols in corpus_mined={list(bad) or 'none'}")
    finally:
        await engine.dispose()
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
