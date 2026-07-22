#!/usr/bin/env python3
"""Download NASDAQ Trader symbol directories into tickers / ticker_names."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from config import get_settings
from db.session import create_engine, create_session_factory
from tickers.loader import load_nasdaq_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        n_tickers, n_names = await load_nasdaq_universe(session_factory)
    finally:
        await engine.dispose()
    print(f"done: tickers_upserted={n_tickers} ticker_names_upserted={n_names}")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
