#!/usr/bin/env python3
"""Run Reddit ingestion pollers."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.runner import configure_logging, run_ingest


def main() -> int:
    configure_logging()
    asyncio.run(run_ingest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
