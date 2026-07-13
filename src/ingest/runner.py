"""Ingestion runner: coordinates all pollers."""

from __future__ import annotations

import asyncio
import logging

from config import get_settings
from db.session import create_engine, create_session_factory
from ingest.comment_poller import run_comment_poller
from ingest.metrics import IngestMetrics, run_heartbeat
from ingest.post_poller import run_post_pollers
from ingest.reddit_client import create_reddit
from ingest.wsb_daily import WsbDailyState, run_wsb_daily_poller

logger = logging.getLogger(__name__)


async def run_ingest() -> None:
    """Run all ingestion workers until an unrecoverable error crashes the process."""
    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    metrics = IngestMetrics()
    wsb_state = WsbDailyState()

    reddit = create_reddit(settings, metrics)
    try:
        async with session_factory() as session:
            await wsb_state.load(session)

        async with asyncio.TaskGroup() as group:
            group.create_task(run_post_pollers(reddit, session_factory, metrics))
            group.create_task(run_comment_poller(reddit, session_factory, metrics, wsb_state=wsb_state))
            group.create_task(run_wsb_daily_poller(reddit, session_factory, metrics, wsb_state))
            group.create_task(run_heartbeat(metrics))
    finally:
        await reddit.close()
        await engine.dispose()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
