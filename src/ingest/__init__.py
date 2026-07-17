"""Reddit ingestion: pollers and API client.

Live pollers are deprecated (API access revoked July 2026). Archive-first
ingestion lives in ``archive.loader``.
"""

from ingest.runner import run_ingest

__all__ = ["run_ingest"]