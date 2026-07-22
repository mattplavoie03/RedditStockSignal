"""enable TimescaleDB compression on raw hypertables

Revision ID: 004
Revises: 003
Create Date: 2026-07-17

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "004"
down_revision: Union[str, Sequence[str], None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE raw_posts SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'subreddit',
            timescaledb.compress_orderby = 'created_utc DESC'
        )
        """
    )
    op.execute(
        """
        ALTER TABLE raw_comments SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'subreddit',
            timescaledb.compress_orderby = 'created_utc DESC'
        )
        """
    )
    op.execute(
        "SELECT add_compression_policy('raw_posts', compress_after => INTERVAL '7 days')"
    )
    op.execute(
        "SELECT add_compression_policy('raw_comments', compress_after => INTERVAL '7 days')"
    )


def downgrade() -> None:
    op.execute("SELECT remove_compression_policy('raw_comments', if_exists => true)")
    op.execute("SELECT remove_compression_policy('raw_posts', if_exists => true)")
    op.execute("ALTER TABLE raw_comments SET (timescaledb.compress = false)")
    op.execute("ALTER TABLE raw_posts SET (timescaledb.compress = false)")
