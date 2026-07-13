"""create wsb_capture_stats table

Revision ID: 003
Revises: 002
Create Date: 2026-07-13

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "003"
down_revision: Union[str, Sequence[str], None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wsb_capture_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("thread_id", sa.Text(), nullable=False),
        sa.Column(
            "polled_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("num_comments_reported", sa.Integer(), nullable=True),
        sa.Column("comments_in_db_for_thread", sa.Integer(), nullable=False),
        sa.Column("fetched_this_cycle", sa.Integer(), nullable=False),
        sa.Column("was_truncated", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_wsb_capture_stats_polled_at_thread_id",
        "wsb_capture_stats",
        ["polled_at", "thread_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_wsb_capture_stats_polled_at_thread_id", table_name="wsb_capture_stats")
    op.drop_table("wsb_capture_stats")
