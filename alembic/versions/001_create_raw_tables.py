"""create raw_posts and raw_comments hypertables

Revision ID: 001
Revises:
Create Date: 2026-07-13

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "raw_posts",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("subreddit", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("selftext", sa.Text(), nullable=True),
        sa.Column("score", sa.Integer(), nullable=True),
        sa.Column("num_comments", sa.Integer(), nullable=True),
        sa.Column("created_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "raw_comments",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("post_id", sa.Text(), nullable=False),
        sa.Column("subreddit", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("score", sa.Integer(), nullable=True),
        sa.Column("created_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_raw_posts_subreddit_created_utc",
        "raw_posts",
        ["subreddit", "created_utc"],
    )
    op.create_index(
        "ix_raw_comments_subreddit_created_utc",
        "raw_comments",
        ["subreddit", "created_utc"],
    )
    op.execute(
        "SELECT create_hypertable('raw_posts', 'created_utc', if_not_exists => TRUE)"
    )
    op.execute(
        "SELECT create_hypertable('raw_comments', 'created_utc', if_not_exists => TRUE)"
    )


def downgrade() -> None:
    op.drop_index("ix_raw_comments_subreddit_created_utc", table_name="raw_comments")
    op.drop_index("ix_raw_posts_subreddit_created_utc", table_name="raw_posts")
    op.drop_table("raw_comments")
    op.drop_table("raw_posts")
