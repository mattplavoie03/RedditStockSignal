"""create tickers and ticker_names tables

Revision ID: 005
Revises: 004
Create Date: 2026-07-22

"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "005"
down_revision: Union[str, Sequence[str], None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tickers",
        sa.Column("symbol", sa.Text(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("exchange", sa.Text(), nullable=True),
        sa.Column("is_etf", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("ticker_source", sa.Text(), nullable=False, server_default="nasdaq_current"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "ticker_source IN ('nasdaq_current', 'corpus_mined', 'manual')",
            name="ck_tickers_ticker_source",
        ),
    )
    op.create_table(
        "ticker_names",
        sa.Column("normalized_name", sa.Text(), primary_key=True),
        sa.Column("symbol", sa.Text(), sa.ForeignKey("tickers.symbol"), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_ticker_names_symbol", "ticker_names", ["symbol"])


def downgrade() -> None:
    op.drop_index("ix_ticker_names_symbol", table_name="ticker_names")
    op.drop_table("ticker_names")
    op.drop_table("tickers")
