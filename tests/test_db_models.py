"""Tests for database models."""

from __future__ import annotations

from db.models import Base, RawComment, RawPost


def test_raw_post_table_name() -> None:
    assert RawPost.__tablename__ == "raw_posts"


def test_raw_comment_table_name() -> None:
    assert RawComment.__tablename__ == "raw_comments"


def test_models_registered_on_base() -> None:
    tables = set(Base.metadata.tables)
    assert tables == {"raw_posts", "raw_comments"}
