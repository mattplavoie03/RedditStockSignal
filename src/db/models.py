"""SQLAlchemy models for raw Reddit data."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawPost(Base):
    __tablename__ = "raw_posts"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    subreddit: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    selftext: Mapped[str | None] = mapped_column(Text)
    score: Mapped[int | None] = mapped_column(Integer)
    num_comments: Mapped[int | None] = mapped_column(Integer)
    created_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False)


class RawComment(Base):
    __tablename__ = "raw_comments"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    post_id: Mapped[str] = mapped_column(Text, nullable=False)
    subreddit: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[str | None] = mapped_column(Text)
    body: Mapped[str | None] = mapped_column(Text)
    score: Mapped[int | None] = mapped_column(Integer)
    created_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False)
