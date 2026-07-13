"""Tests for Reddit object serialization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from ingest.serialize import serialize_reddit_thing, submission_to_row, utc_from_reddit


@dataclass
class FakeAuthor:
    name: str


@dataclass
class FakeSubreddit:
    display_name: str


class FakeSubmission:
    STR_FIELD = "id"

    def __init__(self) -> None:
        self.name = "t3_abc123"
        self.id = "abc123"
        self.subreddit = FakeSubreddit("stocks")
        self.author = FakeAuthor("tester")
        self.title = "Hello"
        self.selftext = "body"
        self.score = 10
        self.num_comments = 2
        self.created_utc = 1_700_000_000.0


def test_utc_from_reddit() -> None:
    assert utc_from_reddit(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_submission_to_row() -> None:
    row = submission_to_row(FakeSubmission())
    assert row["id"] == "t3_abc123"
    assert row["subreddit"] == "stocks"
    assert row["author"] == "tester"
    assert row["raw"]["title"] == "Hello"


def test_serialize_reddit_thing_skips_private_attrs() -> None:
    submission = FakeSubmission()
    submission._fetched = True  # noqa: SLF001
    payload = serialize_reddit_thing(submission)
    assert "_fetched" not in payload
    assert payload["title"] == "Hello"
