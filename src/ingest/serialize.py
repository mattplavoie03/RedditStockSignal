"""Serialize Reddit API objects for storage."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_from_reddit(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def author_name(author: Any) -> str | None:
    if author is None:
        return None
    name = getattr(author, "name", None)
    return name if isinstance(name, str) else str(author)


def subreddit_name(subreddit: Any) -> str:
    if isinstance(subreddit, str):
        return subreddit.lower()
    display_name = getattr(subreddit, "display_name", None)
    if isinstance(display_name, str):
        return display_name.lower()
    return str(subreddit).lower()


def serialize_reddit_thing(thing: Any) -> dict[str, Any]:
    """Build a JSON-serializable snapshot of a Reddit object's API fields."""
    payload: dict[str, Any] = {}
    for key, value in thing.__dict__.items():
        if key.startswith("_"):
            continue
        payload[key] = _to_json_value(value)
    return payload


def _to_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _to_json_value(v) for k, v in value.items()}
    if hasattr(value, "STR_FIELD"):
        nested: dict[str, Any] = {"id": str(value)}
        for key, nested_value in value.__dict__.items():
            if key.startswith("_"):
                continue
            nested[key] = _to_json_value(nested_value)
        return nested
    return str(value)


def submission_to_row(submission: Any) -> dict[str, Any]:
    return {
        "id": submission.name,
        "subreddit": subreddit_name(submission.subreddit),
        "author": author_name(submission.author),
        "title": submission.title,
        "selftext": submission.selftext,
        "score": submission.score,
        "num_comments": submission.num_comments,
        "created_utc": utc_from_reddit(submission.created_utc),
        "raw": serialize_reddit_thing(submission),
    }


def comment_to_row(comment: Any, *, post_id: str, subreddit: str) -> dict[str, Any]:
    return {
        "id": comment.name,
        "post_id": post_id,
        "subreddit": subreddit,
        "author": author_name(comment.author),
        "body": comment.body,
        "score": comment.score,
        "created_utc": utc_from_reddit(comment.created_utc),
        "raw": serialize_reddit_thing(comment),
    }
