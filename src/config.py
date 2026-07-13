"""Central configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True, slots=True)
class Settings:
    reddit_client_id: str
    reddit_client_secret: str
    reddit_user_agent: str
    database_url: str


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_settings() -> Settings:
    return Settings(
        reddit_client_id=_require("REDDIT_CLIENT_ID"),
        reddit_client_secret=_require("REDDIT_CLIENT_SECRET"),
        reddit_user_agent=_require("REDDIT_USER_AGENT"),
        database_url=_require("DATABASE_URL"),
    )
