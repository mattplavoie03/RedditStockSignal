# Reddit Stock Signal

Reddit-based stock discovery: ingest posts/comments from finance subreddits, extract ticker mentions, detect anomalous mention velocity, and backtest predictive value.

See [reddit-signal-plan.md](reddit-signal-plan.md) for the full build plan.

## Setup

```bash
# Install dependencies (requires uv: https://docs.astral.sh/uv/)
uv sync

# Copy env template and fill in Reddit credentials
cp .env.example .env

# Start TimescaleDB
docker compose up -d

# Run migrations
uv run alembic upgrade head

# Verify Reddit API auth
uv run python scripts/reddit_smoke_test.py
```

### Reddit app registration

1. Go to https://www.reddit.com/prefs/apps and create a **script** app.
2. Copy the client ID (under the app name) and secret into `.env`.
3. Set `REDDIT_USER_AGENT` to `platform:reddit-signal:v0.1 (by /u/yourusername)`.

## Development

```bash
uv run pytest
```
