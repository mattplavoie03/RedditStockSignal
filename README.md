# Reddit Stock Signal

Reddit-based stock discovery: ingest posts/comments from finance subreddits, extract ticker mentions, detect anomalous mention velocity, and backtest predictive value.

## Setup

```bash
# Install dependencies (requires uv: https://docs.astral.sh/uv/)
uv sync

# Copy env template and fill in values
cp .env.example .env

# Start TimescaleDB
docker compose up -d

# Run migrations
uv run alembic upgrade head
```

## Data source (Arctic Shift archives)

Live Reddit API access is unavailable (client flagged; new-app registration is approval-gated as of July 2026). Ingestion is **archive-first** from [Arctic Shift](https://arctic-shift.photon-reddit.com/) dumps: JSONL (optionally `.zst`), one raw Reddit API payload object per line.

```bash
# Load comments or posts (kind auto-detects via presence of title)
uv run python scripts/load_archive.py \
  --files data/samples/r_smallstreetbets_comments.jsonl \
  --subreddits wallstreetbets stocks pennystocks smallstreetbets
```

Supports plain `.jsonl` and zstandard-compressed `.zst` (streamed; never loaded whole-file into memory). Re-runs are idempotent (`INSERT … ON CONFLICT DO NOTHING`).

### Archive caveats

1. **Score is as-of `retrieved_on`, not posting time.** Do not use `score` as a point-in-time feature without checking `retrieved_on − created_utc` staleness (both fields are preserved in `raw`).
2. **~10%+ of comments are `[removed]`/`[deleted]` with no body.** They produce no ticker mentions; because moderation targets spam/pumps, manipulation is slightly under-represented in the archive.
3. **Deleted authors are stored as NULL** (`author == "[deleted]"`) and cannot contribute to unique-author counts.

Live poller modules under `src/ingest/` remain in the tree but are deprecated.

## TimescaleDB compression

`raw_posts` and `raw_comments` use native compression (`segmentby=subreddit`, `orderby=created_utc DESC`) with a policy that compresses chunks older than 7 days.

```bash
# Compression settings (should list both hypertables)
docker compose exec -T db psql -U reddit_signal -d reddit_signal -c \
  "SELECT * FROM timescaledb_information.compression_settings;"

# Per-chunk compression status
docker compose exec -T db psql -U reddit_signal -d reddit_signal -c \
  "SELECT * FROM chunk_compression_stats('raw_comments');"

# Force-compress existing chunks (e.g. after a bulk archive load)
docker compose exec -T db psql -U reddit_signal -d reddit_signal -c \
  "SELECT compress_chunk(c, true) FROM show_chunks('raw_comments') c;"
docker compose exec -T db psql -U reddit_signal -d reddit_signal -c \
  "SELECT compress_chunk(c, true) FROM show_chunks('raw_posts') c;"
```

## Development

```bash
uv run pytest
```
