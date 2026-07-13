# Reddit Stock Signal — Build Plan

A step-by-step plan for building a Reddit-based stock discovery tool: ingest posts/comments from finance subreddits, extract ticker mentions, detect anomalous mention velocity, and backtest whether the signal has any predictive value.

**Stack:** Python 3.12 · asyncpraw · Postgres + TimescaleDB (Docker) · pandas/scipy · VADER (then LLM) · yfinance · systemd/Docker for scheduling · Streamlit for the eventual dashboard.

**Working style with Cursor:** Each phase below ends with a "Cursor prompt seed" — a description you can paste into Cursor's chat to scaffold that phase. Keep a `.cursor/rules` file (contents in Phase 0) so the agent stays consistent across sessions. Review every generated migration and every piece of rate-limit logic by hand; those are the two places agent-generated code will quietly hurt you.

---

## Phase 0 — Project setup (evening 1)

### 0.1 Repo and environment
- [ ] Create repo `reddit-signal` with this layout:
  ```
  reddit-signal/
  ├── .cursor/rules
  ├── docker-compose.yml
  ├── pyproject.toml          # use uv or poetry
  ├── .env.example
  ├── src/
  │   ├── ingest/             # poller, reddit client
  │   ├── extract/            # ticker extraction
  │   ├── signal/             # anomaly detection
  │   ├── backtest/           # evaluation
  │   └── db/                 # models, migrations
  └── tests/
  ```
- [ ] `uv init`, add deps: `asyncpraw`, `asyncpg`, `sqlalchemy[asyncio]`, `alembic`, `pandas`, `scipy`, `vaderSentiment`, `yfinance`, `python-dotenv`, `pytest`, `pytest-asyncio`.
- [ ] `.env` for secrets (client id/secret, DB URL). Never commit it. Commit `.env.example`.

### 0.2 Cursor rules file
Create `.cursor/rules` (or `.cursorrules`) with project conventions so every agent session inherits them:

```
- Python 3.12, async-first. Use asyncpraw, never praw.
- All DB access through SQLAlchemy async sessions; migrations via Alembic only.
- Never hardcode credentials; read from environment via a single config module.
- All timestamps stored as UTC. Store both created_utc (Reddit's) and ingested_at (ours).
- Reddit API: respect rate limit headers, exponential backoff on 429/5xx,
  descriptive user_agent from env.
- Raw Reddit JSON is immutable once stored; extraction writes to separate tables
  and must be re-runnable over history.
- Every module gets a test. Ticker extraction changes require re-running
  tests/fixtures/labeled_mentions.jsonl and reporting precision/recall.
```

### 0.3 Reddit app registration
- [ ] reddit.com/prefs/apps → create app → type **script**.
- [ ] Note client_id (under the app name) and secret.
- [ ] User agent format: `platform:reddit-signal:v0.1 (by /u/yourusername)`.
- [ ] Confirm auth works with a 5-line asyncpraw smoke test (fetch 1 post from r/stocks).

### 0.4 Database up
- [ ] `docker-compose.yml` with `timescale/timescaledb:latest-pg16`, volume-mounted data dir, port 5432.
- [ ] Initialize Alembic. First migration: the `raw_posts` and `raw_comments` tables (schema in Phase 1).

**Cursor prompt seed:** "Scaffold a Python 3.12 project per the layout in reddit-signal-plan.md Phase 0. docker-compose for TimescaleDB, Alembic setup, config module reading from .env, and an asyncpraw smoke test script."

---

## Phase 1 — Ingestion (weekend 1)

Goal: raw posts and comments flowing into Postgres continuously. **No processing yet.** Raw-first matters because your extraction logic will be wrong several times and you want to re-run it over stored history instead of re-collecting.

### 1.1 Schema
```sql
raw_posts (
  id            text primary key,        -- reddit fullname, e.g. t3_abc123
  subreddit     text not null,
  author        text,                    -- null if deleted
  title         text,
  selftext      text,
  score         int,
  num_comments  int,
  created_utc   timestamptz not null,
  ingested_at   timestamptz not null default now(),
  raw           jsonb not null           -- full API payload
);
raw_comments (
  id            text primary key,        -- t1_...
  post_id       text not null,
  subreddit     text not null,
  author        text,
  body          text,
  score         int,
  created_utc   timestamptz not null,
  ingested_at   timestamptz not null default now(),
  raw           jsonb not null
);
-- make both hypertables on created_utc; index on (subreddit, created_utc)
poller_state (
  key           text primary key,        -- e.g. 'wsb_daily_last_comment'
  value         text,
  updated_at    timestamptz
);
```

### 1.2 Subreddit poller
- [ ] Target subs: `wallstreetbets, stocks, pennystocks, smallstreetbets, investing, StockMarket, Shortsqueeze, options`.
- [ ] Async loop per sub: fetch `/new` (limit=100), upsert into `raw_posts` (ON CONFLICT DO UPDATE for score/num_comments), sleep, repeat.
- [ ] Poll interval: every 2–3 min for WSB, every 5–10 min for slower subs. Total budget must stay well under 100 QPM — instrument `x-ratelimit-remaining` and log it.
- [ ] Retry with exponential backoff + jitter on 429/5xx (`prawcore.exceptions`). Crash-only design: process dies on unrecoverable error, systemd/Docker restarts it, upserts make re-ingestion harmless.

### 1.3 Comment ingestion — the hard part
- [ ] For normal posts: fetch comments once when post is ~1h old and again at ~24h (scores/discussion stabilize). Expand only top-level + second-level; **never** `replace_more(limit=None)`.
- [ ] **WSB daily discussion thread:** special-cased worker. Find today's daily thread (stickied), poll its comments every ~5 min *incrementally* — track last-seen comment id in `poller_state`, fetch only newer. This single thread is where most mentions live; it deserves its own module.

### 1.4 Run it
- [ ] Deploy as a systemd unit or `restart: always` Docker service on your machine or a $6 VPS.
- [ ] Add a heartbeat: log rows-ingested-per-hour; alert (even just a Discord webhook) if it hits zero.
- [ ] **Let it run for a full week before Phase 2.** You need baseline history anyway.

**Sanity checks before moving on:** posts from all 8 subs present · `created_utc` vs `ingested_at` lag under 5 min for WSB · daily-thread comments in the thousands per day · zero duplicate ids.

**Cursor prompt seed:** "Implement the ingestion layer per Phase 1: asyncpraw pollers for the listed subs writing raw JSON to the raw_posts/raw_comments hypertables, plus a special incremental poller for the WSB daily thread using poller_state. Crash-only with backoff."

---

## Phase 2 — Ticker extraction (weekends 2–3)

Goal: `mentions` table with high precision. This phase determines whether the whole project works. Naive matching produces garbage — `IT, ALL, ON, A, SO, AI, DD, CEO, IV, NOW, OPEN` are all real tickers and all common WSB vocabulary.

### 2.1 Ticker universe
- [ ] Download NASDAQ Trader symbol directory files (nasdaqlisted.txt, otherlisted.txt — free, updated nightly). Load into a `tickers` table: symbol, name, exchange, is_etf.
- [ ] Build a company-name → ticker map for the top ~1000 by volume ("Palantir" → PLTR). Many mentions never use the symbol.
- [ ] Refresh weekly via a small job.

### 2.2 Three-layer extractor
```
Layer 1: $CASHTAG regex           → confidence HIGH, accept
Layer 2: \b[A-Z]{1,5}\b in universe,
         minus stoplist            → confidence MEDIUM, accept
         in stoplist               → send to Layer 3
Layer 3: LLM adjudication          → confidence from model
Also:    company-name matches      → confidence MEDIUM
```
- [ ] Stoplist: start with ~300 entries (common English words, finance jargon like DD/YOLO/ATH/IV/PT/EOD, month/state abbreviations). Keep it in a versioned data file, not code.
- [ ] Layer 3: batch 20–50 ambiguous mentions per call to a cheap model (Haiku-class). Prompt: sentence + candidate symbol → JSON `[{idx, is_ticker: bool}]`. Cache results keyed on (symbol, normalized-sentence-hash) so repeats are free. Set a daily spend cap.

### 2.3 Mentions table
```sql
mentions (
  id           bigserial primary key,
  ticker       text not null references tickers(symbol),
  source_id    text not null,           -- post or comment id
  source_type  text not null,           -- 'post' | 'comment'
  subreddit    text not null,
  author       text,
  created_utc  timestamptz not null,    -- from the source, NOT extraction time
  confidence   text not null,           -- high | medium | llm
  sentiment    real,                    -- filled in 2.5
  extractor_v  int not null             -- version, so re-runs are auditable
);
```
- [ ] Extraction runs as a batch job over raw rows, idempotent, re-runnable with a new `extractor_v`.

### 2.4 Ground truth — do not skip
- [ ] Hand-label **200 random candidate mentions** (mix of layers) into `tests/fixtures/labeled_mentions.jsonl`.
- [ ] Script that reports precision/recall against this set on every extractor change.
- [ ] Target: **precision ≥ 0.9** before Phase 3. Recall matters less — missing some mentions is fine; false tickers poison the signal.

### 2.5 Sentiment
- [ ] VADER on the containing sentence. Store compound score. Known-bad on WSB irony ("this is going to zero 🚀🚀" reads negative); note it, measure later, don't fix yet.

**Cursor prompt seed:** "Implement the three-layer ticker extractor per Phase 2. Include the stoplist data file, batched LLM adjudication with caching and a spend cap, the mentions table migration, and an evaluation script against tests/fixtures/labeled_mentions.jsonl printing precision/recall."

---

## Phase 3 — Signal construction (weekend 4)

Goal: a daily ranked list of anomalous tickers.

### 3.1 Aggregates
- [ ] TimescaleDB continuous aggregate: per ticker per hour — mention count, unique authors, mean sentiment, distinct subs.

### 3.2 Anomaly score
For each ticker each day:
- [ ] `z = (mentions_today − μ_30d) / max(σ_30d, floor)` — the ticker's own trailing baseline, with a σ floor so tickers going 0→3 don't produce infinite z.
- [ ] **Unique authors**, not raw mentions, as the primary count (10 posts by 1 account = pump; 10 by 10 = interest).
- [ ] Author-quality weight: account age + karma if cheaply fetchable; at minimum, discount authors whose first-ever appearance in your data is today.
- [ ] Cross-sub spread: number of distinct subs mentioning it (≥3 simultaneously is a strong feature).
- [ ] First-mention flag: ticker never before seen in your history.
- [ ] Composite score = weighted combination. Start with hand weights; tune in Phase 4.

### 3.3 Daily output
- [ ] Nightly job writes top-20 to a `signals` table and prints a report.
- [ ] Eyeball it for two weeks. Failure modes to look for: mega-caps dominating (baseline normalization broken), garbage symbols (extraction precision too low), or pure pump chatter (author weighting too weak).

**Cursor prompt seed:** "Implement Phase 3: continuous aggregates, the daily anomaly scoring job with per-ticker 30-day baselines and the listed features, writing to a signals table with a printed daily report."

---

## Phase 4 — Honest backtest (weekends 5–6)

Goal: find out if the signal is worth anything. Expect the answer to be "mostly no" — the documented result is that returns *precede* mention spikes.

### 4.1 Price data
- [ ] yfinance daily OHLCV for every ticker that ever hit your top-20, plus SPY. Cache to Postgres.

### 4.2 Event study
- [ ] Event = ticker crosses z-threshold on day *t* (signal computed from data timestamped strictly before that day's close).
- [ ] Measure forward excess returns vs SPY at t+1, t+3, t+7 (entry = next day's open, never same-day close).
- [ ] Also measure t−3 → t returns: this shows how much of the move you already missed.
- [ ] Report: mean/median excess return, hit rate, distribution plot, per-bucket breakdown (by z-score, by unique-author count, by first-mention flag).

### 4.3 Lookahead audit
- [ ] Assert in code: every mention timestamp < signal date < entry date.
- [ ] If results look great, assume a bug first. The classic ones: using `ingested_at` gaps to peek, same-day close entry, survivorship in the ticker universe (delisted pump-and-dumps vanish from yfinance — note this bias explicitly).

### 4.4 Variants worth testing
- [ ] Long the spike vs **fade the extreme spike** (short/avoid signal — better empirical support).
- [ ] Early-detection subset only: low absolute mentions (3–10) from aged accounts, before trending-list territory.
- [ ] Sub-specific: pennystocks/smallstreetbets vs WSB.

Write up the result honestly in the README, whatever it is. A rigorous null result is a *better* portfolio artifact than a suspicious Sharpe ratio — it shows you can evaluate, not just build.

---

## Phase 5 — Dashboard (only after Phase 4)

- [ ] Streamlit first: today's top-20, per-ticker mention history chart, links to backtest report. ~1 day of work.
- [ ] Graduate to FastAPI + React only if this becomes something you show off or share. (That rebuild is also a clean Cursor project of its own.)
- [ ] If sharing publicly: derived numbers only — no post bodies, titles, usernames, or excerpts. Keeps you on the defensible side of the ToS line and costs the product little.

---

## Ongoing / hygiene

- [ ] Weekly: check poller heartbeat, ticker universe refresh, LLM spend.
- [ ] Monthly: re-run extraction eval; drift in WSB slang will erode precision.
- [ ] Keep the Reddit ingestion behind an interface (`SourceAdapter`) so StockTwits or a data reseller can be swapped in if the API situation changes.
- [ ] README documents: architecture diagram, the disambiguation approach, the backtest methodology and result. These three are the interview-talking-point core.

## Rough timeline

| Phase | Effort | Calendar |
|---|---|---|
| 0 Setup | 1 evening | Week 1 |
| 1 Ingestion | 1 weekend + let it run | Weeks 1–2 |
| 2 Extraction | 2 weekends | Weeks 2–4 |
| 3 Signal | 1 weekend | Week 5 |
| 4 Backtest | 2 weekends | Weeks 6–7 |
| 5 Dashboard | 1 day | Week 8 |

Total: ~7–8 weeks of side-project pace, with the pipeline collecting data in the background from week 1.
