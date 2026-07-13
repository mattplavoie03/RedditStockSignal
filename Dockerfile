FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY alembic.ini alembic/ ./
COPY src/ src/
COPY scripts/run_ingest.py scripts/run_ingest.py

ENV PYTHONPATH=/app/src

CMD ["uv", "run", "python", "scripts/run_ingest.py"]
