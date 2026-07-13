FROM python:3.12-slim-bookworm

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY alembic.ini ./
COPY alembic/ alembic/
COPY src/ src/
COPY scripts/run_ingest.py scripts/run_ingest.py

ENV PYTHONPATH=/app/src

CMD ["uv", "run", "python", "scripts/run_ingest.py"]
