#!/usr/bin/env bash
set -euo pipefail
cd /Users/matthewlavoie/RedditStockSignal
LOG=data/recompress.log
echo "=== recompress start $(date -u) ===" | tee "$LOG"

run_sql() {
  docker compose exec -T db psql -U reddit_signal -d reddit_signal -v ON_ERROR_STOP=1 "$@" </dev/null
}

recompress_table() {
  local table="$1"
  echo "=== listing nonempty ${table} chunks ===" | tee -a "$LOG"
  run_sql -At -c "
    SELECT format('%I.%I', ch.chunk_schema, ch.chunk_name)
    FROM timescaledb_information.chunks ch
    JOIN pg_class c ON c.oid = format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass
    WHERE ch.hypertable_name = '${table}'
      AND c.relpages > 0
    ORDER BY c.relpages DESC;
  " > "/tmp/nonempty_${table}.txt"

  local total chunk i=0
  total=$(wc -l < "/tmp/nonempty_${table}.txt" | tr -d ' ')
  echo "nonempty ${table} chunks: ${total}" | tee -a "$LOG"

  while IFS= read -r chunk; do
    [ -z "$chunk" ] && continue
    i=$((i + 1))
    echo "[${table} ${i}/${total}] ${chunk} $(date -u)" | tee -a "$LOG"
    run_sql -c "SET statement_timeout = 0; SELECT decompress_chunk('${chunk}'::regclass, true); SELECT compress_chunk('${chunk}'::regclass, true);" \
      >>"$LOG" 2>&1
    if (( i % 10 == 0 )); then
      run_sql -At -c "SELECT pg_size_pretty(pg_database_size('reddit_signal'));" | tee -a "$LOG"
    fi
  done < "/tmp/nonempty_${table}.txt"
}

recompress_table raw_comments
recompress_table raw_posts

echo "=== final sizes $(date -u) ===" | tee -a "$LOG"
run_sql -c "
  SELECT pg_size_pretty(pg_database_size('reddit_signal')) AS db_size;
  SELECT 'raw_comments' AS ht, pg_size_pretty(total_bytes) AS total
  FROM hypertable_detailed_size('raw_comments')
  UNION ALL
  SELECT 'raw_posts', pg_size_pretty(total_bytes)
  FROM hypertable_detailed_size('raw_posts');
  SELECT count(*) FILTER (WHERE c.relpages = 0) AS empty_heap,
         count(*) FILTER (WHERE c.relpages > 0) AS nonempty_heap
  FROM timescaledb_information.chunks ch
  JOIN pg_class c ON c.oid = format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass
  WHERE ch.hypertable_name = 'raw_comments';
" | tee -a "$LOG"
echo "=== recompress done $(date -u) ===" | tee -a "$LOG"
