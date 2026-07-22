# Arctic Shift archive manifest

Complete corpus for `wallstreetbets`, `stocks`, `pennystocks`, `smallstreetbets`.
Loads into Postgres are scoped to `--start-date 2020-01-01`; these files remain the
**sole full-history copies** (including pre-2020). **Do not delete.**

| File | Kind | Size (approx) | Retention |
|------|------|---------------|-----------|
| `smallstreetbets_comments.zst` | comments | 56 MB | retained-full-history |
| `smallstreetbets_submissions.zst` | posts | 33 MB | retained-full-history |
| `pennystocks_comments.zst` | comments | 322 MB | retained-full-history |
| `pennystocks_submissions.zst` | posts | 87 MB | retained-full-history |
| `stocks_comments.zst` | comments | 994 MB | retained-full-history |
| `stocks_submissions.zst` | posts | 109 MB | retained-full-history |
| `wallstreetbets_comments.zst` | comments | 8.0 GB | retained-full-history |
| `wallstreetbets_submissions.zst` | posts | 572 MB | retained-full-history |

All paths are under `data/samples/`. Prefer an external backup in addition to local retention.
