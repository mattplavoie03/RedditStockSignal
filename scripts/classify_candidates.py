#!/usr/bin/env python3
"""Classify corpus ticker candidates against SEC, history, crypto, and slang.

Pipeline
--------
1. Primary equity match (SEC + Wayback NASDAQ/NYSE) → ``confirmed`` / ``ambiguous``.
2. Secondary: stoplist / SEC OTC / foreign-suffix / CoinGecko → else intermediate
   ``review``.
3. Refine ``review``:
   - expanded slang → ``stoplist``
   - alias / 1-edit / company-name → ``alias``
   - curated confident equity (+ SEC EDGAR full-text hits) → ``confirmed``
   - cashtag_count >= 200 survivors → ``confirmed`` (manual spot-check)
   - remainder → ``unrecognized``

Does NOT insert into the database.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tickers.aliases import (
    CONFIDENT_EQUITY,
    find_alias_target,
)
from tickers.loader import parse_nasdaq_listed, parse_other_listed
from tickers.stoplist import SLANG_STOPLIST, should_drop_foreign_suffix

logger = logging.getLogger(__name__)

SEC_UA = "reddit-signal/0.1 (research; mattplavoie03@gmail.com)"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"

WAYBACK_TIMESTAMPS = (
    "20200613122327",
    "20210722051721",
    "20221007151526",
    "20230928074518",
    "20240519044910",
    "20251007085649",
)

NASDAQ_LISTED_PATH = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_PATH = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"

# SEC exchange field values treated as OTC / non-national-listed foreign.
OTC_FOREIGN_EXCHANGES = frozenset(
    {
        "OTC",
        "OTCMKTS",
        "OTCBB",
        "PINK",
        "GREY",
        "EXPERT",
        None,
    }
)

# NASDAQ fifth-character issuer codes commonly seen on Reddit for delisted names.
NASDAQ_FIFTH_FOREIGN = frozenset({"F", "Y", "Q"})  # foreign / ADR / bankruptcy

DEFAULT_IN = Path("data/corpus_ticker_candidates.csv")
DEFAULT_CACHE = Path("data/cache/listings")

BUCKET_OUT = {
    "confirmed": Path("data/candidates_confirmed.csv"),
    "ambiguous": Path("data/candidates_ambiguous.csv"),
    "crypto": Path("data/candidates_crypto.csv"),
    "review_foreign": Path("data/candidates_review_foreign.csv"),
    "stoplist": Path("data/candidates_stoplist.csv"),
    "alias": Path("data/candidates_alias.csv"),
    "review": Path("data/candidates_review.csv"),
    "unrecognized": Path("data/candidates_unrecognized.csv"),
}

OUTPUT_FIELDS = [
    "symbol",
    "cashtag_count",
    "allcaps_count",
    "total_count",
    "evidence",
    "bucket",
    "reason",
    "sources",
    "matched_names",
    "action",
]


@dataclass
class SymbolHit:
    sources: set[str] = field(default_factory=set)
    names: set[str] = field(default_factory=set)
    exchanges: set[str] = field(default_factory=set)

    def add(
        self,
        source: str,
        name: str | None = None,
        exchange: str | None = None,
    ) -> None:
        self.sources.add(source)
        if name:
            self.names.add(name.strip())
        if exchange is not None:
            self.exchanges.add(str(exchange))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_IN)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--min-cashtag", type=int, default=50)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download SEC / Wayback / CoinGecko even if cached",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def http_get(url: str, *, timeout: float = 120.0, sec: bool = False) -> bytes:
    headers = {"User-Agent": SEC_UA if sec else "reddit-signal/0.1"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def cached_get(
    cache_dir: Path,
    cache_name: str,
    url: str,
    *,
    refresh: bool,
    sec: bool = False,
    sleep_s: float = 0.3,
) -> str:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / cache_name
    if path.exists() and not refresh:
        return path.read_text(encoding="utf-8", errors="replace")
    logger.info("downloading %s", url)
    try:
        data = http_get(url, sec=sec)
    except urllib.error.HTTPError as exc:
        if exc.code in {429, 503}:
            logger.warning("HTTP %s on %s; sleeping 5s and retrying", exc.code, url)
            time.sleep(5)
            data = http_get(url, sec=sec)
        else:
            raise
    text = data.decode("utf-8" if sec or "coingecko" in url else "latin-1", errors="replace")
    path.write_text(text, encoding="utf-8")
    time.sleep(sleep_s)
    return text


def load_sec_symbols(cache_dir: Path, *, refresh: bool) -> dict[str, SymbolHit]:
    hits: dict[str, SymbolHit] = {}
    raw = cached_get(
        cache_dir, "sec_company_tickers.json", SEC_TICKERS_URL, refresh=refresh, sec=True
    )
    for row in json.loads(raw).values():
        sym = str(row.get("ticker", "")).strip().upper()
        if sym:
            hits.setdefault(sym, SymbolHit()).add("sec_company_tickers", row.get("title"))

    raw_ex = cached_get(
        cache_dir,
        "sec_company_tickers_exchange.json",
        SEC_EXCHANGE_URL,
        refresh=refresh,
        sec=True,
    )
    payload = json.loads(raw_ex)
    fields = payload["fields"]
    ti, ni, ei = fields.index("ticker"), fields.index("name"), fields.index("exchange")
    for row in payload["data"]:
        sym = str(row[ti]).strip().upper()
        if not sym:
            continue
        exch = row[ei]
        hits.setdefault(sym, SymbolHit()).add(
            f"sec_exchange:{exch}",
            str(row[ni]),
            exchange=None if exch is None else str(exch),
        )
    return hits


def wayback_url(timestamp: str, original: str) -> str:
    return f"https://web.archive.org/web/{timestamp}id_/{original}"


def load_historical_listings(cache_dir: Path, *, refresh: bool) -> dict[str, SymbolHit]:
    hits: dict[str, SymbolHit] = {}
    for ts in WAYBACK_TIMESTAMPS:
        year = ts[:4]
        for kind, original, parser in (
            ("nasdaqlisted", NASDAQ_LISTED_PATH, parse_nasdaq_listed),
            ("otherlisted", OTHER_LISTED_PATH, parse_other_listed),
        ):
            cache_name = f"wayback_{year}_{kind}.txt"
            try:
                text = cached_get(
                    cache_dir,
                    cache_name,
                    wayback_url(ts, original),
                    refresh=refresh,
                    sleep_s=0.5,
                )
                rows = parser(text)
            except Exception as exc:  # noqa: BLE001
                logger.warning("skip %s: %s", cache_name, exc)
                continue
            source = f"wayback_{year}_{kind}"
            for row in rows:
                hits.setdefault(row.symbol, SymbolHit()).add(
                    source, row.name, exchange=row.exchange
                )
            logger.info("%s: %s symbols", cache_name, len(rows))
    return hits


def load_coingecko_symbols(cache_dir: Path, *, refresh: bool) -> dict[str, set[str]]:
    """Map SYMBOL → set of coin names (uppercased symbols from CoinGecko)."""
    raw = cached_get(
        cache_dir,
        "coingecko_coins_list.json",
        COINGECKO_LIST_URL,
        refresh=refresh,
        sec=False,
        sleep_s=0.5,
    )
    out: dict[str, set[str]] = {}
    for row in json.loads(raw):
        sym = str(row.get("symbol", "")).strip().upper()
        name = str(row.get("name", "")).strip()
        if not sym or not sym.isalnum() or len(sym) > 5:
            continue
        out.setdefault(sym, set()).add(name)
    logger.info("coingecko symbols: %s", len(out))
    return out


def merge_hits(*maps: dict[str, SymbolHit]) -> dict[str, SymbolHit]:
    out: dict[str, SymbolHit] = {}
    for m in maps:
        for sym, hit in m.items():
            dest = out.setdefault(sym, SymbolHit())
            dest.sources |= hit.sources
            dest.names |= hit.names
            dest.exchanges |= hit.exchanges
    return out


def load_candidates(path: Path, *, min_cashtag: int) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return [r for r in rows if int(r["cashtag_count"]) >= min_cashtag]


def is_foreign_suffix(symbol: str) -> bool:
    """ADR / foreign / bankruptcy-style ticker morphology."""
    if len(symbol) >= 4 and symbol[-1] in {"F", "Y"}:
        return True
    # NASDAQ fifth-character issue codes (e.g. SHLDQ, HTZGQ, RYCEY, TCEHY)
    if len(symbol) == 5 and symbol[-1] in NASDAQ_FIFTH_FOREIGN:
        return True
    return False


def is_sec_otc_or_foreign(hit: SymbolHit) -> bool:
    if not hit.exchanges:
        # source tags like sec_exchange:OTC when exchange parsed
        return any(
            s.startswith("sec_exchange:OTC")
            or s.startswith("sec_exchange:None")
            or s.endswith(":OTC")
            for s in hit.sources
        )
    return any(
        (ex is None) or (str(ex).upper() in {"OTC", "OTCMKTS", "OTCBB", "PINK", "GREY"})
        for ex in hit.exchanges
    )


def primary_bucket(symbol: str, hit: SymbolHit | None) -> str | None:
    """Return confirmed/ambiguous, or None to send through secondary classifiers."""
    if hit is None or not hit.sources:
        return None
    # Listed on a national exchange dump / SEC national listing
    if symbol in SLANG_STOPLIST or len(symbol) <= 1:
        return "ambiguous"
    return "confirmed"


def secondary_bucket(
    symbol: str,
    *,
    sec_hit: SymbolHit | None,
    crypto_names: set[str] | None,
) -> tuple[str, str]:
    """Classify symbols missing from primary equity listings.

    Returns (bucket, reason). ``review`` is an intermediate bucket refined later.
    """
    if symbol in SLANG_STOPLIST:
        return "stoplist", "slang_or_profanity"

    if sec_hit is not None and is_sec_otc_or_foreign(sec_hit):
        return "confirmed", "sec_otc_or_foreign_exchange"

    if is_foreign_suffix(symbol):
        reason = "suffix_adr_foreign"
        if len(symbol) == 5 and symbol[-1] == "Q":
            reason = "suffix_nasdaq_bankruptcy_Q"
        elif symbol.endswith("Y"):
            reason = "suffix_adr_Y"
        elif symbol.endswith("F"):
            reason = "suffix_foreign_F"
        return "review_foreign", reason

    if crypto_names:
        return "crypto", "coingecko_symbol"

    return "review", "unmatched"


def sec_edgar_mentions_ticker(symbol: str, *, cache_dir: Path) -> tuple[bool, str]:
    """Query SEC EDGAR full-text for $SYMBOL / ticker mentions (cached)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "sec_edgar_hits" / f"{symbol}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists():
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return bool(payload.get("hit")), str(payload.get("label", ""))

    queries = [f'"${symbol}"', symbol]
    label = ""
    hit = False
    for q in queries:
        url = (
            "https://efts.sec.gov/LATEST/search-index?"
            f"q={urllib.parse.quote(q)}"
            "&dateRange=custom&startdt=2019-01-01&enddt=2026-12-31"
            "&forms=8-K,10-K,10-Q,S-1,424B2,424B4,424B5"
        )
        try:
            raw = http_get(url, sec=True, timeout=60)
            data = json.loads(raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.debug("SEC EDGAR miss/error for %s (%s): %s", symbol, q, exc)
            time.sleep(0.2)
            continue
        total = data.get("hits", {}).get("total", {})
        value = total.get("value", 0) if isinstance(total, dict) else int(total or 0)
        if value <= 0:
            time.sleep(0.15)
            continue
        hits = data.get("hits", {}).get("hits", [])
        names = hits[0].get("_source", {}).get("display_names") or [] if hits else []
        label = names[0] if names else f"sec_hits={value}"
        joined = " ".join(names).upper()
        if names and f"({symbol})" not in joined and symbol not in joined.split():
            if q.startswith('"$') and value >= 3:
                hit = True
                break
            time.sleep(0.15)
            continue
        hit = True
        break
    cache_path.write_text(json.dumps({"hit": hit, "label": label}), encoding="utf-8")
    time.sleep(0.15)
    return hit, label


def refine_review_bucket(
    review_rows: list[dict[str, str]],
    *,
    equity_universe: dict[str, SymbolHit],
    cache_dir: Path,
) -> dict[str, list[dict[str, str]]]:
    """Split intermediate ``review`` rows into final buckets."""
    _ = equity_universe  # kept for call-site compatibility / future matching
    out: dict[str, list[dict[str, str]]] = {
        "stoplist": [],
        "alias": [],
        "confirmed": [],
        "unrecognized": [],
    }

    need_sec: list[dict[str, str]] = []
    for row in review_rows:
        symbol = row["symbol"]
        if symbol in SLANG_STOPLIST:
            out["stoplist"].append(
                {
                    **row,
                    "bucket": "stoplist",
                    "reason": "slang_or_profanity_review",
                    "sources": "slang_stoplist",
                    "matched_names": "",
                }
            )
            continue

        # Allowlist wins over alias (RDBX≠DBX, MMTLP≠MMLP, EHANG≠EH).
        if symbol in CONFIDENT_EQUITY:
            out["confirmed"].append(
                {
                    **row,
                    "bucket": "confirmed",
                    "reason": "human_confident_equity",
                    "sources": "manual_review_allowlist",
                    "matched_names": "",
                    "action": "review",
                }
            )
            continue

        alias = find_alias_target(symbol, set())
        if alias is not None:
            out["alias"].append(
                {
                    **row,
                    "bucket": "alias",
                    "reason": f"alias_of:{alias}",
                    "sources": "known_alias_map",
                    "matched_names": alias,
                    "action": "review",
                }
            )
            continue

        need_sec.append(row)

    logger.info("SEC EDGAR full-text checks for %s review symbols", len(need_sec))
    after_sec: list[dict[str, str]] = []
    for row in need_sec:
        symbol = row["symbol"]
        hit, label = sec_edgar_mentions_ticker(symbol, cache_dir=cache_dir)
        if hit:
            out["confirmed"].append(
                {
                    **row,
                    "bucket": "confirmed",
                    "reason": "sec_edgar_fulltext",
                    "sources": "sec_efts",
                    "matched_names": label,
                    "action": "review",
                }
            )
        else:
            after_sec.append(row)

    for row in after_sec:
        cash = int(row["cashtag_count"])
        if cash >= 200:
            out["confirmed"].append(
                {
                    **row,
                    "bucket": "confirmed",
                    "reason": "high_cashtag_spotcheck",
                    "sources": "cashtag_threshold_200",
                    "matched_names": "",
                    "action": "review",
                }
            )
        else:
            out["unrecognized"].append(
                {
                    **row,
                    "bucket": "unrecognized",
                    "reason": "unmatched_low_cashtag",
                    "sources": "",
                    "matched_names": "",
                    "action": "ignore_or_manual",
                }
            )

    return out


def write_bucket(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    candidates = load_candidates(args.input, min_cashtag=args.min_cashtag)
    logger.info("candidates with cashtag >= %s: %s", args.min_cashtag, len(candidates))

    sec_hits = load_sec_symbols(args.cache_dir, refresh=args.refresh)
    hist_hits = load_historical_listings(args.cache_dir, refresh=args.refresh)
    equity_universe = merge_hits(sec_hits, hist_hits)
    crypto_map = load_coingecko_symbols(args.cache_dir, refresh=args.refresh)
    logger.info("equity reference symbols: %s", len(equity_universe))

    buckets: dict[str, list[dict[str, str]]] = {name: [] for name in BUCKET_OUT}

    for raw in candidates:
        symbol = raw["symbol"].strip().upper()
        hit = equity_universe.get(symbol)
        sec_only = sec_hits.get(symbol)
        bucket = primary_bucket(symbol, hit)
        reason = "equity_listing_match"
        sources: set[str] = hit.sources if hit else set()
        names: set[str] = hit.names if hit else set()

        if bucket is None:
            bucket, reason = secondary_bucket(
                symbol,
                sec_hit=sec_only,
                crypto_names=crypto_map.get(symbol),
            )
            if bucket == "confirmed" and sec_only:
                sources = sec_only.sources
                names = sec_only.names
            elif bucket == "crypto" and symbol in crypto_map:
                sources = {"coingecko"}
                names = crypto_map[symbol]
            elif bucket == "review_foreign":
                sources = sources or {reason}
            elif bucket == "stoplist":
                sources = {"slang_stoplist"}

        out = {
            "symbol": symbol,
            "cashtag_count": raw["cashtag_count"],
            "allcaps_count": raw["allcaps_count"],
            "total_count": raw["total_count"],
            "evidence": raw.get("evidence", ""),
            "bucket": bucket,
            "reason": reason,
            "sources": ";".join(sorted(sources)) if sources else "",
            "matched_names": " | ".join(sorted(names)[:5]) if names else "",
            "action": "review",
        }
        buckets[bucket].append(out)

    review_rows = buckets.get("review", [])
    logger.info("refining %s intermediate review rows", len(review_rows))
    refined = refine_review_bucket(
        review_rows,
        equity_universe=equity_universe,
        cache_dir=args.cache_dir,
    )
    buckets["review"] = []
    for name, rows in refined.items():
        buckets[name].extend(rows)

    # Drop foreign-suffix false positives (BECKY/BABY/TSLAQ/…) into stoplist.
    kept_foreign: list[dict[str, str]] = []
    for row in buckets.get("review_foreign", []):
        drop, drop_reason = should_drop_foreign_suffix(
            row["symbol"],
            cashtag_count=int(row["cashtag_count"]),
            allcaps_count=int(row["allcaps_count"]),
        )
        if drop:
            buckets["stoplist"].append(
                {
                    **row,
                    "bucket": "stoplist",
                    "reason": f"foreign_suffix_false_positive:{drop_reason}",
                    "sources": "foreign_suffix_filter",
                    "matched_names": "",
                    "action": "ignore",
                }
            )
        else:
            kept_foreign.append(row)
    buckets["review_foreign"] = kept_foreign

    for name, rows in buckets.items():
        rows.sort(key=lambda r: (-int(r["cashtag_count"]), r["symbol"]))
        write_bucket(BUCKET_OUT[name], rows)

    parts = [f"{name}={len(rows)}" for name, rows in buckets.items()]
    print(f"done: input={len(candidates)} " + " ".join(parts))
    for name, path in BUCKET_OUT.items():
        print(f"  {path} ({len(buckets[name])})")
    print("NOTE: nothing was inserted into tickers — review the CSVs first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
