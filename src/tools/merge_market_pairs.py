"""
merge_market_pairs.py
---------------------
Merges new entries from predicted_equivalent_markets.csv into tracked_pairs.csv
(used by portfolio_arb_monitor.py and portfolio_exit_executor.py) without adding
duplicates.

This script:
  1. Reads predicted_equivalent_markets.csv (source of new pairs)
  2. Looks up the Polymarket slug for each pair (from polymarket_markets.csv)
  3. Converts each row into the tracked_pairs format
  4. Merges into tracked_pairs.csv, deduplicating on kalshi_ticker
     - Existing rows in tracked_pairs.csv are kept as-is (preserving
       kalshi_side_held, quantities, notes, etc.)
     - Only rows with a kalshi_ticker NOT already in tracked_pairs are added

Usage:
    python src/tools/merge_market_pairs.py
    python src/tools/merge_market_pairs.py --dry-run
    python src/tools/merge_market_pairs.py --source Data/predicted_equivalent_markets.csv
"""

import argparse
import os
import re
import shutil
import sys
from datetime import datetime

import pandas as pd

# ── paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

SOURCE_CSV       = os.path.join(PROJECT_ROOT, "Data", "predicted_equivalent_markets.csv")
TRACKED_CSV      = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs.csv")
POLY_MARKETS_CSV = os.path.join(PROJECT_ROOT, "Data", "polymarket_markets.csv")

# ── defaults (written into new tracked_pairs rows) ────────────────────────────
DEFAULT_ACTIVE               = True
DEFAULT_MAX_POSITION_USD     = 100.0
DEFAULT_MIN_PROFIT_PCT       = 1.0
DEFAULT_MIN_LIQUIDITY_USD    = 50.0
DEFAULT_COOLDOWN_MINUTES     = 30

TRACKED_COLS = [
    "pair_id", "active",
    "kalshi_ticker", "kalshi_title",
    "kalshi_side_held", "kalshi_quantity",
    "polymarket_ticker", "polymarket_title",
    "polymarket_side_held", "polymarket_quantity",
    "close_time", "match_score",
    "max_position_per_pair_usd", "min_profit_pct",
    "min_liquidity_usd", "cooldown_minutes", "notes",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def build_pair_id(kalshi_ticker: str, polymarket_slug: str) -> str:
    return f"{slugify(kalshi_ticker)}__{slugify(polymarket_slug)}"


def load_slug_lookup(poly_csv: str) -> dict:
    """
    Returns {str(market_ticker): market_slug} from polymarket_markets.csv.
    Falls back to series_slug when market_slug is missing.
    """
    if not os.path.exists(poly_csv):
        print(f"WARNING: {poly_csv} not found – Polymarket slugs cannot be resolved.")
        return {}

    print(f"Loading Polymarket slug lookup from {poly_csv} …")
    try:
        df = pd.read_csv(poly_csv, usecols=["market_ticker", "market_slug", "series_slug"])
    except ValueError:
        # Older file might not have series_slug
        df = pd.read_csv(poly_csv, usecols=["market_ticker", "market_slug"])
        df["series_slug"] = None

    df["market_ticker"] = df["market_ticker"].astype(str).str.replace(r"\.0$", "", regex=True)
    lookup: dict = {}
    for _, row in df.iterrows():
        slug = row["market_slug"] if pd.notna(row.get("market_slug")) else row.get("series_slug")
        if pd.notna(slug):
            lookup[row["market_ticker"]] = str(slug)
    print(f"  Loaded {len(lookup):,} slug entries.")
    return lookup


# ── core logic ────────────────────────────────────────────────────────────────

def convert_to_tracked_row(src_row: pd.Series, slug_lookup: dict) -> dict | None:
    """
    Map one predicted_equivalent_markets row → tracked_pairs row.
    Returns None if no slug can be resolved for the Polymarket side.
    """
    kalshi_ticker = str(src_row["kalshi_market_ticker"]).strip()
    poly_id       = str(src_row["polymarket_market_ticker"]).strip().replace(".0", "")

    poly_slug = slug_lookup.get(poly_id)
    if not poly_slug:
        print(f"  SKIP {kalshi_ticker} – no Polymarket slug for ID {poly_id}")
        return None

    return {
        "pair_id":                 build_pair_id(kalshi_ticker, poly_slug),
        "active":                  DEFAULT_ACTIVE,
        "kalshi_ticker":           kalshi_ticker,
        "kalshi_title":            str(src_row.get("kalshi_market", "")).strip(),
        "kalshi_side_held":        "",          # unknown until position is opened
        "kalshi_quantity":         0.0,
        "polymarket_ticker":       poly_slug,
        "polymarket_title":        str(src_row.get("polymarket_market", "")).strip(),
        "polymarket_side_held":    "",
        "polymarket_quantity":     0.0,
        "close_time":              "",
        "match_score":             src_row.get("semantic_score", ""),
        "max_position_per_pair_usd": DEFAULT_MAX_POSITION_USD,
        "min_profit_pct":          DEFAULT_MIN_PROFIT_PCT,
        "min_liquidity_usd":       DEFAULT_MIN_LIQUIDITY_USD,
        "cooldown_minutes":        DEFAULT_COOLDOWN_MINUTES,
        "notes":                   "added from predicted_equivalent_markets",
    }


def merge(source_path: str, tracked_path: str, poly_csv: str, dry_run: bool) -> None:

    # 1 – load slug lookup
    slug_lookup = load_slug_lookup(poly_csv)

    # 2 – load source
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")
    df_source = pd.read_csv(source_path)
    print(f"\nSource pairs  : {len(df_source)} (from {source_path})")

    # 3 – load existing tracked_pairs
    if os.path.exists(tracked_path):
        df_tracked = pd.read_csv(tracked_path)
        print(f"Tracked pairs : {len(df_tracked)} existing rows")
    else:
        df_tracked = pd.DataFrame(columns=TRACKED_COLS)
        print(f"Tracked pairs : 0 (file will be created at {tracked_path})")

    existing_kalshi_tickers = set(df_tracked["kalshi_ticker"].astype(str).str.strip())

    # 4 – convert source rows and filter to only truly new ones
    new_rows = []
    skipped_no_slug = 0
    skipped_duplicate = 0

    for _, row in df_source.iterrows():
        converted = convert_to_tracked_row(row, slug_lookup)
        if converted is None:
            skipped_no_slug += 1
            continue

        kalshi_t = converted["kalshi_ticker"]
        if kalshi_t in existing_kalshi_tickers:
            skipped_duplicate += 1
            continue

        new_rows.append(converted)
        existing_kalshi_tickers.add(kalshi_t)   # prevent same ticker from appearing twice in new batch

    print(f"\nNew pairs to add : {len(new_rows)}")
    print(f"Already tracked  : {skipped_duplicate}")
    print(f"No slug found    : {skipped_no_slug}")

    if not new_rows:
        print("\nNothing to add – tracked_pairs.csv is unchanged.")
        return

    df_new = pd.DataFrame(new_rows)[TRACKED_COLS]

    if dry_run:
        print("\n[DRY RUN] Would add these rows:\n")
        print(df_new[["kalshi_ticker", "polymarket_ticker", "match_score", "notes"]].to_string(index=False))
        return

    # 5 – backup + write
    if os.path.exists(tracked_path):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = tracked_path.replace(".csv", f"_backup_{ts}.csv")
        shutil.copy2(tracked_path, backup)
        print(f"\nBackup saved  : {backup}")

    df_result = pd.concat([df_tracked, df_new], ignore_index=True)
    df_result.to_csv(tracked_path, index=False)
    print(f"Done. tracked_pairs.csv now has {len(df_result)} rows (+{len(new_rows)} new).")


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Merge new pairs from predicted_equivalent_markets.csv into tracked_pairs.csv"
    )
    parser.add_argument(
        "--source",
        default=SOURCE_CSV,
        help=f"Source CSV (default: {SOURCE_CSV})",
    )
    parser.add_argument(
        "--tracked",
        default=TRACKED_CSV,
        help=f"Target tracked_pairs CSV (default: {TRACKED_CSV})",
    )
    parser.add_argument(
        "--poly-markets",
        default=POLY_MARKETS_CSV,
        help=f"Polymarket raw CSV for slug lookup (default: {POLY_MARKETS_CSV})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing any files",
    )
    args = parser.parse_args()
    merge(args.source, args.tracked, args.poly_markets, args.dry_run)


if __name__ == "__main__":
    main()
