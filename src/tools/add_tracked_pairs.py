"""
add_tracked_pairs.py
--------------------
CLI tool for manually adding matched market pairs to tracked_pairs.csv.

Two modes:
  1) Bulk: Provide a Kalshi series ticker + Polymarket event slug.
     The tool fetches all sub-markets from both, auto-matches by
     country/item name, and appends new pairs.

  2) Single: Provide a specific Kalshi ticker + Polymarket slug
     to add one pair directly.

Usage:
    # Bulk match (dry-run by default)
    python src/tools/add_tracked_pairs.py \
      --kalshi-series KXTRUMPCOUNTRIES \
      --poly-event which-countries-will-donald-trump-visit-in-2026

    # Actually write
    python src/tools/add_tracked_pairs.py \
      --kalshi-series KXTRUMPCOUNTRIES \
      --poly-event which-countries-will-donald-trump-visit-in-2026 \
      --write

    # Single pair
    python src/tools/add_tracked_pairs.py \
      --kalshi-ticker KXTRUMPCOUNTRIES-27JAN01-FRA \
      --poly-slug will-donald-trump-visit-france-in-2026 \
      --write
"""

import argparse
import os
import re
import shutil
import sys
import time
from datetime import datetime

import requests
import pandas as pd

# ── paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

TRACKED_CSV = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs.csv")

# ── API base URLs ─────────────────────────────────────────────────────────────

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"

REQUEST_TIMEOUT = 15
REQUEST_DELAY = 0.3

# ── defaults for new tracked_pairs rows ───────────────────────────────────────

DEFAULT_ACTIVE = True
DEFAULT_MAX_POSITION_USD = 100.0
DEFAULT_MIN_PROFIT_PCT = 1.0
DEFAULT_MIN_LIQUIDITY_USD = 50.0
DEFAULT_COOLDOWN_MINUTES = 30

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


def normalize_name(name: str) -> str:
    """Normalize a country/item name for matching."""
    name = str(name).strip().lower()
    # Remove parenthetical qualifiers like "(incl. Greenland)" or "(West Bank and Gaza Strip)"
    name = re.sub(r"\s*\(.*?\)\s*", "", name)
    # Remove common prefixes/suffixes
    name = name.strip()
    return name


# ── API fetchers ──────────────────────────────────────────────────────────────

def fetch_kalshi_series_markets(series_ticker: str) -> list[dict]:
    """Fetch all open markets in a Kalshi series."""
    url = f"{KALSHI_BASE}/markets"
    params = {
        "series_ticker": series_ticker,
        "status": "open",
        "limit": 200,
    }
    print(f"Fetching Kalshi markets for series: {series_ticker} ...")

    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"ERROR fetching Kalshi series {series_ticker}: {e}")
        return []

    markets = data.get("markets", [])
    print(f"  Found {len(markets)} open Kalshi markets.")
    return markets


def fetch_polymarket_event_markets(event_slug: str) -> list[dict]:
    """Fetch all markets in a Polymarket event by its slug."""
    url = f"{POLYMARKET_GAMMA}/events/slug/{event_slug}"
    print(f"Fetching Polymarket event: {event_slug} ...")

    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"ERROR fetching Polymarket event {event_slug}: {e}")
        return []

    markets = data.get("markets", [])
    print(f"  Found {len(markets)} Polymarket markets.")
    return markets


def fetch_polymarket_market_by_slug(slug: str) -> dict | None:
    """Fetch a single Polymarket market by its slug."""
    url = f"{POLYMARKET_GAMMA}/markets/slug/{slug}"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"ERROR fetching Polymarket market {slug}: {e}")
        return None


# ── name extraction ───────────────────────────────────────────────────────────

def extract_kalshi_item_name(market: dict) -> str:
    """Extract the country/item name from a Kalshi market."""
    # Prefer custom_strike.Area (most structured)
    custom = market.get("custom_strike", {})
    if isinstance(custom, dict) and custom.get("Area"):
        return custom["Area"]

    # Fallback: yes_sub_title
    sub = market.get("yes_sub_title", "")
    if sub:
        return sub

    # Last resort: no_sub_title
    return market.get("no_sub_title", "")


def extract_poly_item_name(market: dict) -> str:
    """Extract the country/item name from a Polymarket market."""
    title = market.get("groupItemTitle", "")
    if title:
        return title

    # Fallback: try to extract from question
    return market.get("question", "")


# ── matching ──────────────────────────────────────────────────────────────────

def match_markets(
    kalshi_markets: list[dict],
    poly_markets: list[dict],
    skip_closed: bool = True,
) -> list[tuple[dict, dict]]:
    """
    Match Kalshi and Polymarket sub-markets by item name.

    Returns list of (kalshi_market, poly_market) tuples.
    """
    # Build lookup: normalized name -> poly market
    poly_lookup: dict[str, dict] = {}
    poly_raw_names: dict[str, str] = {}  # normalized -> original

    for pm in poly_markets:
        if skip_closed and pm.get("closed", False):
            raw_name = extract_poly_item_name(pm)
            print(f"  SKIP (closed): Polymarket '{raw_name}'")
            continue

        raw_name = extract_poly_item_name(pm)
        norm = normalize_name(raw_name)
        if norm:
            poly_lookup[norm] = pm
            poly_raw_names[norm] = raw_name

    matched = []
    used_poly_keys = set()
    unmatched_kalshi = []

    for km in kalshi_markets:
        k_raw = extract_kalshi_item_name(km)
        k_norm = normalize_name(k_raw)

        if not k_norm:
            unmatched_kalshi.append(k_raw or km.get("ticker", "?"))
            continue

        # 1) Try exact match
        if k_norm in poly_lookup:
            matched.append((km, poly_lookup[k_norm]))
            used_poly_keys.add(k_norm)
            continue

        # 2) Try substring match (Kalshi name contained in Poly name, or vice versa)
        found = False
        for p_norm, pm in poly_lookup.items():
            if p_norm in used_poly_keys:
                continue
            if k_norm in p_norm or p_norm in k_norm:
                matched.append((km, pm))
                used_poly_keys.add(p_norm)
                found = True
                break

        if not found:
            unmatched_kalshi.append(k_raw)

    # Report unmatched
    unmatched_poly = [
        poly_raw_names[k]
        for k in poly_lookup
        if k not in used_poly_keys
    ]

    if unmatched_kalshi:
        print(f"\n  Unmatched Kalshi markets ({len(unmatched_kalshi)}):")
        for name in unmatched_kalshi:
            print(f"    - {name}")

    if unmatched_poly:
        print(f"\n  Unmatched Polymarket markets ({len(unmatched_poly)}):")
        for name in unmatched_poly:
            print(f"    - {name}")

    return matched


# ── CSV row builder ───────────────────────────────────────────────────────────

def build_tracked_row(k_market: dict, p_market: dict, note: str = "") -> dict:
    """Build a tracked_pairs CSV row from a matched pair."""
    kalshi_ticker = k_market.get("ticker", "")
    poly_slug = p_market.get("slug", "")
    kalshi_title = k_market.get("title", "")
    poly_title = p_market.get("question", "")
    close_time = k_market.get("close_time", "") or p_market.get("endDate", "")

    return {
        "pair_id": build_pair_id(kalshi_ticker, poly_slug),
        "active": DEFAULT_ACTIVE,
        "kalshi_ticker": kalshi_ticker,
        "kalshi_title": kalshi_title,
        "kalshi_side_held": "",
        "kalshi_quantity": 0.0,
        "polymarket_ticker": poly_slug,
        "polymarket_title": poly_title,
        "polymarket_side_held": "",
        "polymarket_quantity": 0.0,
        "close_time": close_time,
        "match_score": "",
        "max_position_per_pair_usd": DEFAULT_MAX_POSITION_USD,
        "min_profit_pct": DEFAULT_MIN_PROFIT_PCT,
        "min_liquidity_usd": DEFAULT_MIN_LIQUIDITY_USD,
        "cooldown_minutes": DEFAULT_COOLDOWN_MINUTES,
        "notes": note or "manually added via add_tracked_pairs",
    }


# ── write logic ───────────────────────────────────────────────────────────────

def load_existing_tickers(tracked_csv: str) -> tuple[pd.DataFrame, set[str]]:
    """Load existing tracked_pairs.csv and return (df, set_of_kalshi_tickers)."""
    if os.path.exists(tracked_csv):
        df = pd.read_csv(tracked_csv)
        tickers = set(df["kalshi_ticker"].astype(str).str.strip())
        return df, tickers
    else:
        df = pd.DataFrame(columns=TRACKED_COLS)
        return df, set()


def write_pairs(
    new_rows: list[dict],
    tracked_csv: str,
    dry_run: bool = True,
) -> None:
    """Deduplicate, backup, and write new pairs to tracked_pairs.csv."""
    df_existing, existing_tickers = load_existing_tickers(tracked_csv)

    # Filter out already-tracked pairs
    truly_new = []
    skipped_dup = 0
    for row in new_rows:
        if row["kalshi_ticker"] in existing_tickers:
            skipped_dup += 1
            continue
        truly_new.append(row)
        existing_tickers.add(row["kalshi_ticker"])  # prevent batch-internal dupes

    print(f"\n{'=' * 60}")
    print(f"Existing pairs  : {len(df_existing)}")
    print(f"Candidates      : {len(new_rows)}")
    print(f"Already tracked : {skipped_dup}")
    print(f"New to add      : {len(truly_new)}")
    print(f"{'=' * 60}")

    if not truly_new:
        print("\nNothing new to add — tracked_pairs.csv unchanged.")
        return

    df_new = pd.DataFrame(truly_new)

    # Show preview
    print("\nNew pairs to add:\n")
    preview_cols = ["kalshi_ticker", "polymarket_ticker", "kalshi_title"]
    available = [c for c in preview_cols if c in df_new.columns]
    for i, row in df_new.iterrows():
        k_tick = row.get("kalshi_ticker", "?")
        p_tick = row.get("polymarket_ticker", "?")
        k_title = row.get("kalshi_title", "?")
        print(f"  + {k_tick}  <->  {p_tick}")
        print(f"    ({k_title})")

    if dry_run:
        print(f"\n[DRY RUN] Would add {len(truly_new)} pairs. Use --write to commit.")
        return

    # Backup
    if os.path.exists(tracked_csv):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = tracked_csv.replace(".csv", f"_backup_{ts}.csv")
        shutil.copy2(tracked_csv, backup)
        print(f"\nBackup saved: {backup}")

    # Ensure column order
    for col in TRACKED_COLS:
        if col not in df_new.columns:
            df_new[col] = ""
    df_new = df_new[TRACKED_COLS]

    df_result = pd.concat([df_existing, df_new], ignore_index=True)
    df_result.to_csv(tracked_csv, index=False)
    print(f"\nDone. tracked_pairs.csv now has {len(df_result)} rows (+{len(truly_new)} new).")


# ── mode: bulk series matching ────────────────────────────────────────────────

def bulk_match_and_add(
    kalshi_series: str,
    poly_event: str,
    tracked_csv: str,
    dry_run: bool,
    skip_closed: bool,
) -> None:
    """Fetch both platforms' sub-markets, match by name, and add."""
    kalshi_markets = fetch_kalshi_series_markets(kalshi_series)
    if not kalshi_markets:
        print("No Kalshi markets found. Aborting.")
        return

    time.sleep(REQUEST_DELAY)

    poly_markets = fetch_polymarket_event_markets(poly_event)
    if not poly_markets:
        print("No Polymarket markets found. Aborting.")
        return

    matched = match_markets(kalshi_markets, poly_markets, skip_closed=skip_closed)

    if not matched:
        print("\nNo matches found between the two market sets.")
        return

    print(f"\n  Matched {len(matched)} pairs:")
    for km, pm in matched:
        k_name = extract_kalshi_item_name(km)
        p_name = extract_poly_item_name(pm)
        print(f"    {k_name:30s} <-> {p_name}")

    new_rows = [build_tracked_row(km, pm) for km, pm in matched]
    write_pairs(new_rows, tracked_csv, dry_run=dry_run)


# ── mode: single pair ────────────────────────────────────────────────────────

def single_pair_add(
    kalshi_ticker: str,
    poly_slug: str,
    tracked_csv: str,
    dry_run: bool,
) -> None:
    """Add a single explicitly specified pair."""
    print(f"Fetching Polymarket market: {poly_slug} ...")
    pm = fetch_polymarket_market_by_slug(poly_slug)
    if not pm:
        print(f"Could not fetch Polymarket market '{poly_slug}'. Aborting.")
        return

    poly_title = pm.get("question", poly_slug)
    print(f"  Found: {poly_title}")

    # For the Kalshi side, we just use the ticker as provided.
    # We could fetch details, but it's not strictly necessary for CSV generation.
    print(f"Kalshi ticker: {kalshi_ticker}")

    # Try to fetch Kalshi title via API (best effort)
    kalshi_title = kalshi_ticker
    close_time = ""
    try:
        url = f"{KALSHI_BASE}/markets/{kalshi_ticker}"
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.ok:
            data = r.json()
            market = data.get("market", data)
            kalshi_title = market.get("title", kalshi_ticker)
            close_time = market.get("close_time", "")
            print(f"  Found: {kalshi_title}")
    except Exception:
        print(f"  (Could not fetch Kalshi title, using ticker as title)")

    k_market = {
        "ticker": kalshi_ticker,
        "title": kalshi_title,
        "close_time": close_time,
    }

    row = build_tracked_row(k_market, pm)
    write_pairs([row], tracked_csv, dry_run=dry_run)


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Manually add matched market pairs to tracked_pairs.csv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Bulk match all sub-markets (dry-run)
  python src/tools/add_tracked_pairs.py \\
    --kalshi-series KXTRUMPCOUNTRIES \\
    --poly-event which-countries-will-donald-trump-visit-in-2026

  # Bulk match and write
  python src/tools/add_tracked_pairs.py \\
    --kalshi-series KXTRUMPCOUNTRIES \\
    --poly-event which-countries-will-donald-trump-visit-in-2026 \\
    --write

  # Single pair
  python src/tools/add_tracked_pairs.py \\
    --kalshi-ticker KXTRUMPCOUNTRIES-27JAN01-FRA \\
    --poly-slug will-donald-trump-visit-france-in-2026 \\
    --write
        """,
    )

    # Bulk mode
    parser.add_argument(
        "--kalshi-series",
        help="Kalshi series ticker (e.g. KXTRUMPCOUNTRIES)",
    )
    parser.add_argument(
        "--poly-event",
        help="Polymarket event slug (e.g. which-countries-will-donald-trump-visit-in-2026)",
    )

    # Single mode
    parser.add_argument(
        "--kalshi-ticker",
        help="Specific Kalshi market ticker (e.g. KXTRUMPCOUNTRIES-27JAN01-FRA)",
    )
    parser.add_argument(
        "--poly-slug",
        help="Specific Polymarket market slug (e.g. will-donald-trump-visit-france-in-2026)",
    )

    # Options
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write to CSV (default is dry-run)",
    )
    parser.add_argument(
        "--tracked-csv",
        default=TRACKED_CSV,
        help=f"Path to tracked_pairs.csv (default: {TRACKED_CSV})",
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Include closed/resolved Polymarket markets (skipped by default)",
    )

    args = parser.parse_args()
    dry_run = not args.write

    # Determine mode
    has_bulk = args.kalshi_series and args.poly_event
    has_single = args.kalshi_ticker and args.poly_slug

    if not has_bulk and not has_single:
        parser.error(
            "Provide either:\n"
            "  --kalshi-series + --poly-event  (bulk match)\n"
            "  --kalshi-ticker + --poly-slug   (single pair)"
        )

    if has_bulk and has_single:
        parser.error("Cannot use both bulk and single modes at the same time.")

    mode_str = "DRY RUN" if dry_run else "LIVE WRITE"
    print(f"\n{'=' * 60}")
    print(f"  add_tracked_pairs.py  [{mode_str}]")
    print(f"{'=' * 60}\n")

    if has_bulk:
        bulk_match_and_add(
            kalshi_series=args.kalshi_series,
            poly_event=args.poly_event,
            tracked_csv=args.tracked_csv,
            dry_run=dry_run,
            skip_closed=not args.include_closed,
        )
    else:
        single_pair_add(
            kalshi_ticker=args.kalshi_ticker,
            poly_slug=args.poly_slug,
            tracked_csv=args.tracked_csv,
            dry_run=dry_run,
        )


if __name__ == "__main__":
    main()
