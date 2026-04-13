import argparse
import os
import re
import pandas as pd
from datetime import datetime
import requests

# PolyKalshi API functions
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"

# Default tracking settings
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

def parse_urls(kalshi_url, poly_url):
    """Extract the required identifiers from the URLs."""
    # Polymarket URL format: https://polymarket.com/event/.../[market-slug]
    poly_slug = None
    poly_match = re.search(r'polymarket\.com/(?:event|market)/[^/]+/([^/]+)', poly_url)
    if poly_match:
        poly_slug = poly_match.group(1).split('?')[0]  # Remove query params
    else:
        # Check standard market URL format
        poly_match = re.search(r'polymarket\.com/market/([^/?]+)', poly_url)
        if poly_match:
             poly_slug = poly_match.group(1).split('?')[0]
    
    # Kalshi URL format: https://kalshi.com/markets/[series_ticker]/.../[market_ticker]
    kalshi_ticker = None
    kalshi_match = re.search(r'kalshi\.com/markets/[^/]+/[^/]+/([^/?]+)', kalshi_url)
    if kalshi_match:
        kalshi_ticker = kalshi_match.group(1).upper()
    else:
        # sometimes it is just kalshi.com/markets/[ticker]
        kalshi_match = re.search(r'kalshi\.com/markets/([^/?]+)', kalshi_url)
        if kalshi_match:
            kalshi_ticker = kalshi_match.group(1).upper()

    return kalshi_ticker, poly_slug

def slugify(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")

def build_tracked_row(kalshi_ticker: str, poly_slug: str) -> dict:
    # Try fetching details from APIs for titles
    poly_title = poly_slug
    try:
        r = requests.get(f"{POLYMARKET_GAMMA}/markets/slug/{poly_slug}")
        if r.ok:
            poly_title = r.json().get("question", poly_slug)
    except: pass

    kalshi_title = kalshi_ticker
    close_time = ""
    try:
        r = requests.get(f"{KALSHI_BASE}/markets/{kalshi_ticker}")
        if r.ok:
            data = r.json().get("market", {})
            kalshi_title = data.get("title", kalshi_ticker)
            close_time = data.get("close_time", "")
    except: pass

    return {
        "pair_id": f"{slugify(kalshi_ticker)}__{slugify(poly_slug)}",
        "active": DEFAULT_ACTIVE,
        "kalshi_ticker": kalshi_ticker,
        "kalshi_title": kalshi_title,
        "polymarket_ticker": poly_slug,
        "polymarket_title": poly_title,
        "close_time": close_time,
        "max_position_per_pair_usd": DEFAULT_MAX_POSITION_USD,
        "min_profit_pct": DEFAULT_MIN_PROFIT_PCT,
        "min_liquidity_usd": DEFAULT_MIN_LIQUIDITY_USD,
        "cooldown_minutes": DEFAULT_COOLDOWN_MINUTES,
        "notes": "Added from links manual tool",
    }

def main():
    parser = argparse.ArgumentParser(description="Add tracked pairs via direct URL links")
    parser.add_argument("--kalshi", required=True, help="Kalshi market URL")
    parser.add_argument("--poly", required=True, help="Polymarket market URL")
    args = parser.parse_args()

    k_ticker, p_slug = parse_urls(args.kalshi, args.poly)
    
    print("\n--- Link Parsing ---")
    print(f"Kalshi Ticker: {k_ticker}")
    print(f"Polymarket Slug: {p_slug}")

    if not k_ticker or not p_slug:
        print("ERROR: Could not parse both URLs successfully. Please check the formats.")
        return

    csv_path = os.path.join(project_root, "Data", "tracked_pairs.csv")
    
    df_existing = pd.DataFrame(columns=TRACKED_COLS)
    if os.path.exists(csv_path):
        df_existing = pd.read_csv(csv_path)

    # Check for duplicates
    if k_ticker in df_existing['kalshi_ticker'].values:
        print(f"\nWARNING: Pairs containing Kalshi Ticker {k_ticker} already exist in tracked_pairs.csv.")
        return

    print("\nFetching data and building row...")
    new_row = build_tracked_row(k_ticker, p_slug)
    
    print("\nNew Pair:")
    print(f"  Kalshi: {new_row['kalshi_title']}")
    print(f"  Polymarket: {new_row['polymarket_title']}")
    
    df_new = pd.DataFrame([new_row])
    # Ensure columns match
    for col in TRACKED_COLS:
        if col not in df_new.columns:
            df_new[col] = ""
    df_new = df_new[TRACKED_COLS]

    df_final = pd.concat([df_existing, df_new], ignore_index=True)
    df_final.to_csv(csv_path, index=False)
    print(f"\nSuccessfully appended to {csv_path}!")

if __name__ == "__main__":
    main()
