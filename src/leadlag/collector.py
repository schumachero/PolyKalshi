"""
Lead-Lag Price Series Collector
===============================
Polls Kalshi and Polymarket orderbooks at regular intervals for tracked pairs,
computes midprices, and saves a synchronized time series to CSV.

Usage:
    python -m src.leadlag.collector --duration 60 --interval 5 --max-pairs 5
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# --- Path setup ---
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
for p in [SRC_DIR, PROJECT_ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)

import pandas as pd
from apis.orderbook import get_kalshi_orderbook, get_polymarket_orderbook

# ── Configuration ────────────────────────────────────────────────────────────
DEFAULT_INTERVAL = 5       # seconds between snapshots
DEFAULT_DURATION = 60      # minutes of collection
DEFAULT_MAX_PAIRS = 5      # number of pairs to poll
TRACKED_PAIRS_CSV = "Data/tracked_pairs.csv"
OUTPUT_DIR = "Data/leadlag"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "price_series.csv")

CSV_HEADERS = [
    "timestamp",
    "pair_id",
    "kalshi_ticker",
    "poly_ticker",
    "k_best_bid",
    "k_best_ask",
    "k_mid",
    "p_best_bid",
    "p_best_ask",
    "p_mid",
]


def load_tracked_pairs(
    csv_path: str = TRACKED_PAIRS_CSV,
    max_pairs: int | None = None,
) -> list[dict]:
    """Load active tracked pairs from CSV."""
    df = pd.read_csv(csv_path)
    df = df[df["active"].astype(str).str.lower() == "true"]
    if max_pairs:
        df = df.head(max_pairs)

    pairs = []
    for _, row in df.iterrows():
        pairs.append({
            "pair_id": row["pair_id"],
            "kalshi_ticker": row["kalshi_ticker"],
            "poly_ticker": row["polymarket_ticker"],
            "kalshi_title": row.get("kalshi_title", ""),
            "poly_title": row.get("polymarket_title", ""),
        })
    return pairs


def _extract_midprice(book: dict, side: str = "yes") -> tuple[float | None, float | None, float | None]:
    """
    Extract best bid, best ask, and midprice from an orderbook side.
    Prices returned in 0-1 probability scale.
    """
    bids = book.get(side, {}).get("bids", [])
    asks = book.get(side, {}).get("asks", [])

    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None

    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0
    elif best_bid is not None:
        mid = best_bid
    elif best_ask is not None:
        mid = best_ask
    else:
        mid = None

    return best_bid, best_ask, mid


def fetch_snapshot_for_pair(pair: dict) -> dict:
    """Fetch one snapshot row for a single pair."""
    ts = datetime.now(timezone.utc).isoformat()

    try:
        k_book = get_kalshi_orderbook(pair["kalshi_ticker"], levels=1)
    except Exception as e:
        print(f"  [WARN] Kalshi fetch failed for {pair['kalshi_ticker']}: {e}")
        k_book = {"yes": {"bids": [], "asks": []}}

    try:
        p_book = get_polymarket_orderbook(pair["poly_ticker"], levels=1)
    except Exception as e:
        print(f"  [WARN] Poly fetch failed for {pair['poly_ticker']}: {e}")
        p_book = {"yes": {"bids": [], "asks": []}}

    k_bid, k_ask, k_mid = _extract_midprice(k_book, "yes")
    p_bid, p_ask, p_mid = _extract_midprice(p_book, "yes")

    return {
        "timestamp": ts,
        "pair_id": pair["pair_id"],
        "kalshi_ticker": pair["kalshi_ticker"],
        "poly_ticker": pair["poly_ticker"],
        "k_best_bid": round(k_bid, 4) if k_bid is not None else "",
        "k_best_ask": round(k_ask, 4) if k_ask is not None else "",
        "k_mid": round(k_mid, 4) if k_mid is not None else "",
        "p_best_bid": round(p_bid, 4) if p_bid is not None else "",
        "p_best_ask": round(p_ask, 4) if p_ask is not None else "",
        "p_mid": round(p_mid, 4) if p_mid is not None else "",
    }


def run_collector(
    interval_sec: int = DEFAULT_INTERVAL,
    duration_min: int = DEFAULT_DURATION,
    max_pairs: int | None = DEFAULT_MAX_PAIRS,
    pairs_csv: str = TRACKED_PAIRS_CSV,
    output_csv: str = OUTPUT_CSV,
) -> str:
    """
    Main collection loop. Polls all tracked pairs at ``interval_sec`` intervals
    for ``duration_min`` minutes.  Returns path to output CSV.
    """
    Path(os.path.dirname(output_csv)).mkdir(parents=True, exist_ok=True)

    pairs = load_tracked_pairs(pairs_csv, max_pairs=max_pairs)
    if not pairs:
        print("No active tracked pairs found.")
        return output_csv

    print(f"Collecting data for {len(pairs)} pairs every {interval_sec}s for {duration_min}m")
    for p in pairs:
        print(f"  - {p['pair_id']}: {p['kalshi_ticker']} <-> {p['poly_ticker']}")

    # Check if file exists to decide on writing headers
    file_exists = os.path.exists(output_csv)

    deadline = time.time() + duration_min * 60
    tick = 0

    try:
        with open(output_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            if not file_exists:
                writer.writeheader()

            while time.time() < deadline:
                tick += 1
                cycle_start = time.time()

                # Fetch all pairs concurrently
                rows = []
                with ThreadPoolExecutor(max_workers=min(len(pairs), 4)) as pool:
                    futures = {pool.submit(fetch_snapshot_for_pair, p): p for p in pairs}
                    for future in as_completed(futures):
                        try:
                            rows.append(future.result())
                        except Exception as e:
                            p = futures[future]
                            print(f"  [ERR] {p['pair_id']}: {e}")

                for row in rows:
                    writer.writerow(row)
                f.flush()

                elapsed = time.time() - cycle_start
                sleep_time = max(interval_sec - elapsed, 0)

                if tick % 12 == 0:  # Log every ~minute at 5s intervals
                    remaining = max(0, deadline - time.time())
                    print(f"  [tick {tick}] wrote {len(rows)} rows | {remaining/60:.1f}m remaining")

                if time.time() < deadline:
                    time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\nCollection interrupted after {tick} ticks. Data saved to {output_csv}")

    # Summary
    if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
        print(f"\nCollection complete: {len(df)} total rows in {output_csv}")
        print(f"  Pairs: {df['pair_id'].nunique()}")
        print(f"  Time range: {df['timestamp'].min()} -> {df['timestamp'].max()}")
    return output_csv


def main():
    parser = argparse.ArgumentParser(description="Lead-lag price series collector")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION,
                        help="Collection duration in minutes (default: 60)")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help="Polling interval in seconds (default: 5)")
    parser.add_argument("--max-pairs", type=int, default=DEFAULT_MAX_PAIRS,
                        help="Max number of tracked pairs to poll (default: 5)")
    parser.add_argument("--pairs-csv", type=str, default=TRACKED_PAIRS_CSV,
                        help="Path to tracked pairs CSV")
    parser.add_argument("--output", type=str, default=OUTPUT_CSV,
                        help="Output CSV path")
    args = parser.parse_args()

    run_collector(
        interval_sec=args.interval,
        duration_min=args.duration,
        max_pairs=args.max_pairs,
        pairs_csv=args.pairs_csv,
        output_csv=args.output,
    )


if __name__ == "__main__":
    main()
