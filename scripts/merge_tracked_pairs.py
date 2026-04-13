"""
Merges the main-branch version of tracked_pairs.csv with non-duplicate entries
from predicted_equivalent_markets.csv.

Steps:
  1. Read the main-branch tracked_pairs.csv content (already fetched via git show)
  2. Read predicted_equivalent_markets.csv
  3. Add entries from predicted_equivalent_markets that are NOT already present
     in tracked_pairs (deduplication key: kalshi_market_ticker)
  4. Write the result back to Data/tracked_pairs.csv
"""

import csv
import io
import subprocess
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRACKED_PAIRS_PATH = os.path.join(REPO_ROOT, "Data", "tracked_pairs.csv")
PREDICTED_PATH = os.path.join(REPO_ROOT, "Data", "predicted_equivalent_markets.csv")

# ---------------------------------------------------------------------------
# 1. Fetch main-branch tracked_pairs.csv via git
# ---------------------------------------------------------------------------
result = subprocess.run(
    ["git", "show", "main:Data/tracked_pairs.csv"],
    cwd=REPO_ROOT,
    capture_output=True,
    text=True,
    check=True,
)
main_csv_text = result.stdout

# Parse main-branch rows
main_reader = csv.DictReader(io.StringIO(main_csv_text))
tracked_fieldnames = main_reader.fieldnames
tracked_rows = list(main_reader)

# Build set of already-tracked kalshi tickers (case-insensitive for safety)
existing_tickers = {row["kalshi_ticker"].strip().upper() for row in tracked_rows}
print(f"Main branch tracked_pairs: {len(tracked_rows)} pairs")
print("Existing tickers:", existing_tickers)

# ---------------------------------------------------------------------------
# 2. Read predicted_equivalent_markets.csv
# ---------------------------------------------------------------------------
with open(PREDICTED_PATH, newline="", encoding="utf-8") as f:
    pred_reader = csv.DictReader(f)
    pred_rows = list(pred_reader)

# De-duplicate predicted rows themselves (keep first occurrence per kalshi_market_ticker)
seen_pred = set()
deduped_pred = []
for row in pred_rows:
    key = row["kalshi_market_ticker"].strip().upper()
    if key not in seen_pred:
        seen_pred.add(key)
        deduped_pred.append(row)

print(f"Predicted equivalent markets (after own dedup): {len(deduped_pred)}")

# ---------------------------------------------------------------------------
# 3. Find new entries not already in tracked_pairs
# ---------------------------------------------------------------------------
new_entries = []
for row in deduped_pred:
    kalshi_ticker = row["kalshi_market_ticker"].strip()
    if kalshi_ticker.upper() in existing_tickers:
        print(f"  SKIP (already tracked): {kalshi_ticker}")
        continue

    # Build a tracked_pairs row from the predicted_equivalent_markets columns
    # pair_id: lowercase kalshi_market_ticker with hyphens → underscores, prepend polymarket slug
    poly_ticker = str(row.get("polymarket_market_ticker", "")).strip()
    pair_id = f"{kalshi_ticker.lower().replace('-', '_')}__{poly_ticker}".strip("_")

    new_row = {
        "pair_id": pair_id,
        "active": "True",
        "kalshi_ticker": kalshi_ticker,
        "kalshi_title": row.get("kalshi_market", "").strip(),
        "kalshi_side_held": "",
        "kalshi_quantity": "",
        "polymarket_ticker": poly_ticker,
        "polymarket_title": row.get("polymarket_market", "").strip(),
        "polymarket_side_held": "",
        "polymarket_quantity": "",
        "close_time": "",
        "match_score": row.get("semantic_score", "").strip(),
        "max_position_per_pair_usd": "100.0",
        "min_profit_pct": "1.0",
        "min_liquidity_usd": "50.0",
        "cooldown_minutes": "30",
        "notes": "added from predicted_equivalent_markets",
    }
    new_entries.append(new_row)
    existing_tickers.add(kalshi_ticker.upper())  # prevent duplicate additions

print(f"\nNew entries to add: {len(new_entries)}")
for e in new_entries:
    print(f"  + {e['kalshi_ticker']}")

# ---------------------------------------------------------------------------
# 4. Write result
# ---------------------------------------------------------------------------
all_rows = tracked_rows + new_entries

with open(TRACKED_PAIRS_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=tracked_fieldnames)
    writer.writeheader()
    writer.writerows(all_rows)

print(f"\nDone. tracked_pairs.csv now has {len(all_rows)} pairs.")
