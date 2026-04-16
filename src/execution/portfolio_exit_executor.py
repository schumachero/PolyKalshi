import os
import sys
import time
import math
import argparse
from datetime import datetime, timezone

import pandas as pd

# =========================================================
# Path setup
# =========================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(SRC_DIR)

for path in [CURRENT_DIR, SRC_DIR, PROJECT_ROOT]:
    if path not in sys.path:
        sys.path.insert(0, path)

# =========================================================
# Project imports
# =========================================================

from apis.portfolio import get_kalshi_positions, get_polymarket_positions
from apis.orderbook import get_matched_orderbooks

from execution.kalshi_trade import place_limit_order as kalshi_place_limit_order
from execution.polymarket_trade import place_limit_order as polymarket_place_limit_order
from arbitrage_calculator import get_polymarket_fee_category, calculate_polymarket_fee

# =========================================================
# Configuration
# =========================================================

DEFAULT_TRACKED_PAIRS_CSV = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs.csv")
EXECUTION_LOG_CSV = os.path.join(PROJECT_ROOT, "Data", "portfolio_exit_execution_log.csv")

DEFAULT_MIN_CONTRACTS_SELL = 4
DEFAULT_MAX_CONTRACTS_SELL = 50
DEFAULT_CUTOFF_CENTS = 0.9899
DEFAULT_SLEEP_MINUTES = 30

# =========================================================
# Helpers
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)

def append_execution_log(row: dict) -> None:
    ensure_parent_dir(EXECUTION_LOG_CSV)
    df = pd.DataFrame([row])
    write_header = not os.path.exists(EXECUTION_LOG_CSV)
    df.to_csv(EXECUTION_LOG_CSV, mode="a", header=write_header, index=False)

def safe_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

# =========================================================
# Core Logic
# =========================================================

def process_portfolio_exits(
    tracked_pairs_csv: str = DEFAULT_TRACKED_PAIRS_CSV,
    dry_run: bool = True,
    cutoff_cents: float = DEFAULT_CUTOFF_CENTS,
    min_sell: int = DEFAULT_MIN_CONTRACTS_SELL,
    max_sell: int = DEFAULT_MAX_CONTRACTS_SELL,
) -> None:
    
    if not os.path.exists(tracked_pairs_csv):
        print(f"Tracked pairs CSV not found at {tracked_pairs_csv}")
        return

    try:
        df = pd.read_csv(tracked_pairs_csv)
    except Exception as e:
        print(f"Error reading {tracked_pairs_csv}: {e}")
        return

    paired = []
    for _, row in df.iterrows():
        # Only process active pairings
        if 'active' in row and str(row['active']).lower() == 'false':
            continue

        k_ticker = normalize_str(row.get("kalshi_ticker", ""))
        p_ticker = normalize_str(row.get("polymarket_ticker", ""))
        
        if not k_ticker or not p_ticker:
            continue
            
        paired.append({
            "k_ticker": k_ticker,
            "k_side": normalize_str(row.get("kalshi_side_held", "")).lower(),
            "k_quantity": safe_float(row.get("kalshi_quantity", 0)),
            "p_ticker": p_ticker,
            "p_side": normalize_str(row.get("polymarket_side_held", "")).lower(),
            "p_quantity": safe_float(row.get("polymarket_quantity", 0))
        })
        
    print(f"Loaded {len(paired)} matched pairs from {os.path.basename(tracked_pairs_csv)}.")
    if not paired:
        return

    print("Fetching live account positions from APIs...")
    try:
        live_k_pos = get_kalshi_positions()
        live_p_pos = get_polymarket_positions()
    except Exception as e:
        print(f"Error fetching live positions: {e}")
        return

    if not live_k_pos:
        print("Could not fetch Kalshi positions or none exist.")
        return
    if live_p_pos is None:
        print("ERROR: Failed to fetch Polymarket positions (see logs above). Skipping this cycle.")
        return
    if len(live_p_pos) == 0:
        print("Polymarket portfolio is empty. Nothing to exit.")
        return

    for pair in paired:
        pair_id = f"{pair['k_ticker']}__{pair['p_ticker']}"
        
        # Determine actual owned amounts via API
        live_k = next((p for p in live_k_pos if normalize_str(p.get("ticker")) == pair["k_ticker"]), None)
        live_p = next((
            p for p in live_p_pos if 
            normalize_str(p.get("market_id")) == pair["p_ticker"] or 
            normalize_str(p.get("numeric_id")) == pair["p_ticker"]
        ), None)
        
        if not live_k:
            print(f"[{pair_id}] Skipped: Ticker {pair['k_ticker']} not found in live Kalshi positions.")
            continue
        if not live_p:
            print(f"[{pair_id}] Skipped: Ticker {pair['p_ticker']} not found in live Polymarket positions.")
            continue
            
        real_k_owned = safe_float(live_k.get("quantity"))
        real_p_owned = safe_float(live_p.get("size", live_p.get("quantity", 0)))
        
        if real_k_owned < min_sell or real_p_owned < min_sell:
            print(f"[{pair_id}] Skipped: Insufficient quantity owned (K: {real_k_owned}, P: {real_p_owned}, Min: {min_sell})")
            continue
            
        # Get live orderbooks (depth 1)
        try:
            obs = get_matched_orderbooks(pair["k_ticker"], pair["p_ticker"], levels=1)
        except Exception as e:
            print(f"[{pair_id}] Error fetching orderbooks: {e}")
            continue
            
        # Dynamically resolve sides if missing in CSV (use live position data)
        k_side = pair["k_side"] or normalize_str(live_k.get("side")).lower()
        p_side = pair["p_side"] or normalize_str(live_p.get("side")).lower()

        if not k_side or not p_side:
            print(f"[{pair_id}] Skipped: Could not determine side held (K: '{k_side}', P: '{p_side}')")
            continue

        k_bids = obs.get("kalshi", {}).get(k_side, {}).get("bids", [])
        p_bids = obs.get("polymarket", {}).get(p_side, {}).get("bids", [])
        
        if not k_bids or not p_bids:
            # Provide more detail on which book is missing bids
            k_len = len(k_bids)
            p_len = len(p_bids)
            print(f"[{pair_id}] Skipped: Missing bids on side {k_side.upper()}/{p_side.upper()} (K: {k_len}, P: {p_len})")
            continue
            
        k_bid_price = safe_float(k_bids[0]["price"])
        k_bid_vol = safe_float(k_bids[0]["volume"])
        
        p_bid_price = safe_float(p_bids[0]["price"])
        p_bid_vol = safe_float(p_bids[0]["volume"])
        
        # Calculate fee-adjusted Net Bids (we receive less when hitting a bid)
        k_fee_dollar = 0.07 * k_bid_price * (1.0 - k_bid_price) if k_bid_price > 0.0 else 0.0
        k_bid_net = k_bid_price - k_fee_dollar
        
        p_title = normalize_str(live_p.get("title", ""))
        pm_category = get_polymarket_fee_category(p_title, "")
        p_fee_dollar = calculate_polymarket_fee(p_bid_price, pm_category)
        p_bid_net = p_bid_price - p_fee_dollar
        
        sum_bid_price = k_bid_price + p_bid_price
        sum_net_bid_price = k_bid_net + p_bid_net
        
        if sum_net_bid_price < cutoff_cents:
            print(f"[{pair_id}] Skipped: Combined NET bid {sum_net_bid_price:.4f} (Raw: {sum_bid_price:.4f}) below cutoff {cutoff_cents:.4f}")
            continue
            
        # Calculate exactly how many we can sell simultaneously
        executable_contracts = int(math.floor(min(
            k_bid_vol, 
            p_bid_vol, 
            real_k_owned, 
            real_p_owned,
            max_sell
        )))
        
        if executable_contracts < min_sell:
            print(f"[{pair_id}] Skipped: Combined executable volume {executable_contracts} below minimum {min_sell} (K_vol: {k_bid_vol}, P_vol: {p_bid_vol})")
            continue
            
        print(
            f"\n[{pair_id}] EXIT TARGET HIT | "
            f"Kalshi {k_side.upper()} BID @ {k_bid_price:.4f} (Net: {k_bid_net:.4f}) + "
            f"Poly {p_side.upper()} BID @ {p_bid_price:.4f} (Net: {p_bid_net:.4f}) = "
            f"{sum_bid_price:.4f} RAW / {sum_net_bid_price:.4f} NET | Size: {executable_contracts}"
        )
        
        kalshi_price_cents = int(round(k_bid_price * 100))
        polymarket_price = round(p_bid_price, 6)
        
        if not (1 <= kalshi_price_cents <= 99):
            print(f"[{pair_id}] Skipped: Kalshi raw price {kalshi_price_cents} out of 1-99 bounds")
            continue
            
        if not (0 < polymarket_price < 1):
            print(f"[{pair_id}] Skipped: Polymarket raw price {polymarket_price:.4f} out of 0-1 bounds")
            continue
        
        if dry_run:
            print(f"[{pair_id}] DRY RUN: Execution candidate found. Would sell {executable_contracts} contracts.")
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "dry_run_candidate",
                "kalshi_ticker": pair["k_ticker"],
                "polymarket_ticker": pair["p_ticker"],
                "kalshi_side": k_side,
                "polymarket_outcome": p_side,
                "kalshi_bid_price": k_bid_price,
                "polymarket_bid_price": p_bid_price,
                "sum_bid_price": sum_bid_price,
                "contracts_to_sell": executable_contracts,
                "message": "Candidate found; dry run only"
            })
            continue

        # Place the physical trades
        try:
            kalshi_resp = kalshi_place_limit_order(
                ticker=pair["k_ticker"],
                side=k_side,
                action="sell",
                count=executable_contracts,
                price_cents=kalshi_price_cents,
                time_in_force="fill_or_kill"
            )
            
            poly_resp = polymarket_place_limit_order(
                slug=pair["p_ticker"],
                outcome=p_side.upper(),
                size=executable_contracts,
                price=polymarket_price,
                side="SELL",
                order_type="FOK"
            )
            
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "success",
                "kalshi_ticker": pair["k_ticker"],
                "polymarket_ticker": pair["p_ticker"],
                "kalshi_side": k_side,
                "polymarket_outcome": p_side,
                "kalshi_bid_price": k_bid_price,
                "polymarket_bid_price": p_bid_price,
                "sum_bid_price": sum_bid_price,
                "contracts_to_sell": executable_contracts,
                "message": ""
            })
            
            print(f"[{pair_id}] EXECUTED SELL | Checked: {executable_contracts} contracts @ {sum_bid_price:.4f} combined expected payout")
            
        except Exception as e:
            msg = f"ERROR executing dual sell: {e}"
            print(f"[{pair_id}] {msg}")
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "error",
                "kalshi_ticker": pair["k_ticker"],
                "polymarket_ticker": pair["p_ticker"],
                "kalshi_side": k_side,
                "polymarket_outcome": p_side,
                "message": str(e)
            })



# =========================================================
# Run control
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Automated Tracked Pairs Arbitrage Exit Executor")
    parser.add_argument("--input", default=DEFAULT_TRACKED_PAIRS_CSV, help="Path to tracked_pairs.csv")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a loop")
    parser.add_argument("--interval-minutes", type=int, default=DEFAULT_SLEEP_MINUTES, help="Minutes between refreshes")
    parser.add_argument("--live", action="store_true", help="Actually place limit orders (turn off dry run)")
    parser.add_argument("--cutoff-cents", type=float, default=DEFAULT_CUTOFF_CENTS, help="Combined bid price to sell at (e.g. 0.99)")
    parser.add_argument("--min-sell", type=int, default=DEFAULT_MIN_CONTRACTS_SELL, help="Minimum contracts to sell in one slice")
    parser.add_argument("--max-sell", type=int, default=DEFAULT_MAX_CONTRACTS_SELL, help="Maximum contracts to sell at once")
    args = parser.parse_args()

    dry_run = not args.live

    if not args.loop:
        process_portfolio_exits(
            tracked_pairs_csv=args.input,
            dry_run=dry_run,
            cutoff_cents=args.cutoff_cents,
            min_sell=args.min_sell,
            max_sell=args.max_sell
        )
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1

    mode_str = "LIVE EXECUTION" if not dry_run else "DRY RUN"
    print(f"=== STARTING AUTOMATED EXIT MONITOR ({mode_str}) ===")
    print(f"Interval: {args.interval_minutes} min | Cutoff >= ${args.cutoff_cents:.2f}")

    while True:
        cycle_start = time.time()
        print(f"\n\n########## LOOP ITERATION {iteration} ##########")
        
        process_portfolio_exits(
            tracked_pairs_csv=args.input,
            dry_run=dry_run,
            cutoff_cents=args.cutoff_cents,
            min_sell=args.min_sell,
            max_sell=args.max_sell
        )

        elapsed = time.time() - cycle_start
        sleep_seconds = max(interval_seconds - elapsed, 0)

        print(f"\nIteration {iteration} finished in {elapsed:.1f}s")
        print(f"Sleeping for {sleep_seconds / 60:.2f} minutes... Press Ctrl+C to stop.")

        try:
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("\nContinuous mode stopped by user.")
            break

        iteration += 1

if __name__ == "__main__":
    main()
