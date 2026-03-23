import os
import time
import sys
import argparse
import pandas as pd
from pathlib import Path

# Ensure we can import from the same directory (src)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from apis.kalshi_api import main as run_kalshi_api
from apis.polymarket_api import main as run_polymarket_api
from matching.matching import main as run_matching
from matching.semantic_matching import rescore_existing_matches
from apis.orderbook import run_batch_fetch as run_fetcher
from arbitrage_calculator import calculate_arbitrage as run_calculator

# Configuration
KALSHI_CSV = "Data/kalshi_markets.csv"
POLYMARKET_CSV = "Data/polymarket_markets.csv"
MATCHES_CSV = "Data/candidate_series_matches.csv"
MAX_AGE_SECONDS = 36000  # 10 hours

def is_file_updated(filepath, max_age):
    """Checks if a file exists and is newer than max_age (in seconds)."""
    if not os.path.exists(filepath):
        print(f"File {filepath} does not exist.")
        return False
    
    file_age = time.time() - os.path.getmtime(filepath)
    is_updated = file_age < max_age
    
    if is_updated:
        print(f"File {filepath} is up to date ({(file_age / 60):.2f}m old).")
    else:
        print(f"File {filepath} is outdated ({(file_age / 60):.2f}m old).")
    
    return is_updated

def main():
    parser = argparse.ArgumentParser(description="PolyKalshi Orchestrator")
    parser.add_argument("--force", action="store_true", help="Force refresh of all data")
    parser.add_argument("--skip-api", action="store_true", help="Skip fetching data from APIs")
    parser.add_argument("--semantic", action="store_true", help="Use semantic matching for rescoring")
    parser.add_argument("--semantic-threshold", type=float, default=0.40, help="Threshold for semantic matching")
    args = parser.parse_args()

    print("=== STARTING ORCHESTRATOR ===")
    
    if not args.skip_api:
        # Check if we need to fetch new data
        kalshi_ok = is_file_updated(KALSHI_CSV, MAX_AGE_SECONDS) or args.force
        poly_ok = is_file_updated(POLYMARKET_CSV, MAX_AGE_SECONDS) or args.force
        
        if not kalshi_ok or args.force:
            print("\n--- RUNNING KALSHI API ---")
            try:
                run_kalshi_api()
            except Exception as e:
                print(f"Error running Kalshi API: {e}")
            
        if not poly_ok or args.force:
            print("\n--- RUNNING POLYMARKET API ---")
            try:
                run_polymarket_api()
            except Exception as e:
                print(f"Error running Polymarket API: {e}")
    else:
        print("\n--- SKIPPING API FETCH ---")
        
    print("\n--- RUNNING MATCHING ---")
    try:
        run_matching()
    except Exception as e:
        print(f"Error running Matching: {e}")
        return

    if args.semantic:
        print("\n--- RUNNING SEMANTIC RESCORING ---")
        try:
            if os.path.exists(MATCHES_CSV):
                matches_df = pd.read_csv(MATCHES_CSV)
                rescored_df = rescore_existing_matches(
                    matches_df, 
                    threshold=args.semantic_threshold
                )
                rescored_df.to_csv(MATCHES_CSV, index=False)
                print(f"Rescored {len(rescored_df)} matches and saved to {MATCHES_CSV}")
            else:
                print(f"Matches file {MATCHES_CSV} not found. Skipping semantic rescoring.")
        except Exception as e:
            print(f"Error during semantic rescoring: {e}")

    print("\n--- RUNNING ORDERBOOK FETCHER ---")
    try:
        run_fetcher()
    except Exception as e:
        print(f"Error running Orderbook Fetcher: {e}")
    
    print("\n--- RUNNING ARBITRAGE CALCULATOR ---")
    try:
        run_calculator()
    except Exception as e:
        print(f"Error running Arbitrage Calculator: {e}")
    
    print("\n=== ORCHESTRATOR DONE ===")

if __name__ == "__main__":
    main()
