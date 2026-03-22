import os
import time
import sys
from pathlib import Path

# Ensure we can import from the same directory (src)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from apis.kalshi_api import main as run_kalshi_api
from apis.polymarket_api import main as run_polymarket_api
from matching.matching import main as run_matching
from orderbook_fetcher import main as run_fetcher
from arbitrage_calculator import calculate_arbitrage as run_calculator

# Configuration
KALSHI_CSV = "Data/kalshi_markets.csv"
POLYMARKET_CSV = "Data/polymarket_markets.csv"
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
    print("=== STARTING ORCHESTRATOR ===")
    
    # Check if we need to fetch new data
    kalshi_ok = is_file_updated(KALSHI_CSV, MAX_AGE_SECONDS)
    poly_ok = is_file_updated(POLYMARKET_CSV, MAX_AGE_SECONDS)
    
    if not kalshi_ok:
        print("\n--- RUNNING KALSHI API ---")
        run_kalshi_api()
        
    if not poly_ok:
        print("\n--- RUNNING POLYMARKET API ---")
        run_polymarket_api()
        
    print("\n--- RUNNING MATCHING ---")
    run_matching()
    
    print("\n--- RUNNING ORDERBOOK FETCHER ---")
    run_fetcher()
    
    print("\n--- RUNNING ARBITRAGE CALCULATOR ---")
    run_calculator()
    
    print("\n=== ORCHESTRATOR DONE ===")

if __name__ == "__main__":
    main()
