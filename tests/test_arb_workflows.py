import os
import sys
import json
import time
import argparse
import pandas as pd
import concurrent.futures

# Ensure the root directory is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from src.apis.orderbook import get_matched_orderbooks
from src.arbitrage_calculator import calculate_arbitrage, quick_check_arbitrage, get_best_combo_price

# =========================
# Configuration
# =========================

# Data file paths
CANDIDATE_MATCHES_CSV = "Data/candidate_series_matches.csv"
SEMANTIC_MATCHES_CSV = "Data/semantic_matches.csv"
PREDICTED_MATCHES_CSV = "Data/predicted_equivalent_markets.csv"
LLM_ALL_PREDICTIONS_CSV = "Data/llm_all_predictions.csv"
DEEP_ARBS_OUTPUT_CSV = "Data/llm_deep_arbs.csv"

# Orderbook depth (number of price levels to fetch)
ORDERBOOK_LEVELS = 5
CANDIDATE_ORDERBOOK_LEVELS = 20

# Price thresholds
CANDIDATE_PRICE_THRESHOLD = 1.10
SEMANTIC_PRICE_UPPER = 1.30
LLM_PRICE_UPPER = 0.96
MIN_PRICE_FLOOR = 0.80

# Quick check thresholds
QUICK_CHECK_STRICT = 0.95
QUICK_CHECK_LOOSE = 1.50

# Concurrent workers for parallel orderbook fetching
MAX_WORKERS = 20

# Default test limit
DEFAULT_LIMIT = 50

# =========================
# Tests
# =========================

def run_candidate_matches_test():
    """Simple test to demonstrate Orderbook Merging calculations on top candidate match."""
    try:
        df = pd.read_csv(CANDIDATE_MATCHES_CSV)
        if df.empty:
            print("Matches CSV empty. Cannot test.")
            return
            
        first = df.iloc[0]
        k_tick = first["kalshi_market_ticker"]
        p_tick = first["polymarket_market_ticker"]
        
        print(f"Testing Arbitrage Merged Book logic for:")
        print(f"Kalshi: {k_tick}")
        print(f"Polymarket: {p_tick}")
        print("-" * 50)
        
        obs = get_matched_orderbooks(k_tick, p_tick, levels=CANDIDATE_ORDERBOOK_LEVELS)
        
        results = calculate_arbitrage(obs, price_threshold=CANDIDATE_PRICE_THRESHOLD)
        
        print(f"\nMax volume available below ${CANDIDATE_PRICE_THRESHOLD} marginal cost:")
        print(json.dumps(results, indent=2))
        
        # Test Quick Check
        quick_strict = quick_check_arbitrage(obs, threshold=QUICK_CHECK_STRICT)
        quick_loose = quick_check_arbitrage(obs, threshold=QUICK_CHECK_LOOSE)
        
        print("\n--- Quick Check Tests ---")
        print(f"Quick check with strict threshold (0.95): {quick_strict}")
        print(f"Quick check with loose threshold (1.50): {quick_loose}")
        
    except FileNotFoundError:
        print("Matches CSV not found. Ensure you have run matching phase first.")
    except Exception as e:
        print(f"Unexpected error: {e}")

def run_semantic_matches_test(limit=50):
    """Reads top N semantic matches, fetches orderbooks, and checks arb potential."""
    try:
        df = pd.read_csv(SEMANTIC_MATCHES_CSV)
        if df.empty:
            print("Matches CSV empty. Cannot test.")
            return
            
        top_n = df.head(limit)
        
        print(f"\n--- Testing Top {limit} Semantic Matches for Arbitrage (< $1.30) ---")
        matches_found = 0
        
        for idx, row in top_n.iterrows():
            k_tick = str(row["kalshi_market_ticker"]).strip()
            p_tick = str(row["polymarket_market_ticker"]).strip()
            k_title = str(row.get("kalshi_market", k_tick)).strip().replace('\n', ' ').replace('\r', '')
            p_title = str(row.get("polymarket_market", p_tick)).strip().replace('\n', ' ').replace('\r', '')
            score = row.get("semantic_score", 0)
            
            obs = get_matched_orderbooks(k_tick, p_tick, levels=ORDERBOOK_LEVELS)
            best = get_best_combo_price(obs)
            
            if best and MIN_PRICE_FLOOR <= best["price"] <= SEMANTIC_PRICE_UPPER:
                print(f"\n[MATCH FOUND! Score: {score}]")
                print(f"Kalshi: {k_title} ({k_tick})")
                print(f"Polymarket: {p_title} ({p_tick})")
                print(f"-> Price: ${best['price']} ({best['strategy']})")
                matches_found += 1
                
            time.sleep(0.2)
            
        print(f"\nDone. Found {matches_found} potential arbs below ${SEMANTIC_PRICE_UPPER} out of {limit}.")
        
    except FileNotFoundError:
        print("Data/semantic_matches.csv not found.")
    except Exception as e:
        print(f"Error during semantic matches test: {e}")

def run_llm_predicted_matches_test(limit=None):
    """Reads predicted_equivalent_markets.csv, fetches orderbooks, and checks arb potential below $0.96."""
    try:
        df = pd.read_csv(PREDICTED_MATCHES_CSV)
        if df.empty:
            print("Predicted Matches CSV empty. Cannot test.")
            return
            
        if limit and limit > 0:
            df = df.head(limit)
            
        print(f"\n--- Testing LLM Predicted Matches for Deep Arbitrage (< $0.96) ---")
        total = len(df)
        print(f"Fetching {total} markets concurrently. This should be lightning fast...")
        
        arbs_list = []
        
        def process_row(row):
            k_tick = str(row["kalshi_market_ticker"]).strip()
            p_tick = str(row["polymarket_market_ticker"]).strip()
            k_title = str(row.get("kalshi_market", k_tick)).strip().replace('\n', ' ').replace('\r', '')
            p_title = str(row.get("polymarket_market", p_tick)).strip().replace('\n', ' ').replace('\r', '')
            
            try:
                obs = get_matched_orderbooks(k_tick, p_tick, levels=ORDERBOOK_LEVELS)
                best = get_best_combo_price(obs)
                
                if best and MIN_PRICE_FLOOR <= best["price"] <= LLM_PRICE_UPPER:
                    return {
                        "kalshi_market_ticker": k_tick,
                        "kalshi_series_ticker": row.get("kalshi_series_ticker", ""),
                        "kalshi_market": k_title,
                        "polymarket_market_ticker": p_tick,
                        "polymarket_series_ticker": row.get("polymarket_series_ticker", ""),
                        "polymarket_market": p_title,
                        "price": best["price"],
                        "strategy": best["strategy"]
                    }
            except Exception as e:
                pass
            return None
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            rows_to_process = [row for _, row in df.iterrows()]
            futures = [executor.submit(process_row, r) for r in rows_to_process]
            
            matches_found = 0
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res:
                    print(f"\n[DEEP ARB FOUND!]")
                    print(f"Kalshi: {res['kalshi_market']} ({res['kalshi_market_ticker']})")
                    print(f"Polymarket: {res['polymarket_market']} ({res['polymarket_market_ticker']})")
                    print(f"-> Price: ${res['price']} ({res['strategy']})")
                    arbs_list.append(res)
                    matches_found += 1
            
        print(f"\nDone. Found {matches_found} potential arbs below ${LLM_PRICE_UPPER} out of {total}.")
        
        if arbs_list:
            out_df = pd.DataFrame(arbs_list)
            out_df.sort_values(by="price", inplace=True)
            out_path = DEEP_ARBS_OUTPUT_CSV
            out_df.to_csv(out_path, index=False)
            print(f"Saved sorted arbitrages to {out_path}")
            
    except FileNotFoundError:
        print(f"{PREDICTED_MATCHES_CSV} not found.")
    except Exception as e:
        print(f"Error during LLM predicted matches test: {e}")

def run_llm_all_predictions_test(limit=None):
    """Reads llm_all_predictions.csv, filters for Equivalent pairs, fetches orderbooks, and checks arb potential."""
    try:
        df = pd.read_csv(LLM_ALL_PREDICTIONS_CSV)
        if df.empty:
            print("LLM All Predictions CSV empty. Cannot test.")
            return
        
        # Filter to only LLM-predicted equivalent pairs
        df = df[df["prediction_label"] == "Equivalent"].copy()
        # Drop duplicates (the file may contain duplicate runs)
        df = df.drop_duplicates(subset=["kalshi_market_ticker", "polymarket_market_ticker"]).reset_index(drop=True)
        
        if df.empty:
            print("No Equivalent pairs found in llm_all_predictions.csv.")
            return
            
        if limit and limit > 0:
            df = df.head(limit)
            
        print(f"\n--- Testing LLM All Predictions (Equivalent only) for Deep Arbitrage (< ${LLM_PRICE_UPPER}) ---")
        total = len(df)
        print(f"Fetching {total} markets concurrently...")
        
        arbs_list = []
        
        def process_row(row):
            k_tick = str(row["kalshi_market_ticker"]).strip()
            p_tick = str(row["polymarket_market_ticker"]).strip()
            k_title = str(row.get("kalshi_market", k_tick)).strip().replace('\n', ' ').replace('\r', '')
            p_title = str(row.get("polymarket_market", p_tick)).strip().replace('\n', ' ').replace('\r', '')
            
            try:
                obs = get_matched_orderbooks(k_tick, p_tick, levels=ORDERBOOK_LEVELS)
                best = get_best_combo_price(obs)
                
                if best and MIN_PRICE_FLOOR <= best["price"] <= LLM_PRICE_UPPER:
                    return {
                        "kalshi_market_ticker": k_tick,
                        "kalshi_series_ticker": row.get("kalshi_series_ticker", ""),
                        "kalshi_market": k_title,
                        "polymarket_market_ticker": p_tick,
                        "polymarket_series_ticker": row.get("polymarket_series_ticker", ""),
                        "polymarket_market": p_title,
                        "price": best["price"],
                        "strategy": best["strategy"]
                    }
            except Exception as e:
                pass
            return None
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            rows_to_process = [row for _, row in df.iterrows()]
            futures = [executor.submit(process_row, r) for r in rows_to_process]
            
            matches_found = 0
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res:
                    print(f"\n[DEEP ARB FOUND!]")
                    print(f"Kalshi: {res['kalshi_market']} ({res['kalshi_market_ticker']})")
                    print(f"Polymarket: {res['polymarket_market']} ({res['polymarket_market_ticker']})")
                    print(f"-> Price: ${res['price']} ({res['strategy']})")
                    arbs_list.append(res)
                    matches_found += 1
            
        print(f"\nDone. Found {matches_found} potential arbs below ${LLM_PRICE_UPPER} out of {total}.")
        
        if arbs_list:
            out_df = pd.DataFrame(arbs_list)
            out_df.sort_values(by="price", inplace=True)
            out_path = DEEP_ARBS_OUTPUT_CSV
            out_df.to_csv(out_path, index=False)
            print(f"Saved sorted arbitrages to {out_path}")
            
    except FileNotFoundError:
        print(f"{LLM_ALL_PREDICTIONS_CSV} not found.")
    except Exception as e:
        print(f"Error during LLM all predictions test: {e}")

def main():
    import sys
    # If arguments are provided, use argparse
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description="Standalone Test Runner for PolyKalshi Arbitrage Workflows")
        parser.add_argument("--test", choices=["candidate", "semantic", "llm", "llm_all", "all"], required=True, help="Which test to run")
        parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Limit of matches for the semantic test")
        
        args = parser.parse_args()
        test_choice = args.test
        limit = args.limit
    else:
        # Interactive mode
        print("\n=== PolyKalshi Arbitrage Test Runner ===")
        print("1. Run Candidate Merging Test (tests first match from candidate_series_matches.csv)")
        print("2. Run Top Semantic Matches Test (tests N matches from semantic_matches.csv)")
        print("3. Run LLM Predicted Matches Test (tests < $0.96 arb on predicted_equivalent_markets.csv)")
        print("4. Run LLM All Predictions Test (tests < $0.96 arb on llm_all_predictions.csv, Equivalent only)")
        print("5. Run All Tests")
        print("6. Exit")
        
        choice = input("\nSelect an option (1-6): ").strip()
        
        limit = 50
        if choice == '1':
            test_choice = 'candidate'
        elif choice == '2':
            test_choice = 'semantic'
            try:
                limit_input = input("How many semantic matches to test? (default: 50): ").strip()
                limit = int(limit_input) if limit_input else 50
            except ValueError:
                print("Invalid input, using default limit of 50.")
        elif choice == '3':
            test_choice = 'llm'
        elif choice == '4':
            test_choice = 'llm_all'
        elif choice == '5':
            test_choice = 'all'
        elif choice == '6':
            print("Exiting.")
            sys.exit(0)
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)
            
    if test_choice in ["candidate", "all"]:
        run_candidate_matches_test()
        
    if test_choice in ["semantic", "all"]:
        run_semantic_matches_test(limit=limit)

    if test_choice in ["llm", "all"]:
        run_llm_predicted_matches_test(limit=limit)

    if test_choice in ["llm_all", "all"]:
        run_llm_all_predictions_test(limit=limit)

if __name__ == "__main__":
    main()
