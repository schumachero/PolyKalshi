import os
import sys
import json
import math
import time
import argparse
import requests
import pandas as pd
import concurrent.futures

# Ensure the root directory is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.append(src_dir)

from src.apis.orderbook import get_matched_orderbooks

# These helpers were removed during refactoring; provide local stubs so older
# tests (candidate / semantic / llm) continue to work without the deleted module.
try:
    from src.arbitrage_calculator import calculate_arbitrage, quick_check_arbitrage, get_best_combo_price
except ImportError:
    def calculate_arbitrage(obs, price_threshold=1.10):
        """Stub: returns a simple best-combo summary."""
        return get_best_combo_price(obs)

    def quick_check_arbitrage(obs, threshold=0.95):
        """Stub: True if best combo price is below threshold."""
        best = get_best_combo_price(obs)
        return best is not None and best["price"] <= threshold

    def get_best_combo_price(obs):
        """
        Computes the cheapest of the two hedge strategies using top-of-book asks.
        Returns {"price": float, "strategy": str} or None if data is missing.
        """
        try:
            k = obs["kalshi"]
            p = obs["polymarket"]
            options = []
            # Strategy A: Buy YES on Kalshi + Buy NO on Polymarket
            k_yes_asks = k["yes"]["asks"]
            p_no_asks  = p["no"]["asks"]
            if k_yes_asks and p_no_asks:
                price = round(k_yes_asks[0]["price"] + p_no_asks[0]["price"], 4)
                options.append({"price": price, "strategy": "K_YES_P_NO"})
            # Strategy B: Buy YES on Polymarket + Buy NO on Kalshi
            p_yes_asks = p["yes"]["asks"]
            k_no_asks  = k["no"]["asks"]
            if p_yes_asks and k_no_asks:
                price = round(p_yes_asks[0]["price"] + k_no_asks[0]["price"], 4)
                options.append({"price": price, "strategy": "P_YES_K_NO"})
            return min(options, key=lambda x: x["price"]) if options else None
        except Exception:
            return None

# =========================
# Configuration
# =========================

# Data file paths
CANDIDATE_MATCHES_CSV = "Data/candidate_series_matches.csv"
SEMANTIC_MATCHES_CSV = "Data/semantic_matches.csv"
PREDICTED_MATCHES_CSV = "Data/predicted_equivalent_markets.csv"
LLM_ALL_PREDICTIONS_CSV = "Data/llm_all_predictions.csv"
DEEP_ARBS_OUTPUT_CSV = "Data/llm_deep_arbs.csv"
FEE_AWARE_ARBS_OUTPUT_CSV = "Data/fee_aware_arbs.csv"

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

# =========================
# Fee-Aware Arb Scan
# =========================

# Kalshi fee constants (taker): ceil(KALSHI_FEE_RATE * C * P * (1-P))
KALSHI_FEE_RATE = 0.07
KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"

# Price window for fee-aware scan
FEE_ARB_MIN = 0.80
FEE_ARB_MAX = 0.96

# Timeout for expiry-date fetches
EXPIRY_FETCH_TIMEOUT = 8


def _kalshi_taker_fee(price: float) -> float:
    """Compute Kalshi taker fee for a single contract at `price` (0-1 scale).
    Formula: ceil(0.07 * 1 * P * (1-P)) in cents, converted back to dollars.
    """
    fee_cents = math.ceil(KALSHI_FEE_RATE * price * (1.0 - price) * 100)
    return fee_cents / 100.0


def _fetch_kalshi_close_time(market_ticker: str) -> str:
    """Returns the close_time string for a Kalshi market, or '' on error."""
    try:
        url = f"{KALSHI_BASE_URL}/markets/{market_ticker}"
        r = requests.get(url, timeout=EXPIRY_FETCH_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        market = data.get("market", {})
        return market.get("close_time", "") or ""
    except Exception:
        return ""


def _fetch_polymarket_end_date(market_id: str) -> str:
    """Returns the endDate string for a Polymarket market, or '' on error."""
    try:
        url = f"{POLYMARKET_GAMMA_URL}/markets/{market_id}"
        r = requests.get(url, timeout=EXPIRY_FETCH_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("endDate", "") or data.get("end_date", "") or ""
    except Exception:
        return ""


def run_fee_aware_arb_scan(limit=None):
    """
    Reads predicted_equivalent_markets.csv (LLM-confirmed equivalent pairs), fetches
    live top-of-book asks for both sides, applies Kalshi taker fees, and saves pairs
    where 0.80 <= net_cost <= 0.96 to fee_aware_arbs.csv sorted cheapest first.
    """
    try:
        df = pd.read_csv(PREDICTED_MATCHES_CSV)
        if df.empty:
            print("Predicted Equivalent Markets CSV empty. Cannot scan.")
            return

        # predicted_equivalent_markets.csv has no prediction_label column — all rows are equivalent
        df = df.drop_duplicates(subset=["kalshi_market_ticker", "polymarket_market_ticker"]).reset_index(drop=True)

        if df.empty:
            print("No equivalent pairs found.")
            return

        if limit and limit > 0:
            df = df.head(limit)

        total = len(df)
        print(f"\n--- Fee-Aware Arb Scan ({total} pairs, threshold: ${FEE_ARB_MIN}–${FEE_ARB_MAX}) ---")
        print("Kalshi fee model: ceil(0.07 * P * (1-P)) per contract (taker)")
        print("Polymarket fee: $0.00 (standard prediction markets)")
        print(f"Fetching {total} orderbooks concurrently...\n")

        candidates = []  # rows that pass price filter

        def process_row(row):
            k_tick = str(row["kalshi_market_ticker"]).strip()
            p_tick = str(row["polymarket_market_ticker"]).strip()
            k_title = str(row.get("kalshi_market", k_tick)).strip().replace("\n", " ").replace("\r", "")
            p_title = str(row.get("polymarket_market", p_tick)).strip().replace("\n", " ").replace("\r", "")

            try:
                obs = get_matched_orderbooks(k_tick, p_tick, levels=1)

                k_yes_asks = obs["kalshi"]["yes"]["asks"]
                k_no_asks = obs["kalshi"]["no"]["asks"]
                p_yes_asks = obs["polymarket"]["yes"]["asks"]
                p_no_asks = obs["polymarket"]["no"]["asks"]

                # Need at least one valid ask on each relevant side to compute a strategy
                results_for_row = []

                # Strategy A: Buy YES on Kalshi + Buy NO on Polymarket
                if k_yes_asks and p_no_asks:
                    k_ask = k_yes_asks[0]["price"]   # Kalshi side (0-1)
                    p_ask = p_no_asks[0]["price"]    # Polymarket side (0-1)
                    k_fee = _kalshi_taker_fee(k_ask)
                    net = round(k_ask + p_ask + k_fee, 4)
                    if FEE_ARB_MIN <= net <= FEE_ARB_MAX:
                        results_for_row.append({
                            "strategy": "K_YES_P_NO",
                            "k_ask": k_ask,
                            "p_ask": p_ask,
                            "kalshi_fee": k_fee,
                            "net_cost": net,
                        })

                # Strategy B: Buy YES on Polymarket + Buy NO on Kalshi
                if p_yes_asks and k_no_asks:
                    p_ask = p_yes_asks[0]["price"]   # Polymarket side (0-1)
                    k_ask = k_no_asks[0]["price"]    # Kalshi side (0-1)
                    k_fee = _kalshi_taker_fee(k_ask)
                    net = round(p_ask + k_ask + k_fee, 4)
                    if FEE_ARB_MIN <= net <= FEE_ARB_MAX:
                        results_for_row.append({
                            "strategy": "P_YES_K_NO",
                            "k_ask": k_ask,
                            "p_ask": p_ask,
                            "kalshi_fee": k_fee,
                            "net_cost": net,
                        })

                if not results_for_row:
                    return None

                # Keep only the cheapest strategy for this pair
                best = min(results_for_row, key=lambda x: x["net_cost"])
                best["kalshi_market_ticker"] = k_tick
                best["kalshi_market"] = k_title
                best["polymarket_market_ticker"] = p_tick
                best["polymarket_market"] = p_title
                return best

            except Exception:
                return None

        # Step 1: concurrent orderbook fetch + price filter
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            rows_to_process = [row for _, row in df.iterrows()]
            futures = [executor.submit(process_row, r) for r in rows_to_process]
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res:
                    candidates.append(res)

        print(f"Found {len(candidates)} candidates in price window. Fetching expiry dates...\n")

        if not candidates:
            print("No arb candidates found in the specified price range.")
            return

        # Step 2: fetch expiry dates concurrently for all candidates
        def fetch_expiry(cand):
            k_tick = cand["kalshi_market_ticker"]
            p_tick = cand["polymarket_market_ticker"]
            cand["kalshi_close_time"] = _fetch_kalshi_close_time(k_tick)
            cand["polymarket_end_date"] = _fetch_polymarket_end_date(p_tick)
            return cand

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            candidates = list(executor.map(fetch_expiry, candidates))

        # Step 3: build and save output
        out_df = pd.DataFrame(candidates)
        column_order = [
            "net_cost", "strategy",
            "kalshi_market_ticker", "kalshi_market", "kalshi_close_time",
            "polymarket_market_ticker", "polymarket_market", "polymarket_end_date",
            "k_ask", "p_ask", "kalshi_fee",
        ]
        # Only include columns that exist
        column_order = [c for c in column_order if c in out_df.columns]
        out_df = out_df[column_order].sort_values(by="net_cost").reset_index(drop=True)

        out_df.to_csv(FEE_AWARE_ARBS_OUTPUT_CSV, index=False)

        print(f"Saved {len(out_df)} opportunities to {FEE_AWARE_ARBS_OUTPUT_CSV}\n")
        print(out_df[["net_cost", "strategy", "kalshi_market_ticker", "polymarket_market_ticker"]].to_string(index=False))

    except FileNotFoundError:
        print(f"{LLM_ALL_PREDICTIONS_CSV} not found.")
    except Exception as e:
        print(f"Error during fee-aware arb scan: {e}")


def main():
    import sys
    # If arguments are provided, use argparse
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description="Standalone Test Runner for PolyKalshi Arbitrage Workflows")
        parser.add_argument("--test", choices=["candidate", "semantic", "llm", "llm_all", "fee_aware", "all"], required=True, help="Which test to run")
        parser.add_argument("--limit", type=int, default=None, help="Limit number of pairs to test (default: no limit for fee_aware, 50 for others)")
        
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
        print("5. Run Fee-Aware Arb Scan (with Kalshi fees, saves to fee_aware_arbs.csv, $0.80-$0.96)")
        print("6. Run All Tests")
        print("7. Exit")

        choice = input("\nSelect an option (1-7): ").strip()
        
        # fee_aware scans all by default; others default to 50
        limit = None
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
            test_choice = 'fee_aware'
        elif choice == '6':
            test_choice = 'all'
        elif choice == '7':
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

    if test_choice in ["fee_aware", "all"]:
        run_fee_aware_arb_scan(limit=limit)

if __name__ == "__main__":
    main()
