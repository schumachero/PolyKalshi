import sys
import os
import pandas as pd
import json

# Add src to pythonpath
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from apis.orderbook import get_matched_orderbooks, scale_book_to_cents
from arbitrage_calculator import calculate_arbitrage

def test_specific_market():
    kalshi_ticker = "KXNETANYAHUPARDON-26-JUL01"
    polymarket_ticker = "will-netanyahu-be-pardoned-by-june-30"
    
    print(f"Fetching real-time orderbooks for:\n- Kalshi: {kalshi_ticker}\n- Polymarket: {polymarket_ticker}\n")
    
    # 1. Fetch Orderbooks using main function (fetches 50 levels by default)
    obs = get_matched_orderbooks(kalshi_ticker, polymarket_ticker, levels=50)
    
    k_yes_bids = obs["kalshi"]["yes"]["bids"]
    k_yes_asks = obs["kalshi"]["yes"]["asks"]
    k_no_bids = obs["kalshi"]["no"]["bids"]
    k_no_asks = obs["kalshi"]["no"]["asks"]
    
    p_yes_bids = obs["polymarket"]["yes"]["bids"]
    p_yes_asks = obs["polymarket"]["yes"]["asks"]
    p_no_bids = obs["polymarket"]["no"]["bids"]
    p_no_asks = obs["polymarket"]["no"]["asks"]
    
    # 2. Mock a dataframe row to look exactly like the output from `run_batch_fetch`
    row = {
        "kalshi_market_ticker": kalshi_ticker,
        "polymarket_market_ticker": polymarket_ticker,
        
        # Adding titles so Polymarket Fee Logic deduces category (Politics/Geopolitics)
        "polymarket_market": "Will Netanyahu be pardoned?",
        "polymarket_series": "Pardon June 30",
        
        # Add mock resolution times (roughly 4 months away ~ 120 days)
        "kalshi_close_time": "2026-07-01T00:00:00Z",
        "polymarket_close_time": "2026-06-30T00:00:00Z",
        
        # Depth
        "k_yes_asks": json.dumps(scale_book_to_cents(k_yes_asks)),
        "k_no_asks": json.dumps(scale_book_to_cents(k_no_asks)),
        "p_yes_asks": json.dumps(scale_book_to_cents(p_yes_asks)),
        "p_no_asks": json.dumps(scale_book_to_cents(p_no_asks)),
    }
    
    # 3. Create DataFrame
    df = pd.DataFrame([row])
    
    # 4. Call our dynamically parameterized calculate_arbitrage Function
    results_df = calculate_arbitrage(input_data=df, output_csv=None, return_df=True)
    
    if results_df is None or results_df.empty:
        print("\nResult: No profitable arbitrage paths found that meet minimum constraints (0.1% Profit).")
    else:
        print(f"\nResult: Found {len(results_df)} potential setup(s)!")
        for idx, res in results_df.iterrows():
            print("\n-----")
            print(f"Direction (Side to Buy): {res['direction']}")
            print(f"Average Kalshi Base Ask (Cents): {res['avg_k_price']}")
            print(f"Average Polymarket Base Ask (Cents): {res['avg_p_price']}")
            print(f"Total Blended Cost AFTER platform fees: {res['total_cost']}")
            print(f"Expected Net Profit: {res['expected_profit']}%")
            print(f"Available Safe Contracts: {res['contracts']}")
            print(f"Required Profit to execute: {res['required_profit_pct']}%")

if __name__ == "__main__":
    test_specific_market()
