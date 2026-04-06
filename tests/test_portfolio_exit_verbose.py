import os
import sys
import pandas as pd
import json

# Add src to Python Path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from apis.orderbook import get_matched_orderbooks
from arbitrage_calculator import calculate_exit_opportunities

def test_portfolio_exit_verbose():
    csv_path = "Data/portfolio.csv"
    if not os.path.exists(csv_path):
        print("portfolio.csv not found! Please run the dashboard or history logger first.")
        return

    df = pd.read_csv(csv_path)
    
    # 1. Gather matched Kalshi positions
    k_match = df[df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side_df = df[df['Platform'] == 'Polymarket']
    
    if k_match.empty:
        print("No matched positions found in the portfolio. Cannot evaluate exit.")
        return

    print("--- VERBOSE: Evaluating Exit Opportunities (Min Revenue > 0.00c) ---\n")
    
    found_matches = False
    
    for _, k_row in k_match.iterrows():
        p_row = p_side_df[p_side_df['Ticker'] == k_row['Matched_Ticker']]
        if p_row.empty: 
            continue
            
        found_matches = True
        p_row = p_row.iloc[0]
        
        k_ticker = k_row['Ticker']
        k_side = k_row['Side']
        k_qty = k_row['Quantity']
        
        p_ticker = p_row['Ticker']
        p_side = p_row['Side']
        p_qty = p_row['Quantity']
        
        print("-" * 60)
        print(f"Position Pair: {k_row['Title']}")
        print(f"Holdings: {k_qty} {k_side} on Kalshi | {p_qty} {p_side} on Polymarket")
        
        # 2. Fetch Orderbook
        try:
            obs = get_matched_orderbooks(k_ticker, p_ticker, levels=10)
            
            # Extract applicable live Bid ladders
            k_bids = obs.get("kalshi", {}).get(k_side.lower(), {}).get("bids", [])
            p_bids = obs.get("polymarket", {}).get(p_side.lower(), {}).get("bids", [])
            
            # Fallbacks when strings vs lists are loaded from API dict
            if isinstance(k_bids, str): k_bids = json.loads(k_bids)
            if isinstance(p_bids, str): p_bids = json.loads(p_bids)
            
            # Restructure arrays to (Price_Cents, Size)
            formatted_k_bids = [(b['price'] * 100.0, b['volume']) for b in k_bids]
            formatted_p_bids = [(b['price'] * 100.0, b['volume']) for b in p_bids]
            
            # Predict Polymarket fee category dynamically from title
            from arbitrage_calculator import get_polymarket_fee_category
            pm_tag = get_polymarket_fee_category(str(k_row.get("Title", "")), "")
            
            # 3. Calculate Exit using floor = 0.00
            # This forces the loop to yield everywhere liquidity actually exists!
            exit_tranches = calculate_exit_opportunities(
                formatted_k_bids, 
                formatted_p_bids, 
                pm_category=pm_tag, 
                min_revenue=0.0  # Show everything
            )
            
            if not exit_tranches:
                print("  => No bids found at all on the orderbooks!\n")
            else:
                print(f"  => PM Fee Category detected as: {pm_tag}")
                print(f"  => Found {len(exit_tranches)} available exit tranches in orderbook depth:")
                
                # We'll print up to the first 5 tranches to show depth behavior
                for i, tranche in enumerate(exit_tranches[:5]):
                    print(f"     Tranche {i+1}: Sell {tranche['qty']:>6.2f} contracts -> Receive {tranche['combined_net_revenue']:>6.2f}c net")
                    print(f"                (Kalshi Base Bid @ {tranche['kalshi_price']}c -> Net: {tranche['kalshi_net']}c)")
                    print(f"                (Polymarket Base Bid @ {tranche['polymarket_price']}c -> Net: {tranche['polymarket_net']}c)")
                if len(exit_tranches) > 5:
                    print("     ... (more tranches available down the ladder)")
            print()
                
        except Exception as e:
            print(f"Error evaluating orderbooks for {k_ticker}: {e}\n")
            
    if not found_matches:
        print("Your Kalshi portfolio matched no Polymarket positions.")

if __name__ == "__main__":
    test_portfolio_exit_verbose()
