import os
import sys
import pandas as pd
import json

# Add src to Python Path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from apis.orderbook import get_matched_orderbooks
from arbitrage_calculator import calculate_exit_opportunities

def test_portfolio_exit_live():
    csv_path = "Data/portfolio.csv"
    if not os.path.exists(csv_path):
        print("portfolio.csv not found! Please run the dashboard or history logger first.")
        return

    df = pd.read_csv(csv_path)
    
    # 1. Gather all matched Kalshi positions
    k_match = df[df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side_df = df[df['Platform'] == 'Polymarket']
    
    if k_match.empty:
        print("No matched positions found in the portfolio. Cannot evaluate exit.")
        return

    print("--- Evaluating Exit Opportunities (Min Revenue > 90.00c) ---\n")
    
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
        
        print(f"Position Pair: {k_row['Title']}")
        print(f"  Holdings : {k_qty} {k_side} on Kalshi | {p_qty} {p_side} on Polymarket")
        
        # 2. Fetch Orderbook
        try:
            obs = get_matched_orderbooks(k_ticker, p_ticker, levels=10) # 10 levels deep is sufficient
            
            # 3. We hold positions, so to exit, we must SELL them to the BIDS ladder on our side
            k_bids = obs.get("kalshi", {}).get(k_side.lower(), {}).get("bids", [])
            p_bids = obs.get("polymarket", {}).get(p_side.lower(), {}).get("bids", [])
            
            # Format explicitly for parse_orderbook_side format: list of tuples
            formatted_k_bids = [(b['price'] * 100.0, b['volume']) for b in k_bids]
            formatted_p_bids = [(b['price'] * 100.0, b['volume']) for b in p_bids]
            
            # We also need polymarket category (we don't have it natively here so we assume "Other / General" 
            # or try to extract it from title as in the system)
            from arbitrage_calculator import get_polymarket_fee_category
            pm_tag = get_polymarket_fee_category(str(k_row.get("Title", "")), "") # Approximation
            
            # 4. Calculate Exit
            exit_tranches = calculate_exit_opportunities(
                formatted_k_bids, 
                formatted_p_bids, 
                pm_category=pm_tag, 
                min_revenue=90.0  # Test threshold parameter!
            )
            
            if not exit_tranches:
                print("  => No exit opportunities found above 90 cents net revenue.\n")
            else:
                print("  => Exit Opportunities Found:")
                for tranche in exit_tranches:
                    print(f"     sell {tranche['qty']:>6.2f} contracts -> receive {tranche['combined_net_revenue']:>6.2f}c net "
                          f"(K@{tranche['kalshi_price']}c P@{tranche['polymarket_price']}c)")
                print()
                
        except Exception as e:
            print(f"Error evaluating orderbooks for {k_ticker}: {e}\n")
            
    if not found_matches:
        print("Your Kalshi portfolio matched no Polymarket positions.")

if __name__ == "__main__":
    test_portfolio_exit_live()
