import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from arbitrage_calculator import calculate_exit_opportunities, parse_orderbook_side

def test_exit_opportunities():
    # Mock Bid Data
    # Best bids are the highest prices
    raw_k_bids = json.dumps([
        {"price": 50, "size": 10},
        {"price": 49, "size": 20},
        {"price": 48, "size": 30}
    ])
    
    raw_p_bids = json.dumps([
        {"price": 51, "size": 5},
        {"price": 50, "size": 15},
        {"price": 49, "size": 50}
    ])
    
    # 1. Test parsing with reverse sorting (Highest first for bids)
    k_bids = parse_orderbook_side(raw_k_bids, is_bid=True)
    p_bids = parse_orderbook_side(raw_p_bids, is_bid=True)
    
    print("Parsed Kalshi Bids:", k_bids)
    print("Parsed Polymarket Bids:", p_bids)
    
    # 2. Run the exit calculator
    # Kalshi Fee at 50 cents = 0.07 * 0.5 * 0.5 = $0.0175 (1.75 cents) -> Net = 48.25
    # Poly Fee at 51 cents (Politics) = 0.04 * (0.51 * 0.49)^1 = ~0.01 -> Net = 50.00
    # Combined net revenue should be around 98.25
    opportunities = calculate_exit_opportunities(k_bids, p_bids, pm_category="Politics", min_revenue=97.0)
    
    print("\nCalculated Exit Tranches:")
    for opp in opportunities:
        print(json.dumps(opp, indent=2))

if __name__ == "__main__":
    test_exit_opportunities()
