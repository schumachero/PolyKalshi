import time
import requests
import pandas as pd
import os

# Platforms
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLY_GAMMA_BASE = "https://gamma-api.polymarket.com"
POLY_CLOB_BASE = "https://clob.polymarket.com"

# IO
MATCHES_CSV = "Data/candidate_series_matches.csv"
OUTPUT_CSV = "Data/matched_orderbooks.csv"

# Request settings
DELAY = 0.25
SESSION = requests.Session()

def safe_get(url, params=None):
    time.sleep(DELAY)
    try:
        r = SESSION.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_kalshi_orderbook(ticker):
    url = f"{KALSHI_BASE}/markets/{ticker}/orderbook"
    data = safe_get(url)
    if not data or "orderbook_fp" not in data:
        # Fallback to orderbook if exists
        ob = data.get("orderbook", {}) if data else {}
        yes_bids = ob.get("yes", [])
        no_bids = ob.get("no", [])
    else:
        ob = data["orderbook_fp"]
        yes_bids = ob.get("yes_dollars", [])
        no_bids = ob.get("no_dollars", [])
    
    if not yes_bids and not no_bids:
        return None
    
    # Kalshi returns bids only. Yes bid at X is No ask at 100-X.
    # Prices in yes_dollars/no_dollars are often strings like '0.4500'
    
    def get_best_price(bids):
        if not bids: return None
        # Bids are usually sorted by price descending? 
        # For Kalshi, they are [['price', 'quantity'], ...]
        # Let's find the max price
        prices = [float(b[0]) for b in bids]
        return max(prices)

    best_yes_bid = get_best_price(yes_bids)
    best_no_bid = get_best_price(no_bids)
    
    # Convert to 0-100 scale
    s_best_yes_bid = best_yes_bid * 100 if best_yes_bid is not None else None
    s_best_no_bid = best_no_bid * 100 if best_no_bid is not None else None
    
    best_yes_ask = 100 - s_best_no_bid if s_best_no_bid is not None else None
    best_no_ask = 100 - s_best_yes_bid if s_best_yes_bid is not None else None
    
    return {
        "k_yes_bid": s_best_yes_bid,
        "k_yes_ask": best_yes_ask,
        "k_no_bid": s_best_no_bid,
        "k_no_ask": best_no_ask
    }

import json

def get_poly_orderbook(market_id):
    # Step 1: Get market detail to get tokens
    market_url = f"{POLY_GAMMA_BASE}/markets/{market_id}"
    market_data = safe_get(market_url)
    if not market_data:
        return None
    
    tokens = market_data.get("clobTokenIds")
    
    # Handle case where clobTokenIds might be a string representation of a list
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except:
            pass

    if not tokens:
        # Try finding in outcomeTokens
        tokens = [t.get("token_id") for t in market_data.get("outcomeTokens", []) if t.get("token_id")]
    
    if not tokens or not isinstance(tokens, list) or len(tokens) < 2:
        print(f"No tokens found for Poly market {market_id}")
        return None
    
    # Usually tokens[0] is YES, tokens[1] is NO
    yes_token = tokens[0]
    no_token = tokens[1]
    
    # Step 2: Get orderbooks from CLOB
    def fetch_best_book(token_id):
        url = f"{POLY_CLOB_BASE}/book"
        res = safe_get(url, params={"token_id": token_id})
        if not res: return None, None
        
        bids = res.get("bids", [])
        asks = res.get("asks", [])
        
        best_bid = float(bids[0].get("price")) if bids else None
        best_ask = float(asks[0].get("price")) if asks else None
        return best_bid, best_ask

    p_yes_bid, p_yes_ask = fetch_best_book(yes_token)
    p_no_bid, p_no_ask = fetch_best_book(no_token)
    
    return {
        "p_yes_bid": p_yes_bid * 100 if p_yes_bid is not None else None,
        "p_yes_ask": p_yes_ask * 100 if p_yes_ask is not None else None,
        "p_no_bid": p_no_bid * 100 if p_no_bid is not None else None,
        "p_no_ask": p_no_ask * 100 if p_no_ask is not None else None
    }

def main():
    if not os.path.exists(MATCHES_CSV):
        print(f"{MATCHES_CSV} not found.")
        return

    df = pd.read_csv(MATCHES_CSV)
    print(f"Processing {len(df)} matches...")
    
    results = []
    
    for i, row in df.iterrows():
        k_ticker = row["kalshi_market_ticker"]
        p_ticker = row["polymarket_market_ticker"]
        
        print(f"[{i+1}/{len(df)}] Fetching {k_ticker} and {p_ticker}...")
        
        k_data = get_kalshi_orderbook(k_ticker)
        p_data = get_poly_orderbook(p_ticker)
        
        res_row = row.to_dict()
        if k_data: res_row.update(k_data)
        if p_data: res_row.update(p_data)
        
        results.append(res_row)
        
    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Results saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
