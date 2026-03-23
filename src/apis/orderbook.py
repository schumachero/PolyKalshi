import requests
import pandas as pd
import json
import concurrent.futures
import time
import os

# =========================
# Configuration
# =========================

# API base URLs
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB = "https://clob.polymarket.com"

# Default number of orderbook price levels to fetch
DEFAULT_LEVELS = 20

# HTTP request timeout in seconds
REQUEST_TIMEOUT = 10

# Thread pool workers for concurrent fetching
THREAD_POOL_WORKERS = 2

# =========================
# Orderbook Functions
# =========================

def get_kalshi_orderbook(market_ticker, levels=DEFAULT_LEVELS):
    """
    Fetches the top orderbook levels (bids and asks) for YES and NO from Kalshi.
    Kalshi provides "yes_dollars" (Yes Bids) and "no_dollars" (No Bids).
    Yes Asks are mathematically derived from No Bids (1 - price), and vice versa.
    """
    url = f"{KALSHI_BASE}/markets/{market_ticker}/orderbook"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Error fetching Kalshi orderbook for {market_ticker}: {e}")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}
        
    book_info = data.get("orderbook_fp") or data.get("orderbook", {})
    
    yes_raw = book_info.get("yes_dollars") or book_info.get("yes", [])
    no_raw = book_info.get("no_dollars") or book_info.get("no", [])
    
    def parse_side(side_data):
        parsed = []
        for row in side_data:
            if len(row) >= 2:
                price = float(row[0])
                if 'dollars' not in (book_info.keys() if hasattr(book_info, 'keys') else []) and price > 1:
                    price = price / 100.0
                qty = float(row[1])
                parsed.append({"price": round(price, 4), "volume": round(qty, 2)})
        # Sort by highest bid price first
        parsed.sort(key=lambda x: x["price"], reverse=True)
        return parsed

    yes_bids = parse_side(yes_raw)
    no_bids = parse_side(no_raw)
    
    # Invert to derive asks. A No Bid of $0.80 is a Yes Ask of $0.20
    # Sorting inversion automatically happens because we do `1 - price` 
    # of the highest bids, which gives the lowest asks first.
    yes_asks = [{"price": round(1.0 - p["price"], 4), "volume": p["volume"]} for p in no_bids]
    no_asks = [{"price": round(1.0 - p["price"], 4), "volume": p["volume"]} for p in yes_bids]

    return {
        "yes": {
            "bids": yes_bids[:levels],
            "asks": yes_asks[:levels]
        },
        "no": {
            "bids": no_bids[:levels],
            "asks": no_asks[:levels]
        }
    }

def get_polymarket_orderbook(market_ticker, levels=DEFAULT_LEVELS):
    """
    Fetches the market details to get token IDs, then fetches both bids and asks from CLOB.
    """
    try:
        m_r = requests.get(f"{POLYMARKET_GAMMA}/markets/{market_ticker}", timeout=REQUEST_TIMEOUT)
        m_r.raise_for_status()
        market_data = m_r.json()
    except Exception as e:
        print(f"Error fetching Polymarket market {market_ticker}: {e}")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}
    
    tokens = market_data.get("clobTokenIds", [])
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            pass
            
    if not tokens or len(tokens) < 2:
        print(f"Polymarket market {market_ticker} does not have standard clobTokenIds.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}
        
    yes_token, no_token = tokens[0], tokens[1]
    
    def fetch_clob_book(token_id):
        url = f"{POLYMARKET_CLOB}/book?token_id={token_id}"
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            bids_raw = data.get("bids", [])
            asks_raw = data.get("asks", [])
        except Exception as e:
            print(f"Error fetching Polymarket CLOB for token {token_id}: {e}")
            return {"bids": [], "asks": []}
            
        def parse_array(arr, is_bid):
            parsed = []
            for item in arr:
                price = float(item.get("price", 0))
                qty = float(item.get("size", 0))
                parsed.append({"price": round(price, 4), "volume": round(qty, 2)})
            # Bids: descending (highest first). Asks: ascending (lowest first)
            parsed.sort(key=lambda x: x["price"], reverse=is_bid)
            return parsed[:levels]

        return {
            "bids": parse_array(bids_raw, is_bid=True),
            "asks": parse_array(asks_raw, is_bid=False)
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS) as executor:
        yes_future = executor.submit(fetch_clob_book, yes_token)
        no_future = executor.submit(fetch_clob_book, no_token)
        return {
            "yes": yes_future.result(),
            "no": no_future.result()
        }

def get_matched_orderbooks(kalshi_ticker, polymarket_ticker, levels=DEFAULT_LEVELS):
    """
    Returns structured data containing orderbook levels from both platforms.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS) as executor:
        k_future = executor.submit(get_kalshi_orderbook, kalshi_ticker, levels)
        p_future = executor.submit(get_polymarket_orderbook, polymarket_ticker, levels)
        
        return {
            "kalshi": k_future.result(),
            "polymarket": p_future.result()
        }

def run_batch_fetch(matches_csv="Data/candidate_series_matches.csv", output_csv="Data/matched_orderbooks.csv"):
    """
    Reads matches from CSV, fetches orderbook for each match, and saves results.
    Maintains compatibility with orderbook_fetcher.py output format (0-100 scale).
    """
    if not os.path.exists(matches_csv):
        print(f"File {matches_csv} not found.")
        return

    df = pd.read_csv(matches_csv)
    print(f"Processing {len(df)} matches from {matches_csv}...")
    
    results = []
    
    for i, row in df.iterrows():
        k_ticker = row["kalshi_market_ticker"]
        p_ticker = row["polymarket_market_ticker"]
        
        print(f"[{i+1}/{len(df)}] Fetching {k_ticker} and {p_ticker}...")
        
        # Use existing logic to fetch detailed orderbooks
        obs = get_matched_orderbooks(k_ticker, p_ticker, levels=1)
        
        res_row = row.to_dict()
        
        # Extract best levels and scale to 0-100 to match old fetcher behavior
        try:
            # Kalshi
            k_yes_bids = obs["kalshi"]["yes"]["bids"]
            k_yes_asks = obs["kalshi"]["yes"]["asks"]
            k_no_bids = obs["kalshi"]["no"]["bids"]
            k_no_asks = obs["kalshi"]["no"]["asks"]
            
            res_row["k_yes_bid"] = k_yes_bids[0]["price"] * 100 if k_yes_bids else None
            res_row["k_yes_ask"] = k_yes_asks[0]["price"] * 100 if k_yes_asks else None
            res_row["k_no_bid"] = k_no_bids[0]["price"] * 100 if k_no_bids else None
            res_row["k_no_ask"] = k_no_asks[0]["price"] * 100 if k_no_asks else None
            
            # Polymarket
            p_yes_bids = obs["polymarket"]["yes"]["bids"]
            p_yes_asks = obs["polymarket"]["yes"]["asks"]
            p_no_bids = obs["polymarket"]["no"]["bids"]
            p_no_asks = obs["polymarket"]["no"]["asks"]
            
            res_row["p_yes_bid"] = p_yes_bids[0]["price"] * 100 if p_yes_bids else None
            res_row["p_yes_ask"] = p_yes_asks[0]["price"] * 100 if p_yes_asks else None
            res_row["p_no_bid"] = p_no_bids[0]["price"] * 100 if p_no_bids else None
            res_row["p_no_ask"] = p_no_asks[0]["price"] * 100 if p_no_asks else None
            
        except Exception as e:
            print(f"Error processing row {i}: {e}")
            
        results.append(res_row)
        # Small delay to avoid aggressive rate limiting even though it's threaded
        time.sleep(0.1)
        
    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

def test():
    """Simple test utilizing the matches CSV."""
    try:
        df = pd.read_csv("Data/candidate_series_matches.csv")
        if df.empty:
            print("Matches CSV empty. Cannot test.")
            return
            
        first = df.iloc[0]
        k_tick = first["kalshi_market_ticker"]
        p_tick = first["polymarket_market_ticker"]
        k_title = first.get("kalshi_market", "Unknown Kalshi Title")
        p_title = first.get("polymarket_market", "Unknown Polymarket Title")
        
        print(f"Testing Arbitrage Fetch for:")
        print(f"Kalshi: {k_tick} | Title: {k_title}")
        print(f"Polymarket: {p_tick} | Title: {p_title}")
        print("-" * 50)
        
        obs = get_matched_orderbooks(k_tick, p_tick)
        
        print("\nKALSHI ORDERBOOK (Top 20 Levels):")
        print(f"YES - Bids: {obs['kalshi']['yes']['bids']}")
        print(f"YES - Asks: {obs['kalshi']['yes']['asks']}")
        print(f"NO  - Bids: {obs['kalshi']['no']['bids']}")
        print(f"NO  - Asks: {obs['kalshi']['no']['asks']}")
        
        print("\nPOLYMARKET ORDERBOOK (Top 20 Levels):")
        print(f"YES - Bids: {obs['polymarket']['yes']['bids']}")
        print(f"YES - Asks: {obs['polymarket']['yes']['asks']}")
        print(f"NO  - Bids: {obs['polymarket']['no']['bids']}")
        print(f"NO  - Asks: {obs['polymarket']['no']['asks']}")

    except FileNotFoundError:
        print("Matches CSV not found. Ensure you have run matching phase first.")

if __name__ == "__main__":
    test()
