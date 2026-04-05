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
# Helpers
# =========================

def scale_book_to_cents(levels):
    """
    Convert a list of orderbook levels from 0-1 dollars/probability scale
    into 0-100 cent scale, preserving volume.

    Input:
        [{"price": 0.45, "volume": 10}, {"price": 0.47, "volume": 20}]

    Output:
        [{"price": 45.0, "size": 10.0}, {"price": 47.0, "size": 20.0}]
    """
    out = []
    for lvl in levels or []:
        try:
            out.append({
                "price": round(float(lvl["price"]) * 100, 4),
                "size": round(float(lvl["volume"]), 4),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return out


def best_level(levels):
    """Return (price_in_cents, volume) from the first level, or (None, None)."""
    if not levels:
        return None, None
    return levels[0]["price"] * 100, levels[0]["volume"]


# =========================
# Orderbook Functions
# =========================

def get_kalshi_orderbook(market_ticker, levels=DEFAULT_LEVELS):
    """
    Fetches the top orderbook levels (bids and asks) for YES and NO from Kalshi.
    Kalshi provides yes bids and no bids directly.
    Yes asks are derived from no bids, and no asks are derived from yes bids.
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

    uses_dollar_scale = "yes_dollars" in book_info or "no_dollars" in book_info

    def parse_side(side_data):
        parsed = []
        for row in side_data:
            if len(row) >= 2:
                price = float(row[0])
                qty = float(row[1])

                # Normalize to 0-1 scale internally
                if not uses_dollar_scale and price > 1:
                    price = price / 100.0

                parsed.append({
                    "price": round(price, 4),
                    "volume": round(qty, 4)
                })

        # bids highest first
        parsed.sort(key=lambda x: x["price"], reverse=True)
        return parsed[:levels]

    yes_bids = parse_side(yes_raw)
    no_bids = parse_side(no_raw)

    # Derive asks from opposite-side bids
    yes_asks = [{"price": round(1.0 - p["price"], 4), "volume": p["volume"]} for p in no_bids]
    no_asks = [{"price": round(1.0 - p["price"], 4), "volume": p["volume"]} for p in yes_bids]

    # asks lowest first
    yes_asks.sort(key=lambda x: x["price"])
    no_asks.sort(key=lambda x: x["price"])

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

def fetch_polymarket_market_exact(market_slug_or_id: str):
    if market_slug_or_id.isdigit():
        url = f"{POLYMARKET_GAMMA}/markets/{market_slug_or_id}"
    else:
        url = f"{POLYMARKET_GAMMA}/markets/slug/{market_slug_or_id}"

    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_polymarket_orderbook(market_ticker, levels=DEFAULT_LEVELS):
    if pd.isna(market_ticker):
        print("Error: Polymarket market ticker is missing.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    if isinstance(market_ticker, float) and market_ticker.is_integer():
        market_ticker = str(int(market_ticker))
    else:
        market_ticker = str(market_ticker).strip()

    try:
        market_data = fetch_polymarket_market_exact(market_ticker)
    except Exception as e:
        print(f"Error fetching exact Polymarket market {market_ticker}: {e}")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    # Helpful debug
    #print(
    #    "POLY DEBUG | "
    #    f"requested={market_ticker} | "
    #    f"resolved_slug={market_data.get('slug')} | "
    #    f"question={market_data.get('question')} | "
    #    f"enableOrderBook={market_data.get('enableOrderBook')} | "
    #    f"pendingDeployment={market_data.get('pendingDeployment')} | "
    #    f"active={market_data.get('active')} | "
    #    f"closed={market_data.get('closed')}"
    #)

    if market_data.get("closed", False):
        print(f"Polymarket market {market_ticker} is closed; skipping orderbook fetch.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    if not market_data.get("enableOrderBook", False):
        print(f"Polymarket market {market_ticker} has enableOrderBook=False; skipping.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    if market_data.get("pendingDeployment", False):
        print(f"Polymarket market {market_ticker} is pending deployment; skipping.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    tokens = market_data.get("clobTokenIds", [])
    if isinstance(tokens, str):
        tokens = json.loads(tokens)

    if not tokens or len(tokens) < 2:
        print(f"Polymarket market {market_ticker} does not have usable clobTokenIds.")
        return {"yes": {"bids": [], "asks": []}, "no": {"bids": [], "asks": []}}

    yes_token, no_token = tokens[0], tokens[1]

    def fetch_clob_book(token_id):
        url = f"{POLYMARKET_CLOB}/book?token_id={token_id}"
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"Error fetching Polymarket CLOB for token {token_id}: {e}")
            return {"bids": [], "asks": []}

        def parse_array(arr, is_bid):
            parsed = []
            for item in arr:
                try:
                    parsed.append({
                        "price": round(float(item.get("price", 0)), 4),
                        "volume": round(float(item.get("size", 0)), 4),
                    })
                except (TypeError, ValueError):
                    continue
            parsed.sort(key=lambda x: x["price"], reverse=is_bid)
            return parsed[:levels]

        return {
            "bids": parse_array(data.get("bids", []), True),
            "asks": parse_array(data.get("asks", []), False),
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS) as executor:
        yes_future = executor.submit(fetch_clob_book, yes_token)
        no_future = executor.submit(fetch_clob_book, no_token)
        return {
            "yes": yes_future.result(),
            "no": no_future.result(),
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


def run_batch_fetch(
    matches_csv="Data/predicted_equivalent_markets.csv",
    output_csv="Data/matched_orderbooks.csv",
    levels=DEFAULT_LEVELS,
):
    """
    Reads matches from CSV, fetches orderbooks for each match, and saves results.

    Output contains:
      - existing top-of-book columns (0-100 scale)
      - full depth columns as JSON strings in 0-100 scale:
            k_yes_asks, k_no_asks, p_yes_asks, p_no_asks
            k_yes_bids, k_no_bids, p_yes_bids, p_no_bids
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

        # IMPORTANT: fetch full depth, not just level 1
        obs = get_matched_orderbooks(k_ticker, p_ticker, levels=levels)

        res_row = row.to_dict()

        try:
            # Kalshi
            k_yes_bids = obs["kalshi"]["yes"]["bids"]
            k_yes_asks = obs["kalshi"]["yes"]["asks"]
            k_no_bids = obs["kalshi"]["no"]["bids"]
            k_no_asks = obs["kalshi"]["no"]["asks"]

            # Polymarket
            p_yes_bids = obs["polymarket"]["yes"]["bids"]
            p_yes_asks = obs["polymarket"]["yes"]["asks"]
            p_no_bids = obs["polymarket"]["no"]["bids"]
            p_no_asks = obs["polymarket"]["no"]["asks"]

            # -------------------------
            # Existing top-of-book cols
            # -------------------------
            res_row["k_yes_bid"] = k_yes_bids[0]["price"] * 100 if k_yes_bids else None
            res_row["k_yes_bid_vol"] = k_yes_bids[0]["volume"] if k_yes_bids else None
            res_row["k_yes_ask"] = k_yes_asks[0]["price"] * 100 if k_yes_asks else None
            res_row["k_yes_ask_vol"] = k_yes_asks[0]["volume"] if k_yes_asks else None
            res_row["k_no_bid"] = k_no_bids[0]["price"] * 100 if k_no_bids else None
            res_row["k_no_bid_vol"] = k_no_bids[0]["volume"] if k_no_bids else None
            res_row["k_no_ask"] = k_no_asks[0]["price"] * 100 if k_no_asks else None
            res_row["k_no_ask_vol"] = k_no_asks[0]["volume"] if k_no_asks else None

            res_row["p_yes_bid"] = p_yes_bids[0]["price"] * 100 if p_yes_bids else None
            res_row["p_yes_bid_vol"] = p_yes_bids[0]["volume"] if p_yes_bids else None
            res_row["p_yes_ask"] = p_yes_asks[0]["price"] * 100 if p_yes_asks else None
            res_row["p_yes_ask_vol"] = p_yes_asks[0]["volume"] if p_yes_asks else None
            res_row["p_no_bid"] = p_no_bids[0]["price"] * 100 if p_no_bids else None
            res_row["p_no_bid_vol"] = p_no_bids[0]["volume"] if p_no_bids else None
            res_row["p_no_ask"] = p_no_asks[0]["price"] * 100 if p_no_asks else None
            res_row["p_no_ask_vol"] = p_no_asks[0]["volume"] if p_no_asks else None

            # -------------------------
            # Full depth cols for analyzer
            # Stored as JSON strings in 0-100 scale
            # -------------------------
            res_row["k_yes_bids"] = json.dumps(scale_book_to_cents(k_yes_bids))
            res_row["k_yes_asks"] = json.dumps(scale_book_to_cents(k_yes_asks))
            res_row["k_no_bids"] = json.dumps(scale_book_to_cents(k_no_bids))
            res_row["k_no_asks"] = json.dumps(scale_book_to_cents(k_no_asks))

            res_row["p_yes_bids"] = json.dumps(scale_book_to_cents(p_yes_bids))
            res_row["p_yes_asks"] = json.dumps(scale_book_to_cents(p_yes_asks))
            res_row["p_no_bids"] = json.dumps(scale_book_to_cents(p_no_bids))
            res_row["p_no_asks"] = json.dumps(scale_book_to_cents(p_no_asks))

        except Exception as e:
            print(f"Error processing row {i}: {e}")

        results.append(res_row)
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

        print("Testing Arbitrage Fetch for:")
        print(f"Kalshi: {k_tick} | Title: {k_title}")
        print(f"Polymarket: {p_tick} | Title: {p_title}")
        print("-" * 50)

        obs = get_matched_orderbooks(k_tick, p_tick, levels=DEFAULT_LEVELS)

        print("\nKALSHI ORDERBOOK:")
        print(f"YES - Bids: {obs['kalshi']['yes']['bids']}")
        print(f"YES - Asks: {obs['kalshi']['yes']['asks']}")
        print(f"NO  - Bids: {obs['kalshi']['no']['bids']}")
        print(f"NO  - Asks: {obs['kalshi']['no']['asks']}")

        print("\nPOLYMARKET ORDERBOOK:")
        print(f"YES - Bids: {obs['polymarket']['yes']['bids']}")
        print(f"YES - Asks: {obs['polymarket']['yes']['asks']}")
        print(f"NO  - Bids: {obs['polymarket']['no']['bids']}")
        print(f"NO  - Asks: {obs['polymarket']['no']['asks']}")

    except FileNotFoundError:
        print("Matches CSV not found. Ensure you have run matching phase first.")


if __name__ == "__main__":
    test()