import requests

kalshi_ticker = "KXNYCMINWAGE-27JAN01"
url_k = f"https://api.elections.kalshi.com/trade-api/v2/markets/{kalshi_ticker}/orderbook"
print(f"--- Kalshi Orderbook: {url_k} ---")
r_k = requests.get(url_k).json()
print("Kalshi keys:", list(r_k.keys()))
if "orderbook_fp" in r_k:
    print("Kalshi orderbook_fp keys:", list(r_k["orderbook_fp"].keys()))

yes_token = "71061461875209900766812068768816681292193805160628503277918359666928040556326"
url_p = f"https://clob.polymarket.com/book?token_id={yes_token}"
print(f"\n--- Polymarket CLOB: {url_p} ---")
r_p = requests.get(url_p).json()
print("Polymarket CLOB keys:", list(r_p.keys()))
if "bids" in r_p and len(r_p["bids"]) > 0:
    print("First Polymarket bid:", r_p["bids"][0])
if "asks" in r_p and len(r_p["asks"]) > 0:
    print("First Polymarket ask:", r_p["asks"][0])
