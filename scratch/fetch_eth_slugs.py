import requests
import json

# Polymarket Event
poly_res = requests.get('https://gamma-api.polymarket.com/events?slug=what-price-will-ethereum-hit-in-april-2026')
if poly_res.ok and len(poly_res.json()) > 0:
    print("--- Polymarket Markets ---")
    for m in poly_res.json()[0]['markets']:
        print(f"{m['question']} | {m['slug']}")

# Kalshi MAX Markets
print("\n--- Kalshi MAX Markets ---")
kalshi_max_res = requests.get('https://trading-api.kalshi.com/trade-api/v2/markets?series_ticker=KXETHMAXMON-ETH-26APR30')
if kalshi_max_res.ok:
    for m in kalshi_max_res.json().get('markets', []):
        print(f"{m['ticker']} | {m['title']}")

# Kalshi MIN Markets
print("\n--- Kalshi MIN Markets ---")
kalshi_min_res = requests.get('https://trading-api.kalshi.com/trade-api/v2/markets?series_ticker=KXETHMINMON-ETH-26APR30')
if kalshi_min_res.ok:
    for m in kalshi_min_res.json().get('markets', []):
        print(f"{m['ticker']} | {m['title']}")
