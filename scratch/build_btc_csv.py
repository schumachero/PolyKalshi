"""
Build tracked_pairs_btc_apr.csv by matching:
- Kalshi KXBTCMINMON (BTC dips below $X) ↔ Polymarket "Will Bitcoin dip to $X in April"
- Kalshi KXBTCMAXMON (BTC rises above $X) ↔ Polymarket "Will Bitcoin reach $X in April"

Both platforms ask the same directional question (dip/reach), so YES on Kalshi = YES on Polymarket.
"""
import requests
import csv
import re

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
POLY_API = "https://gamma-api.polymarket.com"

# ── Fetch Kalshi active April 2026 markets ──────────────────────────
def get_kalshi_april_markets(series_ticker):
    url = f"{KALSHI_API}/markets?series_ticker={series_ticker}&limit=50"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    markets = []
    for m in r.json()["markets"]:
        if m["status"] == "active" and "APR30" in m.get("event_ticker", ""):
            strike = m.get("cap_strike") or m.get("floor_strike") or 0
            markets.append({
                "ticker": m["ticker"],
                "title": m["title"],
                "strike": strike,
                "close_time": m["close_time"],
                "type": "min" if "MIN" in series_ticker else "max",
            })
    return markets

# ── Fetch Polymarket April BTC event ────────────────────────────────
def get_polymarket_btc_april():
    url = f"{POLY_API}/events/slug/what-price-will-bitcoin-hit-in-april-2026"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    markets = []
    for m in data.get("markets", []):
        if m.get("closed"):
            continue
        q = m["question"]
        slug = m["slug"]
        market_id = m["id"]
        end_date = m.get("endDate", "")
        
        # Parse the strike price from groupItemTitle like "↓ 65,000" or "↑ 85,000"
        group_title = m.get("groupItemTitle", "")
        direction = None
        strike = None
        
        if group_title.startswith("↓"):
            direction = "dip"
            num_str = group_title.replace("↓", "").strip().replace(",", "")
            strike = int(num_str)
        elif group_title.startswith("↑"):
            direction = "reach"
            num_str = group_title.replace("↑", "").strip().replace(",", "")
            strike = int(num_str)
        
        if strike and direction:
            markets.append({
                "slug": slug,
                "market_id": market_id,
                "question": q,
                "strike": strike,
                "direction": direction,
                "end_date": end_date,
            })
    return markets

# ── Match ───────────────────────────────────────────────────────────
kalshi_min = get_kalshi_april_markets("KXBTCMINMON")
kalshi_max = get_kalshi_april_markets("KXBTCMAXMON")
poly_markets = get_polymarket_btc_april()

print(f"Kalshi MIN markets (active April): {len(kalshi_min)}")
print(f"Kalshi MAX markets (active April): {len(kalshi_max)}")
print(f"Polymarket BTC April markets (open): {len(poly_markets)}")

# Build lookup: (direction, strike) -> polymarket
poly_lookup = {}
for pm in poly_markets:
    key = (pm["direction"], pm["strike"])
    poly_lookup[key] = pm

rows = []

# Match KXBTCMINMON (dips below) with Polymarket dip markets
for km in kalshi_min:
    strike = km["strike"]
    pm = poly_lookup.get(("dip", strike))
    if pm:
        pair_id = f"{km['ticker']}__{pm['slug']}"
        rows.append({
            "pair_id": pair_id,
            "active": "True",
            "kalshi_ticker": km["ticker"],
            "kalshi_title": km["title"],
            "kalshi_side_held": "",
            "kalshi_quantity": "",
            "polymarket_ticker": pm["slug"],
            "polymarket_title": pm["question"],
            "polymarket_side_held": "",
            "polymarket_quantity": "",
            "close_time": km["close_time"],
            "match_score": 1.0,
            "max_position_per_pair_usd": 100.0,
            "min_profit_pct": 1.0,
            "min_liquidity_usd": 50.0,
            "cooldown_minutes": 30,
            "notes": f"BTC dip below ${strike:,} April 2026",
        })
        print(f"  MATCHED: {km['ticker']} <-> {pm['slug']} (dip ${strike:,})")
    else:
        print(f"  NO MATCH: {km['ticker']} (dip ${strike:,})")

# Match KXBTCMAXMON (rises above) with Polymarket reach markets
for km in kalshi_max:
    strike = km["strike"]
    pm = poly_lookup.get(("reach", strike))
    if pm:
        pair_id = f"{km['ticker']}__{pm['slug']}"
        rows.append({
            "pair_id": pair_id,
            "active": "True",
            "kalshi_ticker": km["ticker"],
            "kalshi_title": km["title"],
            "kalshi_side_held": "",
            "kalshi_quantity": "",
            "polymarket_ticker": pm["slug"],
            "polymarket_title": pm["question"],
            "polymarket_side_held": "",
            "polymarket_quantity": "",
            "close_time": km["close_time"],
            "match_score": 1.0,
            "max_position_per_pair_usd": 100.0,
            "min_profit_pct": 1.0,
            "min_liquidity_usd": 50.0,
            "cooldown_minutes": 30,
            "notes": f"BTC reach ${strike:,} April 2026",
        })
        print(f"  MATCHED: {km['ticker']} <-> {pm['slug']} (reach ${strike:,})")
    else:
        print(f"  NO MATCH: {km['ticker']} (reach ${strike:,})")

# ── Write CSV ───────────────────────────────────────────────────────
output_path = "Data/tracked_pairs_btc_apr.csv"
fieldnames = [
    "pair_id", "active", "kalshi_ticker", "kalshi_title",
    "kalshi_side_held", "kalshi_quantity",
    "polymarket_ticker", "polymarket_title",
    "polymarket_side_held", "polymarket_quantity",
    "close_time", "match_score",
    "max_position_per_pair_usd", "min_profit_pct",
    "min_liquidity_usd", "cooldown_minutes", "notes",
]

with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"\nWrote {len(rows)} matched pairs to {output_path}")
