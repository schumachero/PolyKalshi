import time
import requests
import pandas as pd

BASE = "https://api.elections.kalshi.com/trade-api/v2"
SESSION = requests.Session()
KALSHI_DATA_OUT = "Data/kalshi_markets.csv"
REQUEST_DELAY = 0.25   # 4 requests/sec max
MAX_RETRIES = 5

def get_series(category):
    url = f"{BASE}/series"
    r = SESSION.get(url, params={"category": category, "limit": 100}, timeout=30)
    r.raise_for_status()
    return r.json()["series"]


def safe_get(url, params=None):
    for attempt in range(MAX_RETRIES):
        r = SESSION.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = 2 ** attempt
            print(f"429 hit. Sleeping {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return r.json()
    return None


def get_series_list(category):
    data = safe_get(f"{BASE}/series", params={"category": category, "limit": 100})
    return data["series"] if data else []

def get_markets_for_series(series_ticker):
    data = safe_get(f"{BASE}/markets", params={"series_ticker": series_ticker, "limit": 100})
    return data["markets"] if data else []

def get_market_detail(market_ticker):
    return safe_get(f"{BASE}/markets/{market_ticker}")

def get_series_detail(series_ticker):
    return safe_get(f"{BASE}/series/{series_ticker}")

def main():
    series_list = get_series("Politics")
    print(f"Series fetched: {len(series_list)}")

    rows = []

    for i, s in enumerate(series_list[:100], start=1):
        print(f"[{i}/{len(series_list)}] {s['ticker']}")
        markets = get_markets_for_series(s["ticker"])

        # series-level enrichment
        series_detail = get_series_detail(s["ticker"]) or {}

        for m in markets:
            market_detail = get_market_detail(m["ticker"]) or {}

            rows.append({
                "platform": "kalshi",
                "series_ticker": s.get("ticker"),
                "series_title": s.get("title"),
                "market_ticker": m.get("ticker"),
                "market_title": m.get("title"),
                "status": m.get("status"),
                "close_time": m.get("close_time"),

                # extract clean rules
                "rules_text": (market_detail.get("market", {}).get("rules_primary") or "") + "\n" + (market_detail.get("market", {}).get("rules_secondary") or ""),
                "series_metadata": str(series_detail),
            })


    df = pd.DataFrame(rows)
    print(df.info())
    df.to_csv(KALSHI_DATA_OUT, index=False)
    print(f"Wrote {len(df)} rows to {KALSHI_DATA_OUT}")

if __name__ == "__main__":
    main()