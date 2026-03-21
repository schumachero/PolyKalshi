import time
import requests
import pandas as pd

BASE = "https://gamma-api.polymarket.com"
SESSION = requests.Session()
POLYMARKET_DATA_OUT = "Data/polymarket_markets.csv"

REQUEST_DELAY = 0.25
MAX_RETRIES = 5


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


def get_polymarket_markets(limit=100, offset=0, active=True, closed=False):
    params = {
        "limit": limit,
        "offset": offset,
        "active": "true",
        "closed": "false",
        "order": "id",
        "ascending": "true",
    }
    return safe_get(f"{BASE}/markets", params=params) or []


def main():
    rows = []
    offset = 0
    page_size = 100

    while True:
        
        markets = get_polymarket_markets(
            limit=page_size,
            offset=offset,
            active=True,
            closed=False
        )
        print(f"offset={offset}, got={len(markets)}, rows_so_far={len(rows)}")

        if not markets:
            break

        for m in markets:
            print(f"offset={offset}, got={len(markets)}, rows_so_far={len(rows)}")
            # A market can belong to one or more events; usually take the first parent event
            parent_event = (m.get("events") or [{}])[0]

            rows.append({
                "platform": "polymarket",

                # parent event / container
                "series_ticker": parent_event.get("id"),
                "series_title": parent_event.get("title"),
                "series_slug": parent_event.get("slug"),

                # actual tradable candidate submarket
                "market_ticker": m.get("id"),
                "market_title": m.get("question"),
                "market_slug": m.get("slug"),
                "group_item_title": m.get("groupItemTitle"),

                "status": "active" if m.get("active") and not m.get("closed") else "closed",
                "close_time": m.get("endDate") or m.get("endDateIso"),

                # market-level + event-level metadata
                "rules_text": (m.get("description") or parent_event.get("description") or "") + "\nSpecific: " + (m.get("question") or ""),
                "resolution_source": m.get("resolutionSource") or parent_event.get("resolutionSource"),
                "subtitle": m.get("subtitle") or parent_event.get("subtitle"),

                # useful for downstream dedupe / orderbook joins
                "condition_id": m.get("conditionId"),
                "question_id": m.get("questionID"),
                "market_group": m.get("marketGroup"),
            })

        if len(markets) < page_size:
            break

        offset += page_size

    df = pd.DataFrame(rows)
    print(df.info())
    print(df.head())
    df.to_csv(POLYMARKET_DATA_OUT, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {POLYMARKET_DATA_OUT}")


if __name__ == "__main__":
    main()