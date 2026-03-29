import pandas as pd
import os
import sys
import json
from datetime import datetime, timezone

# Ensure we can import from src/notifications
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from notifications.telegram_bot import notify_arbitrage
except ImportError:
    def notify_arbitrage(*args, **kwargs):
        pass

# IO
INPUT_CSV = "Data/matched_orderbooks.csv"
OUTPUT_CSV = "Data/arbitrage_opportunities.csv"

# Filters
MIN_SCORE = 0.3
MIN_PROFIT = 0.1              # Minimum absolute profit percent
MIN_DAILY_ROI = 0.02          # Minimum ROI per day
NOTIFICATION_THRESHOLD = 5.0  # Notify if profit >= 5%
MAX_RESOLUTION_DAYS = 365


def parse_orderbook_side(raw_value):
    """
    Parse a stored orderbook side from CSV.

    Accepted formats:
      - JSON string: [{"price": 45, "size": 10}, ...]
      - JSON string: [[45, 10], [47, 20], ...]
      - Python list
      - NaN / empty -> []
    """
    if pd.isna(raw_value):
        return []

    value = raw_value
    if isinstance(raw_value, str):
        raw_value = raw_value.strip()
        if not raw_value:
            return []
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            print(f"Warning: could not parse orderbook JSON: {raw_value[:120]}")
            return []

    if not isinstance(value, list):
        return []

    parsed = []
    for level in value:
        try:
            if isinstance(level, dict):
                price = float(level["price"])
                size = float(level["size"])
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price = float(level[0])
                size = float(level[1])
            else:
                continue

            if size > 0:
                parsed.append((price, size))
        except (KeyError, TypeError, ValueError):
            continue

    parsed.sort(key=lambda x: x[0])  # cheapest ask first
    return parsed


def get_days_to_resolution(row):
    kalshi_close = row.get("kalshi_close_time")
    poly_close = row.get("polymarket_close_time")

    close_time = kalshi_close if pd.notna(kalshi_close) else poly_close
    if pd.isna(close_time):
        return 1

    delta = close_time - datetime.now(timezone.utc)
    return max(delta.days, 1)


def get_polymarket_fee_category(market_title, series_title):
    text = f"{market_title} {series_title}".lower()
    
    if any(k in text for k in ["btc", "eth", "sol", "bitcoin", "ethereum", "crypto", "layerzero"]):
        return "Crypto"
    elif any(k in text for k in ["nfl", "nba", "champions league", "premier league", "tennis", "sports", "ufc", "boxing", "mlb", "nhl"]):
        return "Sports"
    elif any(k in text for k in ["fed", "rate", "inflation", "cpi", "gdp", "economy", "job", "unemployment"]):
        return "Economics"
    elif any(k in text for k in ["trump", "biden", "election", "democrat", "republican", "senate", "house", "presidential", "primary", "nominee", "mayor", "congress"]):
        return "Politics"
    elif any(k in text for k in ["box office", "movie", "oscar", "grammy", "culture", "pop", "award", "spotify", "music", "youtube"]):
        return "Culture"
    elif any(k in text for k in ["weather", "temperature", "hurricane", "storm", "rain"]):
        return "Weather"
    elif any(k in text for k in ["ai", "gpt", "openai", "tech", "spacex", "starship", "apple", "google", "meta"]):
        return "Tech"
    elif any(k in text for k in ["israel", "gaza", "russia", "ukraine", "war", "peace", "nato", "geopolitics"]):
        return "Geopolitics"
    elif any(k in text for k in ["finance", "stock", "sp 500", "s&p", "dow", "nasdaq", "price target"]):
        return "Finance"
    elif any(k in text for k in ["say", "tweet", "mention"]):
        return "Mentions"
        
    return "Other / General"

def calculate_polymarket_fee(price_dollar, category):
    fees = {
        "Crypto": (0.072, 1),
        "Sports": (0.03, 1),
        "Finance": (0.04, 1),
        "Politics": (0.04, 1),
        "Economics": (0.03, 0.5),
        "Culture": (0.05, 1),
        "Weather": (0.025, 0.5),
        "Other / General": (0.2, 2),
        "Mentions": (0.25, 2),
        "Tech": (0.04, 1),
        "Geopolitics": (0.0, 1) # Geopolitics is technically 0
    }
    rate, exponent = fees.get(category, (0.04, 1))
    
    if category == "Geopolitics" or price_dollar <= 0 or price_dollar >= 1.0:
        return 0.0
        
    raw_fee = rate * ((price_dollar * (1.0 - price_dollar)) ** exponent)
    return raw_fee



def find_depth_arbitrage(k_asks, p_asks, days_to_res, min_profit, min_daily_roi, pm_category="Other / General"):
    """
    Consume both ask ladders while the next marginal chunk still satisfies:
        marginal_profit_pct >= max(min_profit, min_daily_roi * days_to_res)

    Where:
        marginal_profit_pct = 100 - (k_price + p_price)

    Returns a dict or None if no qualifying execution exists.
    """
    if not k_asks or not p_asks:
        return None

    days_to_res = max(days_to_res, 1)
    required_profit_pct = max(min_profit, min_daily_roi * days_to_res)

    i = 0
    j = 0

    k_remaining = k_asks[0][1]
    p_remaining = p_asks[0][1]

    total_contracts = 0.0
    total_k_cost = 0.0
    total_p_cost = 0.0
    levels_consumed = 0

    while i < len(k_asks) and j < len(p_asks):
        k_price, _ = k_asks[i]
        p_price, _ = p_asks[j]

        # Apply Kalshi fee to the ask price (We pay more when buying)
        k_price_dollar = k_price / 100.0
        k_fee_dollar = 0.07 * k_price_dollar * (1.0 - k_price_dollar) if k_price_dollar < 1.0 else 0
        k_price_net = k_price + (k_fee_dollar * 100.0)

        # Apply Polymarket category fee to the ask price
        p_price_dollar = p_price / 100.0
        p_fee_dollar = calculate_polymarket_fee(p_price_dollar, pm_category)
        p_price_net = p_price + (p_fee_dollar * 100.0)

        marginal_total_cost = k_price_net + p_price_net
        marginal_profit_pct = 100.0 - marginal_total_cost

        if marginal_profit_pct < required_profit_pct:
            break

        qty = min(k_remaining, p_remaining)
        if qty <= 0:
            break

        total_contracts += qty
        total_k_cost += qty * k_price_net
        total_p_cost += qty * p_price_net
        levels_consumed += 1

        k_remaining -= qty
        p_remaining -= qty

        if k_remaining <= 1e-12:
            i += 1
            if i < len(k_asks):
                k_remaining = k_asks[i][1]

        if p_remaining <= 1e-12:
            j += 1
            if j < len(p_asks):
                p_remaining = p_asks[j][1]

    if total_contracts <= 0:
        return None

    avg_k_price = total_k_cost / total_contracts
    avg_p_price = total_p_cost / total_contracts
    blended_total_cost = avg_k_price + avg_p_price
    profit_pct = 100.0 - blended_total_cost
    daily_roi = profit_pct / days_to_res
    liquidity_usd = total_contracts * (blended_total_cost / 100.0)

    return {
        "contracts": round(total_contracts, 4),
        "avg_k_price": round(avg_k_price, 4),
        "avg_p_price": round(avg_p_price, 4),
        "blended_total_cost": round(blended_total_cost, 4),
        "expected_profit": round(profit_pct, 4),
        "daily_roi": round(daily_roi, 6),
        "liquidity_usd": round(liquidity_usd, 2),
        "required_profit_pct": round(required_profit_pct, 4),
        "levels_consumed": levels_consumed,
    }


def calculate_arbitrage(input_data=None, output_csv=OUTPUT_CSV, return_df=False):
    if input_data is None:
        input_data = INPUT_CSV

    if isinstance(input_data, str):
        if not os.path.exists(input_data):
            print(f"{input_data} not found.")
            return pd.DataFrame() if return_df else None
        df = pd.read_csv(input_data)
        print(f"Loaded {len(df)} matches from {input_data}")
    elif isinstance(input_data, pd.DataFrame):
        df = input_data.copy()
        print(f"Processing {len(df)} matches from provided DataFrame.")
    else:
        raise ValueError("input_data must be a file path or a pandas DataFrame")

    # Optional datetime parsing for time-aware thresholding
    if "kalshi_close_time" in df.columns:
        df["kalshi_close_time"] = pd.to_datetime(df["kalshi_close_time"], errors="coerce", utc=True)
    if "polymarket_close_time" in df.columns:
        df["polymarket_close_time"] = pd.to_datetime(df["polymarket_close_time"], errors="coerce", utc=True)

    # Optional score filter if the column exists
    if "combined_score" in df.columns:
        before = len(df)
        df = df[df["combined_score"].isna() | (df["combined_score"] >= MIN_SCORE)]
        print(f"Filtered to {len(df)} matches with score >= {MIN_SCORE} (or missing score), from {before}")
    else:
        print("No combined_score column found; skipping score filter.")

    required_depth_cols = ["k_yes_asks", "k_no_asks", "p_yes_asks", "p_no_asks"]
    missing = [c for c in required_depth_cols if c not in df.columns]
    if missing:
        raise ValueError(
            "Depth-aware arbitrage calculation requires full ask ladders in matched_orderbooks.csv. "
            f"Missing columns: {missing}"
        )

    results = []

    for _, row in df.iterrows():
        days_to_res = get_days_to_resolution(row)

        if days_to_res > MAX_RESOLUTION_DAYS:
            continue

        pm_category = get_polymarket_fee_category(
            str(row.get("polymarket_market", "")),
            str(row.get("polymarket_series", ""))
        )

        strategies = [
            {
                "direction": "K_YES_P_NO",
                "k_asks": parse_orderbook_side(row.get("k_yes_asks")),
                "p_asks": parse_orderbook_side(row.get("p_no_asks")),
            },
            {
                "direction": "P_YES_K_NO",
                "k_asks": parse_orderbook_side(row.get("k_no_asks")),
                "p_asks": parse_orderbook_side(row.get("p_yes_asks")),
            },
        ]

        for s in strategies:
            depth_result = find_depth_arbitrage(
                k_asks=s["k_asks"],
                p_asks=s["p_asks"],
                days_to_res=days_to_res,
                min_profit=MIN_PROFIT,
                min_daily_roi=MIN_DAILY_ROI,
                pm_category=pm_category,
            )

            if depth_result is None:
                continue

            res = row.to_dict()
            res["direction"] = s["direction"]
            res["total_cost"] = depth_result["blended_total_cost"]
            res["expected_profit"] = depth_result["expected_profit"]
            res["daily_roi"] = depth_result["daily_roi"]
            res["liquidity_usd"] = depth_result["liquidity_usd"]
            res["contracts"] = depth_result["contracts"]
            res["avg_k_price"] = depth_result["avg_k_price"]
            res["avg_p_price"] = depth_result["avg_p_price"]
            res["required_profit_pct"] = depth_result["required_profit_pct"]
            res["days_to_resolution"] = days_to_res
            res["levels_consumed"] = depth_result["levels_consumed"]

            results.append(res)

            if depth_result["expected_profit"] >= NOTIFICATION_THRESHOLD:
                market_name = row.get("kalshi_market", row.get("kalshi_market_ticker", "unknown market"))
                print(
                    f"!!! HIGH PROFIT !!! Sending notification for {market_name} "
                    f"({depth_result['expected_profit']:.2f}%)"
                )
                notify_arbitrage(res)

    if not results:
        print("No arbitrage opportunities found.")
        base_cols = df.columns.tolist() if len(df.columns) else []
        extra_cols = [
            "direction", "total_cost", "expected_profit", "daily_roi", "liquidity_usd",
            "contracts", "avg_k_price", "avg_p_price", "required_profit_pct",
            "days_to_resolution", "levels_consumed"
        ]
        empty_df = pd.DataFrame(columns=base_cols + extra_cols)
        if output_csv:
            empty_df.to_csv(output_csv, index=False)
        return empty_df if return_df else None

    out_df = pd.DataFrame(results)
    out_df = out_df.sort_values(
        by=["expected_profit", "liquidity_usd", "daily_roi"],
        ascending=[False, False, False]
    )

    if output_csv:
        out_df.to_csv(output_csv, index=False)
        print(f"Found {len(out_df)} opportunities. Results saved to {output_csv}")
    else:
        print(f"Found {len(out_df)} opportunities.")

    print("\nTop 5 Arbitrage Opportunities:")
    cols_to_show = [
        "kalshi_market",
        "polymarket_market",
        "direction",
        "expected_profit",
        "daily_roi",
        "liquidity_usd",
        "contracts",
        "levels_consumed",
    ]
    available_cols = [c for c in cols_to_show if c in out_df.columns]
    print(out_df[available_cols].head(5).to_string(index=False))

    if return_df:
        return out_df


if __name__ == "__main__":
    calculate_arbitrage()