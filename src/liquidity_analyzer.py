import pandas as pd
import os
import json
from datetime import datetime, timezone

# --- CONFIGURATION ---
INPUT_CSV = "Data/matched_orderbooks.csv"
OUTPUT_ANALYSIS_CSV = "Data/arbitrage_liquidity_analysis.csv"
OUTPUT_VERIFIED_CSV = "Data/liquidity_verified_arbitrage.csv"

# Minimum paired capital required to consider the trade useful
MIN_LIQUIDITY_USD = 50.0

# Minimum ROI per day required
MIN_DAILY_ROI = 0.02

# Minimum absolute profit percent required
MIN_PROFIT = 0.1

# Maximum days until resolution
MAX_RESOLUTION_DAYS = 365


def parse_orderbook_side(raw_value):
    """
    Parse a stored orderbook side from CSV.

    Expected formats:
      - JSON string:
          [{"price": 45, "size": 10}, {"price": 47, "size": 20}]
      - JSON string:
          [[45, 10], [47, 20]]
      - Python list already loaded
      - Empty / NaN -> []

    Returns:
      list of (price, size) tuples sorted ascending by price
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


def find_depth_arbitrage(k_asks, p_asks, days_to_res, min_profit, min_daily_roi):
    """
    Walk two ask books level-by-level and accumulate executable size while
    the NEXT marginal chunk still satisfies the profitability threshold.

    Threshold:
        marginal_profit_pct >= max(min_profit, min_daily_roi * days_to_res)

    Where:
        marginal_profit_pct = 100 - (k_price + p_price)

    Returns:
        dict with blended execution stats, or None if no qualifying trade exists.
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

    levels_used = 0

    while i < len(k_asks) and j < len(p_asks):
        k_price, _ = k_asks[i]
        p_price, _ = p_asks[j]

        marginal_total_cost = k_price + p_price
        marginal_profit_pct = 100.0 - marginal_total_cost

        # Stop once the next chunk is no longer good enough
        if marginal_profit_pct < required_profit_pct:
            break

        qty = min(k_remaining, p_remaining)
        if qty <= 0:
            break

        total_contracts += qty
        total_k_cost += qty * k_price
        total_p_cost += qty * p_price
        levels_used += 1

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
    blended_profit_pct = 100.0 - blended_total_cost
    daily_roi = blended_profit_pct / days_to_res

    # Paired capital required to enter both legs
    paired_capital_usd = total_contracts * (blended_total_cost / 100.0)

    return {
        "contracts": round(total_contracts, 4),
        "avg_k_price": round(avg_k_price, 4),
        "avg_p_price": round(avg_p_price, 4),
        "blended_total_cost": round(blended_total_cost, 4),
        "profit_pct": round(blended_profit_pct, 4),
        "daily_roi": round(daily_roi, 6),
        "liquidity_usd": round(paired_capital_usd, 2),
        "required_profit_pct": round(required_profit_pct, 4),
        "levels_consumed": levels_used,
    }


def get_days_to_resolution(row):
    kalshi_close = row.get("kalshi_close_time")
    poly_close = row.get("polymarket_close_time")

    close_time = kalshi_close if pd.notna(kalshi_close) else poly_close
    if pd.isna(close_time):
        return 1

    delta = close_time - datetime.now(timezone.utc)
    return max(delta.days, 1)


def analyze_liquidity_and_efficiency():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found. Please run matching and orderbook fetcher first.")
        return

    df = pd.read_csv(INPUT_CSV)
    print(f"Analyzing {len(df)} market pairs...")

    # Ensure date columns are datetime
    df["kalshi_close_time"] = pd.to_datetime(df.get("kalshi_close_time"), errors="coerce", utc=True)
    df["polymarket_close_time"] = pd.to_datetime(df.get("polymarket_close_time"), errors="coerce", utc=True)

    # Required full-depth columns
    required_depth_cols = [
        "k_yes_asks",
        "k_no_asks",
        "p_yes_asks",
        "p_no_asks",
    ]
    missing = [c for c in required_depth_cols if c not in df.columns]
    if missing:
        raise ValueError(
            "Depth-aware liquidity analysis requires full ask ladders in matched_orderbooks.csv. "
            f"Missing columns: {missing}"
        )

    results = []

    for _, row in df.iterrows():
        days_to_res = get_days_to_resolution(row)

        strategies = [
            {
                "name": "K_YES_P_NO",
                "k_asks": parse_orderbook_side(row.get("k_yes_asks")),
                "p_asks": parse_orderbook_side(row.get("p_no_asks")),
            },
            {
                "name": "P_YES_K_NO",
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
            )

            if depth_result is None:
                results.append({
                    "kalshi_market": row.get("kalshi_market"),
                    "polymarket_market": row.get("polymarket_market"),
                    "strategy": s["name"],
                    "profit_pct": None,
                    "days_to_resolution": days_to_res,
                    "daily_roi": None,
                    "liquidity_usd": 0.0,
                    "contracts": 0.0,
                    "avg_k_price": None,
                    "avg_p_price": None,
                    "blended_total_cost": None,
                    "required_profit_pct": round(max(MIN_PROFIT, MIN_DAILY_ROI * max(days_to_res, 1)), 4),
                    "levels_consumed": 0,
                    "status": "FAIL",
                    "fail_reason": "No depth that satisfies required profit/ROI",
                })
                continue

            is_good_liquidity = depth_result["liquidity_usd"] >= MIN_LIQUIDITY_USD
            is_efficient = depth_result["daily_roi"] >= MIN_DAILY_ROI
            is_short_enough = days_to_res <= MAX_RESOLUTION_DAYS

            fail_reasons = []
            if not is_good_liquidity:
                fail_reasons.append(f"Low Liq (${depth_result['liquidity_usd']:.2f})")
            if not is_efficient:
                fail_reasons.append(f"Low Daily ROI ({depth_result['daily_roi']:.4f}%)")
            if not is_short_enough:
                fail_reasons.append(f"Too long ({days_to_res} days)")

            analysis = {
                "kalshi_market": row.get("kalshi_market"),
                "polymarket_market": row.get("polymarket_market"),
                "strategy": s["name"],
                "profit_pct": depth_result["profit_pct"],
                "days_to_resolution": days_to_res,
                "daily_roi": depth_result["daily_roi"],
                "liquidity_usd": depth_result["liquidity_usd"],
                "contracts": depth_result["contracts"],
                "avg_k_price": depth_result["avg_k_price"],
                "avg_p_price": depth_result["avg_p_price"],
                "blended_total_cost": depth_result["blended_total_cost"],
                "required_profit_pct": depth_result["required_profit_pct"],
                "levels_consumed": depth_result["levels_consumed"],
                "status": "PASS" if (is_good_liquidity and is_efficient and is_short_enough) else "FAIL",
                "fail_reason": ", ".join(fail_reasons),
            }

            results.append(analysis)

    if not results:
        print("No profitable opportunities found to analyze.")
        return

    analysis_df = pd.DataFrame(results)
    analysis_df = analysis_df.sort_values(
        by=["status", "daily_roi", "liquidity_usd"],
        ascending=[True, False, False]
    )

    analysis_df.to_csv(OUTPUT_ANALYSIS_CSV, index=False)
    print(f"Analysis complete. Results saved to {OUTPUT_ANALYSIS_CSV}")

    passed = analysis_df[analysis_df["status"] == "PASS"].copy()
    passed.to_csv(OUTPUT_VERIFIED_CSV, index=False)
    print(f"Saved {len(passed)} liquidity-verified opportunities to {OUTPUT_VERIFIED_CSV}")

    if not passed.empty:
        print("\nTop Liquidity-Verified Opportunities:")
        print(
            passed[
                [
                    "kalshi_market",
                    "strategy",
                    "profit_pct",
                    "daily_roi",
                    "liquidity_usd",
                    "contracts",
                    "levels_consumed",
                ]
            ].head(10).to_string(index=False)
        )
    else:
        print("\nNo opportunities passed the liquidity/efficiency filters.")
        print("Top failed opportunities for context:")
        print(
            analysis_df[
                [
                    "kalshi_market",
                    "strategy",
                    "profit_pct",
                    "daily_roi",
                    "liquidity_usd",
                    "contracts",
                    "levels_consumed",
                    "fail_reason",
                ]
            ].head(10).to_string(index=False)
        )


if __name__ == "__main__":
    analyze_liquidity_and_efficiency()