import pandas as pd
import numpy as np
import os
from datetime import datetime, timezone

# --- CONFIGURATION ---
INPUT_CSV = "Data/matched_orderbooks.csv"
OUTPUT_ANALYSIS_CSV = "Data/arbitrage_liquidity_analysis.csv"

# Minimum liquidity threshold (total dollars available at the best price)
MIN_LIQUIDITY_USD = 50.0 

# Minimum ROI per day (e.g., 0.1% per day minimum)
MIN_DAILY_ROI = 0.05 

# Maximum days until resolution (to avoid locking funds too long)
MAX_RESOLUTION_DAYS = 365 

def analyze_liquidity_and_efficiency():
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found. Please run matching and orderbook fetcher first.")
        return

    df = pd.read_csv(INPUT_CSV)
    print(f"Analyzing {len(df)} market pairs...")

    # Ensure date columns are datetime
    df["kalshi_close_time"] = pd.to_datetime(df["kalshi_close_time"], errors="coerce", utc=True)
    df["polymarket_close_time"] = pd.to_datetime(df["polymarket_close_time"], errors="coerce", utc=True)

    results = []

    for _, row in df.iterrows():
        # We check both directions
        # Direction A: Buy Kalshi YES, Buy Poly NO
        # Direction B: Buy Kalshi NO, Buy Poly YES
        
        strategies = [
            {
                "name": "K_YES_P_NO",
                "k_price": row.get("k_yes_ask"),
                "k_vol": row.get("k_yes_ask_vol"),
                "p_price": row.get("p_no_ask"),
                "p_vol": row.get("p_no_ask_vol"),
                "k_opp_vol": row.get("k_yes_bid_vol"), # For spread check
                "p_opp_vol": row.get("p_no_bid_vol")
            },
            {
                "name": "P_YES_K_NO",
                "k_price": row.get("k_no_ask"),
                "k_vol": row.get("k_no_ask_vol"),
                "p_price": row.get("p_yes_ask"),
                "p_vol": row.get("p_yes_ask_vol"),
                "k_opp_vol": row.get("k_no_bid_vol"),
                "p_opp_vol": row.get("p_yes_bid_vol")
            }
        ]

        for s in strategies:
            if pd.isna(s["k_price"]) or pd.isna(s["p_price"]):
                continue

            # 1. Basic Profitability
            cost = s["k_price"] + s["p_price"]
            expected_profit_pct = 100 - cost
            
            if expected_profit_pct <= 0:
                continue

            # 2. Time factor (Capital Efficiency)
            close_time = row["kalshi_close_time"] if pd.notna(row["kalshi_close_time"]) else row["polymarket_close_time"]
            days_to_res = 1 # fallback
            if pd.notna(close_time):
                delta = close_time - datetime.now(timezone.utc)
                days_to_res = max(delta.days, 1) # at least 1 day to avoid div by zero

            daily_roi = expected_profit_pct / days_to_res

            # 3. Liquidity Check
            # We look at the dollar volume available at the best price on both sides
            # Note: For binary options, volume is usually in contracts (each $1 max value)
            # Kalshi volume is in contracts. Poly volume is in contracts.
            k_liquidity_usd = (s["k_vol"] or 0) * (s["k_price"] / 100.0)
            p_liquidity_usd = (s["p_vol"] or 0) * (s["p_price"] / 100.0)
            min_executable_usd = min(k_liquidity_usd, p_liquidity_usd)

            # 4. Filter logic
            is_good_liquidity = min_executable_usd >= MIN_LIQUIDITY_USD
            is_efficient = daily_roi >= MIN_DAILY_ROI
            is_short_enough = days_to_res <= MAX_RESOLUTION_DAYS

            analysis = {
                "kalshi_market": row["kalshi_market"],
                "polymarket_market": row["polymarket_market"],
                "strategy": s["name"],
                "profit_pct": round(expected_profit_pct, 2),
                "days_to_resolution": days_to_res,
                "daily_roi": round(daily_roi, 4),
                "liquidity_usd": round(min_executable_usd, 2),
                "kalshi_liq": round(k_liquidity_usd, 2),
                "poly_liq": round(p_liquidity_usd, 2),
                "status": "PASS" if (is_good_liquidity and is_efficient and is_short_enough) else "FAIL",
                "fail_reason": []
            }

            if not is_good_liquidity: analysis["fail_reason"].append(f"Low Liq (${min_executable_usd:.1f})")
            if not is_efficient: analysis["fail_reason"].append(f"Low Daily ROI ({daily_roi:.3f}%)")
            if not is_short_enough: analysis["fail_reason"].append(f"Too long ({days_to_res} days)")
            
            analysis["fail_reason"] = ", ".join(analysis["fail_reason"])
            results.append(analysis)

    if not results:
        print("No profitable opportunities found to analyze.")
        return

    analysis_df = pd.DataFrame(results)
    analysis_df = analysis_df.sort_values(by="daily_roi", ascending=False)
    
    analysis_df.to_csv(OUTPUT_ANALYSIS_CSV, index=False)
    print(f"Analysis complete. Results saved to {OUTPUT_ANALYSIS_CSV}")
    
    # Display Top Passed Opportunities
    passed = analysis_df[analysis_df["status"] == "PASS"]
    if not passed.empty:
        print("\nTop Liquidity-Verified Opportunities:")
        print(passed[["kalshi_market", "strategy", "profit_pct", "daily_roi", "liquidity_usd"]].head(10).to_string(index=False))
    else:
        print("\nNo opportunities passed the liquidity/efficiency filters.")
        print("Top failed opportunities for context:")
        print(analysis_df[["kalshi_market", "strategy", "profit_pct", "daily_roi", "liquidity_usd", "fail_reason"]].head(5).to_string(index=False))

if __name__ == "__main__":
    analyze_liquidity_and_efficiency()
