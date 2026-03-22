import pandas as pd
import os
import sys

# Ensure we can import from src/notifications
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from notifications.telegram_bot import notify_arbitrage
except ImportError:
    def notify_arbitrage(*args, **kwargs): pass

# IO
INPUT_CSV = "Data/matched_orderbooks.csv"
OUTPUT_CSV = "Data/arbitrage_opportunities.csv"

# Filters
MIN_SCORE = 0.3
MIN_PROFIT = 0.1 # Minimum profit to consider
NOTIFICATION_THRESHOLD = 5.0 # Notify if profit >= 5%

def calculate_arbitrage():
    if not os.path.exists(INPUT_CSV):
        print(f"{INPUT_CSV} not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} matches from {INPUT_CSV}")

    # Drop rows without a score
    df = df.dropna(subset=["combined_score"])
    
    # Filter by match score
    df = df[df["combined_score"] >= MIN_SCORE]
    print(f"Filtered to {len(df)} matches with score >= {MIN_SCORE}")

    results = []

    for _, row in df.iterrows():
        # Direction 1: Buy YES on Kalshi, Buy NO on Polymarket
        # Need k_yes_ask and p_no_ask
        if pd.notna(row["k_yes_ask"]) and pd.notna(row["p_no_ask"]):
            cost1 = row["k_yes_ask"] + row["p_no_ask"]
            profit1 = 100 - cost1
            if profit1 > MIN_PROFIT:
                res = row.to_dict()
                res["direction"] = "K_YES_P_NO"
                res["total_cost"] = cost1
                res["expected_profit"] = profit1
                results.append(res)
                
                if profit1 >= NOTIFICATION_THRESHOLD:
                    print(f"!!! HIGH PROFIT !!! Sending notification for {row['kalshi_market']} ({profit1:.2f}%)")
                    notify_arbitrage(res)
            
        # Direction 2: Buy YES on Poly, Buy NO on Kalshi
        # Need p_yes_ask and k_no_ask
        if pd.notna(row["p_yes_ask"]) and pd.notna(row["k_no_ask"]):
            cost2 = row["p_yes_ask"] + row["k_no_ask"]
            profit2 = 100 - cost2
            if profit2 > MIN_PROFIT:
                res = row.to_dict()
                res["direction"] = "P_YES_K_NO"
                res["total_cost"] = cost2
                res["expected_profit"] = profit2
                results.append(res)
                
                if profit2 >= NOTIFICATION_THRESHOLD:
                    print(f"!!! HIGH PROFIT !!! Sending notification for {row['kalshi_market']} ({profit2:.2f}%)")
                    notify_arbitrage(res)

    if not results:
        print("No arbitrage opportunities found.")
        # Create an empty file with headers to maintain consistency
        pd.DataFrame(columns=df.columns.tolist() + ["direction", "total_cost", "expected_profit"]).to_csv(OUTPUT_CSV, index=False)
        return

    out_df = pd.DataFrame(results)
    
    # Sort by profit descending
    out_df = out_df.sort_values(by="expected_profit", ascending=False)
    
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Found {len(out_df)} opportunities. Results saved to {OUTPUT_CSV}")

    # Display Top 5
    print("\nTop 5 Arbitrage Opportunities:")
    cols_to_show = ["kalshi_market", "polymarket_market", "direction", "expected_profit", "combined_score"]
    print(out_df[cols_to_show].head(5).to_string(index=False))

if __name__ == "__main__":
    calculate_arbitrage()
