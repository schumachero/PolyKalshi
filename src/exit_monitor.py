import os
import pandas as pd
from dotenv import load_dotenv

# Import our portfolio and semantic matching logic
try:
    from apis.portfolio import get_kalshi_positions, get_polymarket_positions
    from matching.semantic_matching import generate_semantic_matches
    from notifications.telegram_bot import send_telegram_message
except ImportError:
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from apis.portfolio import get_kalshi_positions, get_polymarket_positions
    from matching.semantic_matching import generate_semantic_matches
    from notifications.telegram_bot import send_telegram_message

# --- CONFIGURATION ---
MATCH_THRESHOLD = 0.75       # Score above which we consider positions a pair
COMBINED_EXIT_TARGET = 0.99  # Exit when total value reaches 99 cents

load_dotenv()

def run_exit_monitor():
    print("\n" + "=" * 60)
    print("  EXIT MONITOR - HEDGE CONVERGENCE")
    print("=" * 60)

    # 1. Fetch Positions
    print("Fetching positions from Kalshi and Polymarket...")
    k_positions = get_kalshi_positions()
    p_positions = get_polymarket_positions()

    if not k_positions or not p_positions:
        print("Required positions on both platforms not found. Nothing to monitor.")
        return

    # 2. Match the portfolio pairs
    print("Pairing Kalshi and Polymarket holdings...")
    k_df = pd.DataFrame([{
        "market_ticker": p["ticker"],
        "market_title": p["title"],
        "rules_text": p["rules"]
    } for p in k_positions])

    p_df = pd.DataFrame([{
        "market_ticker": p["market_id"],
        "market_title": p["title"],
        "rules_text": p["rules"],
        "status": "active"
    } for p in p_positions])

    matches_df = generate_semantic_matches(k_df, p_df, threshold=MATCH_THRESHOLD)

    if matches_df.empty:
        print("No matching hedge pairs found in your current portfolio.")
        return

    # 3. Analyze each matched pair for convergence
    print(f"Analyzing {len(matches_df)} hedge pairs...")
    for _, m in matches_df.iterrows():
        kt = m['kalshi_market_ticker']
        pt = m['polymarket_market_ticker']
        
        # Pull original objects to get current prices
        k_pos = next((p for p in k_positions if p['ticker'] == kt), None)
        p_pos = next((p for p in p_positions if p['market_id'] == pt), None)
        
        if not k_pos or not p_pos: continue

        # --- Calculate Prices ---
        # Kalshi Price (0-1) - now using current market bid instead of initial entry cost
        k_price = k_pos.get("current_price", 0)
        
        # Polymarket Price (0-1)
        p_price = p_pos.get("current_price", 0)

        # Logic: If you hold Kalshi YES and Poly NO, convergence is Price_K_YES + Price_P_NO.
        # If the sides are opposite (which they should be for a hedge), the sum approaches 1.0.
        # We'll calculate the combined sell value based on the actual 'Side' (YES/NO) you hold.
        # e.g. If you hold Polymarket NO, the current_price should already reflect the NO price.
        
        combined_value = k_price + p_price

        print(f"  Analysing {kt} + {pt} | Combined Value: ${combined_value:.3f}")

        if combined_value >= COMBINED_EXIT_TARGET:
            msg = (
                f"💸 <b>Hedge Converged: Take Profit!</b>\n\n"
                f"Your paired positions have hit the 0.99 target.\n\n"
                f"🏛 <b>Kalshi:</b> {k_pos['title']} ({k_pos['side']})\n"
                f"💎 <b>Poly:</b> {p_pos['title']} ({p_pos['side']})\n\n"
                f"💰 <b>Combined Sell Value: ${combined_value:.3f}</b>\n\n"
            )
            send_telegram_message(msg)
            print(f"  [!] Alert sent for converged pair.")

    print("Exit check complete.")

if __name__ == "__main__":
    run_exit_monitor()
