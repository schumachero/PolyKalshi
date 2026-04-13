import os
import argparse
import math
import pandas as pd
from arb_base import (
    MarketState, TRACKED_PAIRS_CSV, utc_now_iso, append_execution_log,
    get_yes_no_books_kalshi, get_outcome_books_polymarket,
    liquidate_pair, send_telegram_message, THRESHOLD_SELL
)

def check_and_exit_matured_positions(state: MarketState, dry_run: bool = True) -> list:
    results = []
    for pid, info in state.exposure.items():
        if info['value_usd'] < 0.20: continue
        
        k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        if not k_books or not p_books: continue

        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        p_bids = p_books['sell_levels']
        if not k_bids or not p_bids: continue

        best_k = k_bids[0]['price']
        best_p = p_bids[0]['price']
        combined = best_k + best_p

        if combined >= THRESHOLD_SELL:
            qty = min(k_bids[0]['size'], p_bids[0]['size'], info['contracts_kalshi'], info['contracts_poly'])
            qty = int(math.floor(qty))
            if qty <= 0: continue

            print(f"[{pid}] MATURITY! Combined: {combined:.4f}. Selling {qty} units.")
            res = liquidate_pair(pid, info, k_qty=qty, p_qty=qty, k_min_cents=int(round(best_k*100)), p_min=best_p, dry_run=dry_run)
            
            if not dry_run and res.get('status') == 'success':
                 send_telegram_message(f"✅ <b>Maturity Exit</b>: {pid}\nQty: {qty} | Bid: {combined:.4f}")

            res['pair_id'] = pid
            results.append(res)
    return results

def run_phase(state: MarketState, dry_run: bool = True):
    matured = check_and_exit_matured_positions(state, dry_run=dry_run)
    if matured:
        print(f"Handled {len(matured)} maturity exits.")
        for res in matured:
            append_execution_log({
                "timestamp": utc_now_iso(), "pair_id": res['pair_id'],
                "status": f"exit_{res['status']}", "message": f"Maturity (0.9995): {res.get('message')}"
            })

def main():
    parser = argparse.ArgumentParser(description="Maturity Exit (Sell) Monitor")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    
    df = pd.read_csv(TRACKED_PAIRS_CSV)
    state = MarketState(df)
    if state.refresh():
        run_phase(state, dry_run=not args.live)

if __name__ == "__main__":
    main()
