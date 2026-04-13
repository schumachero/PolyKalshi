import os
import argparse
import pandas as pd
from typing import Optional
from arb_base import (
    MarketState, TRACKED_PAIRS_CSV, utc_now_iso, append_execution_log,
    get_yes_no_books_kalshi, get_outcome_books_polymarket,
    liquidate_pair, place_dual_orders, send_telegram_message,
    calculate_holding_apy, DEFAULT_MIN_SWAP_GAIN_USD, DEFAULT_MIN_SWAP_APY_DELTA,
    SWAP_FEE_CUSHION_PCT
)
from arb_buy import get_candidate_for_pair, reverify_pair_live

def evaluate_swap_opportunity(candidate_arb: dict, state: MarketState) -> Optional[dict]:
    """
    Check if we should sell a held position to buy candidate_arb.
    Returns the liquidation plan if a swap is advantageous.
    Identical logic to original monitor.
    """
    cand_apy = candidate_arb.get('apy', 0.0)
    
    worst_pid = None
    worst_apy = 999999.0
    best_plan = {}
    
    for pid, info in state.exposure.items():
        if info['value_usd'] < 0.50: continue
            
        k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        if not k_books or not p_books: continue
            
        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        p_bids = p_books['sell_levels']
        if not k_bids or not p_bids: continue
            
        # LIVE DEPTH CHECK: How much can we actually sell?
        # We allow selling slightly below cost per user feedback
        k_depth = get_executable_depth(k_bids, info['avg_price_kalshi'] - 0.01)
        p_depth = get_executable_depth(p_bids, info['avg_price_poly'] - 0.01)
        
        liq_qty = int(min(k_depth, p_depth, info['contracts_kalshi'], info['contracts_poly']))
        if liq_qty <= 0: continue
            
        # Weighted average exit price for this quantity
        avg_k_exit = calculate_average_fill_price(k_bids, liq_qty)
        avg_p_exit = calculate_average_fill_price(p_bids, liq_qty)
        current_unit_value = avg_k_exit + avg_p_exit
        
        # Remaining APY if we HOLD
        h_apy = calculate_holding_apy({
            'value_usd': liq_qty * current_unit_value,
            'contracts_kalshi': liq_qty,
            'contracts_poly': liq_qty,
            'close_time': info['close_time']
        })
        
        # PRIORITY LOCK: Maturity / Exits
        if current_unit_value >= 0.99: h_apy = -10.0 
        elif current_unit_value >= 0.98: h_apy = -1.0
            
        if h_apy < worst_apy:
            worst_apy = h_apy
            worst_pid = pid
            best_plan = {
                "pair_id": pid,
                "qty": liq_qty,
                "k_price_cents": int(round(avg_k_exit * 100)),
                "p_price": avg_p_exit,
                "value_usd": liq_qty * current_unit_value,
                "cost_basis_total": liq_qty * (info['avg_price_kalshi'] + info['avg_price_poly'])
            }
            
    if not worst_pid: return None
    
    # Check APY Hurdle
    if cand_apy < worst_apy + DEFAULT_MIN_SWAP_APY_DELTA: return None
        
    # Check Fee/Slippage Adjusted Absolute Gain
    liq_notional = best_plan['value_usd']
    exit_pnl = liq_notional - best_plan['cost_basis_total']
    
    new_contracts = liq_notional / candidate_arb['sum_price']
    new_profit = new_contracts * (1.0 - candidate_arb['sum_price'])
    
    total_swap_pnl = exit_pnl + new_profit
    buffer = liq_notional * (SWAP_FEE_CUSHION_PCT / 100.0)
    net_gain = total_swap_pnl - buffer
    
    if net_gain < DEFAULT_MIN_SWAP_GAIN_USD:
        print(f"Swap rejected: Net gain ${net_gain:.2f} < min ${DEFAULT_MIN_SWAP_GAIN_USD:.2f}")
        return None
        
    return {
        "sell_pair_id": worst_pid,
        "sell_qty": best_plan['qty'],
        "sell_k_cents": best_plan['k_price_cents'],
        "sell_p_price": best_plan['p_price'],
        "sell_value_usd": liq_notional,
        "net_gain": net_gain,
        "apy_imp": cand_apy - worst_apy,
        "worst_apy": worst_apy
    }

def run_phase(state: MarketState, dry_run: bool = True):
    if state.ks_cash >= 2.5 and state.pm_cash >= 2.5: return # Have cash, skip swap evaluation

    candidates = []
    for _, row in state.df.iterrows():
        cand = get_candidate_for_pair(row, 0.0, 0.0) # Find anything
        if cand: candidates.append(cand)
    if not candidates: return
    best = max(candidates, key=lambda x: x['apy'])

    swap = evaluate_swap_opportunity(best, state)
    if not swap: return

    sid = swap['sell_pair_id']
    bid = best['pair_id']
    msg = f"SWAP: {sid} ({swap['worst_apy']:.1f}%) -> {bid} ({best['apy']:.1f}%) | Gain: ${swap['net_gain']:.2f}"
    print(msg)

    if dry_run:
        append_execution_log({"timestamp": utc_now_iso(), "pair_id": bid, "status": "dry_run_swap", "message": msg})
        return

    # 1. Liquidate
    sres = liquidate_pair(sid, state.exposure[sid], swap['sell_qty'], swap['sell_qty'], swap['sell_k_cents'], swap['sell_p_price'], dry_run=False)
    if sres['status'] != 'success': return

    # 2. Buy
    target = best
    target['contracts'] = swap['sell_qty']
    target['notional_usd'] = target['contracts'] * target['sum_price']
    
    live = reverify_pair_live(best['pair_row'], best)
    if live.get('found'):
        live['contracts'] = swap['sell_qty']
        target = live

    res = place_dual_orders(target['pair_row'], target, state)
    append_execution_log({"timestamp": utc_now_iso(), "pair_id": bid, "status": f"swap_{res['status']}", "message": f"Swap from {sid}"})

def main():
    parser = argparse.ArgumentParser(description="APY Swap Monitor")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    
    df = pd.read_csv(TRACKED_PAIRS_CSV)
    state = MarketState(df)
    if state.refresh():
        run_phase(state, dry_run=not args.live)

if __name__ == "__main__":
    main()
