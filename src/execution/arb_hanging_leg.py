import os
import argparse
from arb_base import (
    MarketState, TRACKED_PAIRS_CSV, utc_now_iso, append_execution_log,
    get_yes_no_books_kalshi, get_outcome_books_polymarket,
    kalshi_place_limit_order, polymarket_place_limit_order,
    send_telegram_message, liquidate_pair, HANGING_LEG_REBALANCE_MAX_COST,
    SLIPPAGE_PROTECTION_FLOOR_PCT
)
import pandas as pd

def find_unbalanced_pairs(state: MarketState) -> list:
    unbalanced = []
    for pid, info in state.exposure.items():
        k_qty = info['contracts_kalshi']
        p_qty = info['contracts_poly']
        if abs(k_qty - p_qty) > 1.0:
            unbalanced.append({
                "pair_id": pid, "k_qty": k_qty, "p_qty": p_qty,
                "diff": k_qty - p_qty, "info": info
            })
    return unbalanced

def resolve_hanging_leg(unbalanced_info: dict, dry_run: bool = True) -> dict:
    pid = unbalanced_info['pair_id']
    info = unbalanced_info['info']
    diff = unbalanced_info['diff']
    missing_venue = "Poly" if diff > 0 else "Kalshi"
    qty_fix = int(abs(diff))
    
    print(f"[{pid}] Fixing {missing_venue} leg ({qty_fix} units)...")
    if dry_run: return {"status": "dry_run", "message": f"Would fix {qty_fix} {missing_venue}"}

    k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
    p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
    if not k_books or not p_books: return {"status": "error", "message": "Book fetch failed"}

    if missing_venue == "Poly":
        held_cost = info['avg_price_kalshi']
        poly_asks = p_books['buy_levels']
        if not poly_asks: return {"status": "error", "message": "No Poly depth"}
        curr_ask = poly_asks[0]['price']
        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        if not k_bids: return {"status": "error", "message": "No Kalshi depth"}
        curr_bid = k_bids[0]['price']
        held_venue = "Kalshi"
    else:
        held_cost = info['avg_price_poly']
        k_asks = k_books['yes_asks'] if info['kalshi_side'] == 'yes' else k_books['no_asks']
        if not k_asks: return {"status": "error", "message": "No Kalshi depth"}
        curr_ask = k_asks[0]['price']
        p_bids = p_books['sell_levels']
        if not p_bids: return {"status": "error", "message": "No Poly depth"}
        curr_bid = p_bids[0]['price']
        held_venue = "Poly"

    total_cost = held_cost + curr_ask

    # Tier 1 & 3: Hedge
    if total_cost <= HANGING_LEG_REBALANCE_MAX_COST:
        tier = "PROFIT" if total_cost < 1.0 else "BREAKEVEN"
        if missing_venue == "Poly":
            polymarket_place_limit_order(slug=info['polymarket_ticker'], outcome=info['polymarket_side'], size=qty_fix, price=curr_ask, side="BUY", order_type="IOC")
        else:
            kalshi_place_limit_order(ticker=info['kalshi_ticker'], side=info['kalshi_side'], action="buy", count=qty_fix, price_cents=int(round(curr_ask*100)), time_in_force="immediate_or_cancel")
        send_telegram_message(f"💰 <b>Hanging Leg Fixed ({tier})</b>: {pid}\nCost: ${total_cost:.4f}")
        return {"status": "success", "message": f"{tier} hedge executed"}

    # Tier 2 & 4: Liquidate
    if curr_bid >= held_cost * (1 - SLIPPAGE_PROTECTION_FLOOR_PCT/100.0):
        res = liquidate_pair(pid, info, k_qty=qty_fix if held_venue=="Kalshi" else 0, p_qty=qty_fix if held_venue=="Poly" else 0, k_min_cents=int(round(curr_bid*100)), p_min=curr_bid, dry_run=False)
        if res['status'] == 'success':
            send_telegram_message(f"⚠️ <b>Hanging Leg Liquidated</b>: {pid}\nPrice: ${curr_bid:.4f}")
        return res

    # Tier 5: Alert
    send_telegram_message(f"🚨 <b>Hanging Leg CRITICAL</b>: {pid}\nNo safe exit. Bid: ${curr_bid:.4f}")
    return {"status": "error", "message": "No safe exit"}

def run_phase(state: MarketState, dry_run: bool = True):
    unbalanced = find_unbalanced_pairs(state)
    if not unbalanced:
        print("No hanging legs detected.")
        return
    
    for item in unbalanced:
        res = resolve_hanging_leg(item, dry_run=dry_run)
        append_execution_log({
            "timestamp": utc_now_iso(), "pair_id": item['pair_id'],
            "status": f"fix_{res['status']}", "message": res.get("message", "")
        })

def main():
    parser = argparse.ArgumentParser(description="Hanging Leg Monitor")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    
    df = pd.read_csv(TRACKED_PAIRS_CSV)
    state = MarketState(df)
    if state.refresh():
        run_phase(state, dry_run=not args.live)

if __name__ == "__main__":
    main()
