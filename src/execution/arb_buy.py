import os
import argparse
import pandas as pd
from typing import List, Dict, Optional
from arb_base import (
    MarketState, TRACKED_PAIRS_CSV, utc_now_iso, append_execution_log,
    get_yes_no_books_kalshi, get_outcome_books_polymarket,
    normalize_book_side, calculate_apy, safe_float, normalize_str,
    KALSHI_FEE_BUFFER, POLY_FEE_BUFFER, DEFAULT_MIN_PROFIT_PCT,
    DEFAULT_MIN_LIQUIDITY_USD, DEFAULT_MAX_TRADE_USD,
    place_dual_orders
)

def consume_dual_books(levels_a, levels_b, max_trade_usd) -> dict:
    a = normalize_book_side(levels_a)
    b = normalize_book_side(levels_b)
    if not a or not b: return {"found": False}
    i, j, total_c, cost_a, cost_b = 0, 0, 0.0, 0.0, 0.0
    levels_used = []
    while i < len(a) and j < len(b):
        pa, pb = a[i]["price"], b[j]["price"]
        msum = pa + pb + KALSHI_FEE_BUFFER + POLY_FEE_BUFFER
        if msum >= 1.0: break
        max_aff = (max_trade_usd - (cost_a + cost_b)) / max(msum, 1e-12)
        if max_aff <= 1e-12: break
        exec_sz = min(a[i]["size"], b[j]["size"], max_aff)
        if exec_sz <= 1e-12: break
        total_c += exec_sz
        cost_a += exec_sz * pa
        cost_b += exec_sz * pb
        levels_used.append({"depth_a": i, "depth_b": j, "price_a": pa, "price_b": pb, "contracts": exec_sz})
        a[i]["size"] -= exec_sz
        b[j]["size"] -= exec_sz
        if a[i]["size"] <= 1e-12: i += 1
        if b[j]["size"] <= 1e-12: j += 1
    if total_c <= 1e-12: return {"found": False}
    avg_a, avg_b = cost_a / total_c, cost_b / total_c
    avg_sum = avg_a + avg_b + KALSHI_FEE_BUFFER + POLY_FEE_BUFFER
    return {
        "found": True, "contracts": total_c, "price_a": avg_a, "price_b": avg_b,
        "sum_price": avg_sum, "profit_pct": (1.0 - avg_sum) * 100.0, "notional_usd": total_c * avg_sum
    }

def choose_best_arb_for_pair(pair_row: pd.Series) -> dict:
    kt, ps = normalize_str(pair_row["kalshi_ticker"]), normalize_str(pair_row["polymarket_ticker"])
    max_usd = safe_float(pair_row.get("max_position_per_pair_usd", DEFAULT_MAX_TRADE_USD))
    k_books = get_yes_no_books_kalshi(kt)
    p_no = get_outcome_books_polymarket(ps, "NO")
    p_yes = get_outcome_books_polymarket(ps, "YES")
    if not k_books or not p_no or not p_yes: return {"found": False}
    options = []
    c1 = consume_dual_books(k_books["yes_asks"], p_no["buy_levels"], max_usd)
    if c1["found"]:
        c1.update({"kalshi_side": "yes", "polymarket_outcome": "NO", "kalshi_ticker": kt, "polymarket_ticker": ps})
        options.append(c1)
    c2 = consume_dual_books(k_books["no_asks"], p_yes["buy_levels"], max_usd)
    if c2["found"]:
        c2.update({"kalshi_side": "no", "polymarket_outcome": "YES", "kalshi_ticker": kt, "polymarket_ticker": ps})
        options.append(c2)
    if not options: return {"found": False}
    close_time = normalize_str(pair_row.get("close_time", ""))
    for opt in options:
        opt["pair_id"] = normalize_str(pair_row.get("pair_id", f"{kt}__{ps}"))
        opt["close_time"] = close_time
        opt["apy"] = calculate_apy(opt["profit_pct"], close_time)
    return max(options, key=lambda x: (x["apy"], x["profit_pct"]))

def get_candidate_for_pair(pair_row: pd.Series, min_profit: float, min_liq: float) -> Optional[dict]:
    arb = choose_best_arb_for_pair(pair_row)
    if not arb.get("found") or arb["profit_pct"] < min_profit or arb["notional_usd"] < min_liq:
        return None
    arb["pair_row"] = pair_row
    return arb

def reverify_pair_live(pair_row: pd.Series, original_arb: dict) -> dict:
    # Simplified re-verify
    return choose_best_arb_for_pair(pair_row)

def run_phase(state: MarketState, dry_run: bool = True, min_profit: float = DEFAULT_MIN_PROFIT_PCT, min_liq: float = DEFAULT_MIN_LIQUIDITY_USD, reverify: bool = True):
    candidates = []
    for _, row in state.df.iterrows():
        cand = get_candidate_for_pair(row, min_profit, min_liq)
        if cand: candidates.append(cand)
    
    if not candidates:
        print("No buy opportunities found.")
        return

    candidates.sort(key=lambda x: x['apy'], reverse=True)
    best = candidates[0]
    pid = best['pair_id']
    print(f"BEST CANDIDATE: [{pid}] Profit={best['profit_pct']:.2f}% | APY={best['apy']:.1f}%")

    if state.ks_cash >= 2.5 and state.pm_cash >= 2.5:
        target = best
        if reverify:
            live = reverify_pair_live(best['pair_row'], best)
            if not live.get('found'): return
            target = live
            target['apy'] = calculate_apy(target['profit_pct'], target.get('close_time', ''))

        if dry_run:
            print(f"[{pid}] Dry run: Would BUY.")
            return

        res = place_dual_orders(target['pair_row'], target, state)
        append_execution_log({
            "timestamp": utc_now_iso(), "pair_id": pid, "status": res["status"],
            "apy": target['apy'], "message": f"Direct buy: {res.get('message')}"
        })

def main():
    parser = argparse.ArgumentParser(description="Direct Buy Monitor")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    
    df = pd.read_csv(TRACKED_PAIRS_CSV)
    state = MarketState(df)
    if state.refresh():
        run_phase(state, dry_run=not args.live)

if __name__ == "__main__":
    main()
