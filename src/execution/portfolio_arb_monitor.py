import os
import time
import argparse
import pandas as pd
from arb_base import (
    TRACKED_PAIRS_CSV, DEFAULT_SLEEP_MINUTES, DEFAULT_MIN_PROFIT_PCT,
    DEFAULT_MIN_LIQUIDITY_USD, DEFAULT_REVERIFY_BOOKS, MarketState
)
import arb_hanging_leg
import arb_sell
import arb_buy
import arb_swap

def run_once(df: pd.DataFrame, dry_run: bool = True, min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT, min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD, reverify: bool = DEFAULT_REVERIFY_BOOKS, verbose: bool = False, enabled_phases: list = None):
    state = MarketState(df)
    if not state.refresh(): return

    if not enabled_phases:
        enabled_phases = ["hanging", "sell", "buy", "swap"]

    # Phase 1: Hanging Legs
    if "hanging" in enabled_phases:
        arb_hanging_leg.run_phase(state, dry_run=dry_run)
        state.refresh() # Refresh after fixes

    # Phase 2: Maturity Exits (Sell)
    if "sell" in enabled_phases:
        arb_sell.run_phase(state, dry_run=dry_run)
        state.refresh() # Refresh after exits

    # Phase 3: Direct Buy
    if "buy" in enabled_phases:
        arb_buy.run_phase(state, dry_run=dry_run, min_profit=min_profit_pct, min_liq=min_liquidity_usd, reverify=reverify)
        # Note: If buy happens, we might not want to refresh instantly as cash is gone. 
        # But if it's the last phase in sequence, it's fine.

    # Phase 4: APY Swap
    if "swap" in enabled_phases:
        arb_swap.run_phase(state, dry_run=dry_run)

def main():
    parser = argparse.ArgumentParser(description="Modular Portfolio Arbitrage Orchestrator")
    parser.add_argument("--input", default=TRACKED_PAIRS_CSV)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-minutes", type=int, default=DEFAULT_SLEEP_MINUTES)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--min-profit-pct", type=float, default=DEFAULT_MIN_PROFIT_PCT)
    parser.add_argument("--min-liquidity-usd", type=float, default=DEFAULT_MIN_LIQUIDITY_USD)
    parser.add_argument("--no-reverify", action="store_true")
    
    # Phase toggles
    parser.add_argument("--only", type=str, help="Comma-separated phases to run (hanging,sell,buy,swap)")
    parser.add_argument("--skip", type=str, help="Comma-separated phases to skip")

    args = parser.parse_args()
    dry_run = not args.live
    reverify = not args.no_reverify
    
    phases = ["hanging", "sell", "buy", "swap"]
    if args.only:
        phases = [p.strip() for p in args.only.split(",")]
    if args.skip:
        skip = [p.strip() for p in args.skip.split(",")]
        phases = [p for p in phases if p not in skip]

    if not args.loop:
        df = pd.read_csv(args.input)
        run_once(df, dry_run, args.min_profit_pct, args.min_liquidity_usd, reverify, enabled_phases=phases)
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1
    while True:
        start = time.time()
        print(f"\n### ITERATION {iteration} | Phases: {phases} ###")
        try:
            df = pd.read_csv(args.input)
            run_once(df, dry_run, args.min_profit_pct, args.min_liquidity_usd, reverify, enabled_phases=phases)
        except Exception as e:
            print(f"Iteration error: {e}")
        
        elapsed = time.time() - start
        sleep_sec = max(interval_seconds - elapsed, 0)
        print(f"Done in {elapsed:.1f}s. Sleeping {sleep_sec/60:.2f}m...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            break
        iteration += 1

if __name__ == "__main__":
    main()
