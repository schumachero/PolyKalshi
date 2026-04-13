import os
import time
import argparse
import traceback
import pandas as pd
from arb_base import (
    TRACKED_PAIRS_CSV, DEFAULT_SLEEP_MINUTES, DEFAULT_MIN_PROFIT_PCT,
    DEFAULT_MIN_LIQUIDITY_USD, DEFAULT_REVERIFY_BOOKS, DEFAULT_MAX_TRADE_USD,
    THRESHOLD_SELL, MarketState, normalize_str, truthy
)
import arb_hanging_leg
import arb_sell
import arb_buy
import arb_swap
from datetime import datetime

SEPARATOR = "=" * 70
PHASE_SEP = "-" * 50

def fmt_usd(v): return f"${v:.2f}"
def fmt_pct(v): return f"{v:.2f}%"


def print_config(dry_run, min_profit_pct, min_liquidity_usd, reverify, phases):
    """Print the active configuration at the start of each run."""
    print(f"\n{SEPARATOR}")
    print(f"  ORCHESTRATOR CONFIG")
    print(f"{SEPARATOR}")
    print(f"  Mode            : {'DRY RUN' if dry_run else '🔴 LIVE TRADING'}")
    print(f"  Min Profit      : {fmt_pct(min_profit_pct)}")
    print(f"  Min Liquidity   : {fmt_usd(min_liquidity_usd)}")
    print(f"  Max Trade/Pair  : {fmt_usd(DEFAULT_MAX_TRADE_USD)}")
    print(f"  Sell Threshold  : {THRESHOLD_SELL}")
    print(f"  Reverify Books  : {reverify}")
    print(f"  Enabled Phases  : {', '.join(phases)}")
    print(f"{SEPARATOR}\n")


def print_state_snapshot(state: MarketState, label: str = ""):
    """Print a detailed snapshot of the current market state."""
    tag = f" ({label})" if label else ""
    print(f"\n{PHASE_SEP}")
    print(f"  STATE SNAPSHOT{tag}")
    print(f"{PHASE_SEP}")
    print(f"  Kalshi Cash     : {fmt_usd(state.ks_cash)}")
    print(f"  Polymarket Cash : {fmt_usd(state.pm_cash)}")
    print(f"  Total Cash      : {fmt_usd(state.ks_cash + state.pm_cash)}")
    print(f"  Position Value  : {fmt_usd(state.total_portfolio_usd)}")
    print(f"  Total NAV       : {fmt_usd(state.total_nav)}")
    print(f"  Tracked Pairs   : {len(state.df)}")

    # Count active positions
    active_positions = {pid: info for pid, info in state.exposure.items()
                        if info['value_usd'] > 0.01}
    print(f"  Active Positions: {len(active_positions)}")

    if active_positions:
        print(f"\n  {'Pair ID':<55} {'K Qty':>6} {'P Qty':>6} {'Value':>8} {'Side'}")
        print(f"  {'─' * 55} {'─' * 6} {'─' * 6} {'─' * 8} {'─' * 10}")
        for pid, info in sorted(active_positions.items(), key=lambda x: -x[1]['value_usd']):
            k_qty = info['contracts_kalshi']
            p_qty = info['contracts_poly']
            val = info['value_usd']
            side = f"K:{info['kalshi_side'].upper()} P:{info['polymarket_side'].upper()}" if info['kalshi_side'] else "—"
            balanced = "✓" if abs(k_qty - p_qty) <= 1 else "⚠ UNBALANCED"
            print(f"  {pid:<55} {k_qty:>6.0f} {p_qty:>6.0f} {fmt_usd(val):>8} {side} {balanced}")
    print()


def run_phase_with_timing(phase_name: str, phase_fn, *args, **kwargs):
    """Run a phase function with timing and error handling."""
    print(f"\n{'▶':} PHASE: {phase_name.upper()}")
    print(f"  {PHASE_SEP}")
    start = time.time()
    try:
        phase_fn(*args, **kwargs)
        elapsed = time.time() - start
        print(f"  ✓ {phase_name} completed in {elapsed:.2f}s")
        return {"status": "ok", "elapsed": elapsed}
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ✗ {phase_name} FAILED after {elapsed:.2f}s: {e}")
        traceback.print_exc()
        return {"status": "error", "elapsed": elapsed, "error": str(e)}


def run_once(df: pd.DataFrame, dry_run: bool = True,
             min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT,
             min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD,
             reverify: bool = DEFAULT_REVERIFY_BOOKS,
             verbose: bool = False, enabled_phases: list = None):

    if not enabled_phases:
        enabled_phases = ["hanging", "sell", "buy", "swap"]

    print_config(dry_run, min_profit_pct, min_liquidity_usd, reverify, enabled_phases)

    # --- Initial state ---
    print("Initializing MarketState...")
    state = MarketState(df)
    if not state.refresh():
        print("❌ CRITICAL: MarketState.refresh() failed. Aborting this iteration.")
        return

    print_state_snapshot(state, "INITIAL")

    phase_results = {}

    # Phase 1: Hanging Legs
    if "hanging" in enabled_phases:
        result = run_phase_with_timing(
            "Hanging Leg Detector",
            arb_hanging_leg.run_phase, state, dry_run=dry_run
        )
        phase_results["hanging"] = result

        if result["status"] == "ok":
            print("  Refreshing state after hanging leg fixes...")
            state.refresh()
    else:
        print("\n⏭ SKIP: Hanging Leg phase (disabled)")

    # Phase 2: Maturity Exits (Sell)
    if "sell" in enabled_phases:
        result = run_phase_with_timing(
            "Maturity Exit (Sell)",
            arb_sell.run_phase, state, dry_run=dry_run
        )
        phase_results["sell"] = result

        if result["status"] == "ok":
            print("  Refreshing state after sell exits...")
            state.refresh()
    else:
        print("\n⏭ SKIP: Sell phase (disabled)")

    # Phase 3: Direct Buy
    if "buy" in enabled_phases:
        print(f"\n  Buy config: min_profit={fmt_pct(min_profit_pct)}, "
              f"min_liq={fmt_usd(min_liquidity_usd)}, reverify={reverify}")
        print(f"  Available cash: Kalshi={fmt_usd(state.ks_cash)}, Poly={fmt_usd(state.pm_cash)}")

        if state.ks_cash < 2.5 or state.pm_cash < 2.5:
            print(f"  ⚠ Low cash on one or both venues — buy phase may skip trades")

        result = run_phase_with_timing(
            "Direct Buy",
            arb_buy.run_phase, state, dry_run=dry_run,
            min_profit=min_profit_pct, min_liq=min_liquidity_usd, reverify=reverify
        )
        phase_results["buy"] = result
    else:
        print("\n⏭ SKIP: Buy phase (disabled)")

    # Phase 4: APY Swap
    if "swap" in enabled_phases:
        print(f"\n  Swap eligibility: cash_sufficient={state.ks_cash >= 2.5 and state.pm_cash >= 2.5}")
        if state.ks_cash >= 2.5 and state.pm_cash >= 2.5:
            print(f"  ℹ Swap phase will skip (enough cash for direct buy)")

        result = run_phase_with_timing(
            "APY Swap",
            arb_swap.run_phase, state, dry_run=dry_run
        )
        phase_results["swap"] = result
    else:
        print("\n⏭ SKIP: Swap phase (disabled)")

    # --- Final summary ---
    print(f"\n{SEPARATOR}")
    print(f"  ITERATION SUMMARY")
    print(f"{SEPARATOR}")
    for phase, res in phase_results.items():
        status_icon = "✓" if res["status"] == "ok" else "✗"
        print(f"  {status_icon} {phase:<15} {res['elapsed']:.2f}s  {res.get('error', '')}")
    total_time = sum(r["elapsed"] for r in phase_results.values())
    print(f"  {'─' * 40}")
    print(f"  Total phase time: {total_time:.2f}s")
    print(f"{SEPARATOR}\n")


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
        print(f"\n{'#' * 70}")
        print(f"  SINGLE RUN  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#' * 70}")
        df = pd.read_csv(args.input)
        print(f"Loaded {len(df)} tracked pairs from {args.input}")
        run_once(df, dry_run, args.min_profit_pct, args.min_liquidity_usd, reverify, enabled_phases=phases)
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1
    while True:
        start = time.time()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n{'#' * 70}")
        print(f"  ITERATION {iteration}  |  {now}  |  Phases: {phases}")
        print(f"{'#' * 70}")

        try:
            df = pd.read_csv(args.input)
            print(f"Loaded {len(df)} tracked pairs from {args.input}")
            run_once(df, dry_run, args.min_profit_pct, args.min_liquidity_usd, reverify, enabled_phases=phases)
        except Exception as e:
            print(f"\n❌ FATAL ITERATION ERROR: {e}")
            traceback.print_exc()

        elapsed = time.time() - start
        sleep_sec = max(interval_seconds - elapsed, 0)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{now}] Iteration {iteration} done in {elapsed:.1f}s. Sleeping {sleep_sec/60:.1f}m ...")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            print("\n🛑 Stopped by user.")
            break
        iteration += 1


if __name__ == "__main__":
    main()

