"""
Lead-Lag Strategy Backtester
============================
Simulates a simple directional strategy based on detected lead-lag signals.

Usage:
    python -m src.leadlag.backtest --input Data/leadlag/price_series.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

# ── Strategy ─────────────────────────────────────────────────────────────────

def run_backtest_single(
    df_pair: pd.DataFrame,
    leader_col: str = "p_mid",
    lagger_col: str = "k_mid",
    threshold: float = 0.02,
    hold_period: int = 10,
    slippage: float = 0.005,
) -> dict:
    """
    Backtest one parameter combo for one pair.

    Strategy:
      - At time t, if leader moves up by > threshold -> buy lagger
      - If leader moves down by > threshold -> sell lagger
      - Hold for hold_period steps, exit at prevailing mid
      - Apply slippage on entry and exit

    Returns summary dict with PnL stats + trade list.
    """
    leader = df_pair[leader_col].values
    lagger = df_pair[lagger_col].values
    n = len(leader)

    trades = []
    open_position = None

    for t in range(1, n):
        # Close position if holding period expired
        if open_position and t >= open_position["exit_time"]:
            exit_price = lagger[t]
            if not np.isfinite(exit_price):
                continue

            if open_position["direction"] == "long":
                exit_price_adj = exit_price - slippage
                pnl = exit_price_adj - open_position["entry_price"]
            else:
                exit_price_adj = exit_price + slippage
                pnl = open_position["entry_price"] - exit_price_adj

            trades.append({
                "entry_time": open_position["entry_idx"],
                "exit_time": t,
                "direction": open_position["direction"],
                "entry_price": round(open_position["entry_price"], 4),
                "exit_price": round(exit_price_adj, 4),
                "pnl": round(pnl, 4),
                "hold_steps": t - open_position["entry_idx"],
            })
            open_position = None

        # Open new position if none active
        if open_position is None:
            if not np.isfinite(leader[t]) or not np.isfinite(leader[t - 1]):
                continue
            if not np.isfinite(lagger[t]):
                continue

            leader_move = leader[t] - leader[t - 1]

            if abs(leader_move) > threshold:
                direction = "long" if leader_move > 0 else "short"
                entry_price = lagger[t] + slippage if direction == "long" else lagger[t] - slippage

                open_position = {
                    "entry_idx": t,
                    "entry_price": entry_price,
                    "exit_time": t + hold_period,
                    "direction": direction,
                }

    # Summary stats
    if not trades:
        return {
            "n_trades": 0,
            "total_pnl": 0,
            "avg_pnl": 0,
            "win_rate": 0,
            "max_drawdown": 0,
            "trades": [],
        }

    pnls = [t["pnl"] for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    cumulative = np.cumsum(pnls)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0

    return {
        "n_trades": len(trades),
        "total_pnl": round(sum(pnls), 4),
        "avg_pnl": round(np.mean(pnls), 4),
        "median_pnl": round(float(np.median(pnls)), 4),
        "win_rate": round(wins / len(trades), 4),
        "max_drawdown": round(max_dd, 4),
        "sharpe": round(float(np.mean(pnls) / np.std(pnls)), 4) if np.std(pnls) > 0 else 0,
        "trades": trades,
    }


def run_grid_search(
    df_pair: pd.DataFrame,
    pair_id: str,
    thresholds: list[float] | None = None,
    hold_periods: list[int] | None = None,
    slippages: list[float] | None = None,
    leader_col: str = "p_mid",
    lagger_col: str = "k_mid",
) -> list[dict]:
    """Run backtest over grid of parameters for one pair."""
    if thresholds is None:
        thresholds = [0.01, 0.02, 0.03, 0.05]
    if hold_periods is None:
        hold_periods = [5, 10, 20, 60]
    if slippages is None:
        slippages = [0.005, 0.01, 0.02]

    results = []
    for thresh, hold, slip in product(thresholds, hold_periods, slippages):
        bt = run_backtest_single(
            df_pair,
            leader_col=leader_col,
            lagger_col=lagger_col,
            threshold=thresh,
            hold_period=hold,
            slippage=slip,
        )
        results.append({
            "pair_id": pair_id,
            "leader": leader_col,
            "lagger": lagger_col,
            "threshold": thresh,
            "hold_period": hold,
            "slippage": slip,
            "n_trades": bt["n_trades"],
            "total_pnl": bt["total_pnl"],
            "avg_pnl": bt["avg_pnl"],
            "win_rate": bt["win_rate"],
            "max_drawdown": bt["max_drawdown"],
            "sharpe": bt.get("sharpe", 0),
        })
    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def run_backtester(
    input_csv: str = "Data/leadlag/price_series.csv",
    output_csv: str = "Data/leadlag/backtest_results.csv",
) -> pd.DataFrame:
    """Run grid-search backtest for all pairs, both leader directions."""
    if not os.path.exists(input_csv):
        print(f"Input file not found: {input_csv}")
        return pd.DataFrame()

    df = pd.read_csv(input_csv)
    for col in ["k_mid", "p_mid"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"Loaded {len(df)} rows, {df['pair_id'].nunique()} pairs")

    all_results = []

    for pair_id, df_pair in df.groupby("pair_id"):
        df_pair = df_pair.sort_values("timestamp").reset_index(drop=True)
        valid = df_pair.dropna(subset=["k_mid", "p_mid"])

        if len(valid) < 30:
            print(f"  Skipping {pair_id}: only {len(valid)} valid rows")
            continue

        print(f"\n{'='*60}")
        print(f"Backtesting: {pair_id} ({len(valid)} observations)")

        # Test both directions
        for leader, lagger in [("p_mid", "k_mid"), ("k_mid", "p_mid")]:
            label = "Poly->Kalshi" if leader == "p_mid" else "Kalshi->Poly"
            print(f"\n  Direction: {label}")

            results = run_grid_search(
                valid, pair_id,
                leader_col=leader,
                lagger_col=lagger,
            )
            all_results.extend(results)

            # Print top 3
            sorted_res = sorted(results, key=lambda x: x["total_pnl"], reverse=True)
            for i, r in enumerate(sorted_res[:3]):
                print(f"    #{i+1} thresh={r['threshold']} hold={r['hold_period']} slip={r['slippage']}")
                print(f"        PnL={r['total_pnl']:.4f} trades={r['n_trades']} "
                      f"win={r['win_rate']:.1%} sharpe={r['sharpe']:.2f}")

    # Save
    Path(os.path.dirname(output_csv)).mkdir(parents=True, exist_ok=True)
    result_df = pd.DataFrame(all_results)
    result_df.to_csv(output_csv, index=False)
    print(f"\n\nBacktest results saved to {output_csv} ({len(result_df)} parameter combos)")

    # Global summary
    if not result_df.empty:
        best = result_df.sort_values("total_pnl", ascending=False).head(5)
        print("\n[TOP] Top 5 configurations across all pairs:")
        for _, r in best.iterrows():
            direction = "Poly->Kalshi" if r["leader"] == "p_mid" else "Kalshi->Poly"
            print(f"  {r['pair_id']} ({direction}): "
                  f"PnL={r['total_pnl']:.4f} win={r['win_rate']:.1%} "
                  f"thresh={r['threshold']} hold={r['hold_period']} slip={r['slippage']}")

    return result_df


def main():
    parser = argparse.ArgumentParser(description="Lead-lag backtest")
    parser.add_argument("--input", type=str, default="Data/leadlag/price_series.csv")
    parser.add_argument("--output", type=str, default="Data/leadlag/backtest_results.csv")
    args = parser.parse_args()

    run_backtester(input_csv=args.input, output_csv=args.output)


if __name__ == "__main__":
    main()
