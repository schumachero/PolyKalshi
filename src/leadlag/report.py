"""
Lead-Lag Report & Visualization
================================
Generates plots and a summary report from analysis + backtest results.

Usage:
    python -m src.leadlag.report
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


OUTPUT_DIR = "Data/leadlag/plots"


def plot_cross_correlation(results: list[dict], output_dir: str = OUTPUT_DIR):
    """Heatmap of cross-correlation values across pairs and lags."""
    if not HAS_MPL or not results:
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for direction_key, title_prefix in [
        ("poly_leads_kalshi_corr", "Poly -> Kalshi"),
        ("kalshi_leads_poly_corr", "Kalshi -> Poly"),
    ]:
        # Build matrix
        pair_ids = []
        all_lags = set()
        for r in results:
            corr_data = r.get(direction_key, {})
            if corr_data:
                pair_ids.append(r["pair_id"][:30])  # truncate long names
                all_lags.update(corr_data.keys())

        if not pair_ids or not all_lags:
            continue

        lags_sorted = sorted(all_lags, key=lambda x: int(x))
        matrix = np.full((len(pair_ids), len(lags_sorted)), np.nan)

        for i, r in enumerate(results):
            corr_data = r.get(direction_key, {})
            for j, lag in enumerate(lags_sorted):
                val = corr_data.get(lag) or corr_data.get(str(lag))
                if val is not None:
                    matrix[i, j] = val

        fig, ax = plt.subplots(figsize=(10, max(4, len(pair_ids) * 0.6)))
        im = ax.imshow(matrix, cmap="RdYlGn", aspect="auto", vmin=-0.3, vmax=0.3)
        ax.set_xticks(range(len(lags_sorted)))
        ax.set_xticklabels([str(l) for l in lags_sorted])
        ax.set_yticks(range(len(pair_ids)))
        ax.set_yticklabels(pair_ids, fontsize=7)
        ax.set_xlabel("Lag (steps)")
        ax.set_title(f"Cross-Correlation: {title_prefix}")
        plt.colorbar(im, ax=ax, shrink=0.8)

        # Annotate cells
        for i in range(matrix.shape[0]):
            for j in range(matrix.shape[1]):
                if np.isfinite(matrix[i, j]):
                    ax.text(j, i, f"{matrix[i,j]:.2f}", ha="center", va="center", fontsize=7)

        fig.tight_layout()
        fname = f"cross_corr_{direction_key}.png"
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)
        print(f"  Saved {fname}")


def plot_directional_accuracy(results: list[dict], output_dir: str = OUTPUT_DIR):
    """Bar chart of directional hit rates across horizons."""
    if not HAS_MPL or not results:
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for direction_key, title_prefix in [
        ("poly_leads_kalshi_directional", "Poly -> Kalshi"),
        ("kalshi_leads_poly_directional", "Kalshi -> Poly"),
    ]:
        fig, ax = plt.subplots(figsize=(10, 6))
        bar_width = 0.8 / max(len(results), 1)

        for idx, r in enumerate(results):
            dir_data = r.get(direction_key, {})
            if not dir_data:
                continue

            horizons = sorted(dir_data.keys(), key=lambda x: int(x))
            hit_rates = []
            for h in horizons:
                hr = dir_data[h].get("hit_rate")
                hit_rates.append(hr if hr is not None else 0.5)

            x = np.arange(len(horizons))
            label = r["pair_id"][:25]
            ax.bar(x + idx * bar_width, hit_rates, bar_width, label=label, alpha=0.8)

        horizons_labels = sorted(
            results[0].get(direction_key, {}).keys(),
            key=lambda x: int(x)
        ) if results[0].get(direction_key) else []

        ax.set_xticks(np.arange(len(horizons_labels)) + bar_width * len(results) / 2)
        ax.set_xticklabels([str(h) for h in horizons_labels])
        ax.set_xlabel("Horizon (steps)")
        ax.set_ylabel("Hit Rate")
        ax.set_title(f"Directional Accuracy: {title_prefix}")
        ax.axhline(y=0.5, color="red", linestyle="--", alpha=0.5, label="Random (50%)")
        ax.legend(fontsize=7, loc="upper right")
        ax.set_ylim(0.3, 0.8)

        fig.tight_layout()
        fname = f"directional_{direction_key}.png"
        fig.savefig(os.path.join(output_dir, fname), dpi=150)
        plt.close(fig)
        print(f"  Saved {fname}")


def plot_backtest_equity_curve(
    input_csv: str = "Data/leadlag/price_series.csv",
    backtest_csv: str = "Data/leadlag/backtest_results.csv",
    output_dir: str = OUTPUT_DIR,
):
    """Plot equity curve for the best parameter combo of each pair."""
    if not HAS_MPL:
        return
    if not os.path.exists(backtest_csv) or not os.path.exists(input_csv):
        return

    # Import backtest runner for re-running with trade detail
    from src.leadlag.backtest import run_backtest_single

    bt_df = pd.read_csv(backtest_csv)
    price_df = pd.read_csv(input_csv)
    for col in ["k_mid", "p_mid"]:
        price_df[col] = pd.to_numeric(price_df[col], errors="coerce")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Find best config per pair
    best_per_pair = bt_df.sort_values("total_pnl", ascending=False).drop_duplicates("pair_id")

    fig, ax = plt.subplots(figsize=(12, 6))

    for _, best in best_per_pair.iterrows():
        pair_id = best["pair_id"]
        df_pair = price_df[price_df["pair_id"] == pair_id].sort_values("timestamp").reset_index(drop=True)
        df_pair = df_pair.dropna(subset=["k_mid", "p_mid"])

        result = run_backtest_single(
            df_pair,
            leader_col=best["leader"],
            lagger_col=best["lagger"],
            threshold=best["threshold"],
            hold_period=int(best["hold_period"]),
            slippage=best["slippage"],
        )

        if result["trades"]:
            pnls = [t["pnl"] for t in result["trades"]]
            cumulative = np.cumsum(pnls)
            label = f"{pair_id[:25]} (PnL={result['total_pnl']:.3f})"
            ax.plot(cumulative, label=label, alpha=0.8)

    ax.set_xlabel("Trade #")
    ax.set_ylabel("Cumulative PnL")
    ax.set_title("Backtest Equity Curves (Best Config per Pair)")
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "equity_curves.png"), dpi=150)
    plt.close(fig)
    print(f"  Saved equity_curves.png")


def plot_price_overlay(
    input_csv: str = "Data/leadlag/price_series.csv",
    output_dir: str = OUTPUT_DIR,
    max_pairs: int = 4,
):
    """Overlay Kalshi and Poly midprices to visually inspect lead-lag."""
    if not HAS_MPL:
        return
    if not os.path.exists(input_csv):
        return

    df = pd.read_csv(input_csv)
    for col in ["k_mid", "p_mid"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    pairs = df["pair_id"].unique()[:max_pairs]
    n_pairs = len(pairs)
    if n_pairs == 0:
        return

    fig, axes = plt.subplots(n_pairs, 1, figsize=(14, 4 * n_pairs), sharex=False)
    if n_pairs == 1:
        axes = [axes]

    for ax, pair_id in zip(axes, pairs):
        df_p = df[df["pair_id"] == pair_id].sort_values("timestamp").reset_index(drop=True)

        ax.plot(df_p.index, df_p["p_mid"], label="Polymarket", alpha=0.8, linewidth=0.8)
        ax.plot(df_p.index, df_p["k_mid"], label="Kalshi", alpha=0.8, linewidth=0.8)
        ax.set_title(pair_id[:50], fontsize=9)
        ax.set_ylabel("Midprice")
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Observation #")
    fig.suptitle("Price Overlay: Polymarket vs Kalshi", fontsize=12, y=1.01)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "price_overlay.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved price_overlay.png")


# ── Main ─────────────────────────────────────────────────────────────────────

def generate_report(
    analysis_json: str = "Data/leadlag/analysis_results.json",
    backtest_csv: str = "Data/leadlag/backtest_results.csv",
    price_csv: str = "Data/leadlag/price_series.csv",
    output_dir: str = OUTPUT_DIR,
):
    """Generate all plots."""
    print("\n=== Generating Lead-Lag Report ===\n")

    if not HAS_MPL:
        print("matplotlib not available — skipping plots")
        return

    # Load analysis results
    results = []
    if os.path.exists(analysis_json):
        with open(analysis_json, "r") as f:
            results = json.load(f)
        print(f"Loaded {len(results)} pair analyses from {analysis_json}")

    if results:
        print("\n1. Cross-correlation heatmaps:")
        plot_cross_correlation(results, output_dir)

        print("\n2. Directional accuracy charts:")
        plot_directional_accuracy(results, output_dir)

    print("\n3. Price overlay:")
    plot_price_overlay(price_csv, output_dir)

    print("\n4. Equity curves:")
    try:
        plot_backtest_equity_curve(price_csv, backtest_csv, output_dir)
    except Exception as e:
        print(f"  Could not generate equity curves: {e}")

    print(f"\nAll plots saved to {output_dir}/")


def main():
    parser = argparse.ArgumentParser(description="Lead-lag report generator")
    parser.add_argument("--analysis-json", default="Data/leadlag/analysis_results.json")
    parser.add_argument("--backtest-csv", default="Data/leadlag/backtest_results.csv")
    parser.add_argument("--price-csv", default="Data/leadlag/price_series.csv")
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    generate_report(
        analysis_json=args.analysis_json,
        backtest_csv=args.backtest_csv,
        price_csv=args.price_csv,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
