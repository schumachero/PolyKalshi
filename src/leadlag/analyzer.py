"""
Lead-Lag Analyzer
=================
Statistical analysis of collected price series to detect lead-lag relationships.

Usage:
    python -m src.leadlag.analyzer --input Data/leadlag/price_series.csv
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_corr(a: np.ndarray, b: np.ndarray) -> float | None:
    """Pearson correlation, returns None if inputs are constant or too short."""
    mask = np.isfinite(a) & np.isfinite(b)
    a, b = a[mask], b[mask]
    if len(a) < 5:
        return None
    if np.std(a) == 0 or np.std(b) == 0:
        return None
    return float(np.corrcoef(a, b)[0, 1])


# ── Core Analysis ────────────────────────────────────────────────────────────

def compute_returns(series: pd.Series, lookback: int = 1) -> pd.Series:
    """Returns measured over `lookback` steps."""
    return series.diff(periods=lookback)


def cross_correlation_at_lags(
    leader_returns: pd.Series,
    lagger_returns: pd.Series,
    lags: list[int],
) -> dict[int, float | None]:
    """
    Compute corr(leader(t), lagger(t + lag)) for each lag.
    Positive correlation at positive lag means leader predicts lagger.
    """
    results = {}
    leader = leader_returns.values
    lagger = lagger_returns.values

    for lag in lags:
        if lag >= 0:
            a = leader[:len(leader) - lag] if lag > 0 else leader
            b = lagger[lag:]
        else:
            a = leader[-lag:]
            b = lagger[:len(lagger) + lag]
        results[int(lag)] = _safe_corr(a, b)
    return results


def directional_accuracy(
    leader_returns: pd.Series,
    lagger_returns: pd.Series,
    horizons: list[int],
    min_move: float = 0.0,
) -> dict[int, dict]:
    """
    For each horizon N, compute:
     - P(lagger goes same direction as leader within next N steps)
     - total signals, hits, hit_rate

    min_move: ignore leader moves smaller than this (noise filter).
    """
    leader = leader_returns.values
    lagger = lagger_returns.values
    results = {}

    for horizon in horizons:
        signals = 0
        hits = 0

        for t in range(len(leader) - horizon):
            if not np.isfinite(leader[t]) or abs(leader[t]) <= min_move:
                continue
            leader_dir = np.sign(leader[t])

            # Check if lagger moves in the same direction in the next N steps
            future_lagger = lagger[t + 1: t + 1 + horizon]
            future_lagger = future_lagger[np.isfinite(future_lagger)]

            if len(future_lagger) == 0:
                continue

            # Use cumulative move over the horizon
            cum_move = np.sum(future_lagger)
            if cum_move == 0:
                continue

            signals += 1
            if np.sign(cum_move) == leader_dir:
                hits += 1

        hit_rate = hits / signals if signals > 0 else None
        results[int(horizon)] = {
            "signals": int(signals),
            "hits": int(hits),
            "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        }
    return results


def granger_causality_test(
    leader_returns: pd.Series,
    lagger_returns: pd.Series,
    max_lag: int = 5,
) -> dict | None:
    """
    Granger causality test: does leader Granger-cause lagger?
    Returns dict with p-values per lag, or None if statsmodels unavailable.
    """
    try:
        from statsmodels.tsa.stattools import grangercausalitytests
    except ImportError:
        return None

    # Build a DataFrame with lagger first (Y), leader second (X)
    df = pd.DataFrame({
        "lagger": lagger_returns,
        "leader": leader_returns,
    }).dropna()

    if len(df) < max_lag * 3:
        return {"error": f"Not enough data ({len(df)} rows) for max_lag={max_lag}"}

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            results = grangercausalitytests(df[["lagger", "leader"]], maxlag=max_lag, verbose=False)

        summary = {}
        for lag_order, result in results.items():
            test_stats = result[0]
            # Use the ssr_ftest p-value
            p_value = test_stats["ssr_ftest"][1]
            summary[lag_order] = {
                "p_value": round(float(p_value), 6),
                "significant_5pct": p_value < 0.05,
                "significant_1pct": p_value < 0.01,
            }
        return summary
    except Exception as e:
        return {"error": str(e)}


# ── Per-pair Analysis ────────────────────────────────────────────────────────

def analyze_pair(
    df_pair: pd.DataFrame,
    lags: list[int] | None = None,
    horizons: list[int] | None = None,
    min_move: float = 0.0,
    lookback: int = 1,
) -> dict:
    """Run full analysis for one pair. Returns structured results dict."""
    if lags is None:
        lags = [1, 2, 3, 5, 10, 20, 60, 120, 360, 720]
    if horizons is None:
        horizons = [1, 2, 3, 5, 10, 20, 60, 120, 360, 720]

    pair_id = df_pair["pair_id"].iloc[0]
    n_obs = len(df_pair)

    # Compute returns
    k_ret = compute_returns(df_pair["k_mid"], lookback=lookback)
    p_ret = compute_returns(df_pair["p_mid"], lookback=lookback)

    result = {
        "pair_id": pair_id,
        "kalshi_ticker": df_pair["kalshi_ticker"].iloc[0],
        "poly_ticker": df_pair["poly_ticker"].iloc[0],
        "n_observations": n_obs,
        "k_mid_mean": round(float(df_pair["k_mid"].mean()), 4) if not df_pair["k_mid"].isna().all() else None,
        "p_mid_mean": round(float(df_pair["p_mid"].mean()), 4) if not df_pair["p_mid"].isna().all() else None,
    }

    # --- Cross-correlation: Poly leads Kalshi ---
    result["poly_leads_kalshi_corr"] = cross_correlation_at_lags(p_ret, k_ret, lags)

    # --- Cross-correlation: Kalshi leads Poly ---
    result["kalshi_leads_poly_corr"] = cross_correlation_at_lags(k_ret, p_ret, lags)

    # --- Directional accuracy: Poly leads Kalshi ---
    result["poly_leads_kalshi_directional"] = directional_accuracy(
        p_ret, k_ret, horizons, min_move=min_move
    )

    # --- Directional accuracy: Kalshi leads Poly ---
    result["kalshi_leads_poly_directional"] = directional_accuracy(
        k_ret, p_ret, horizons, min_move=min_move
    )

    # --- Granger causality ---
    result["granger_poly_causes_kalshi"] = granger_causality_test(p_ret, k_ret)
    result["granger_kalshi_causes_poly"] = granger_causality_test(k_ret, p_ret)

    # --- Summary: which platform leads? ---
    poly_leads_corrs = [v for v in result["poly_leads_kalshi_corr"].values() if v is not None]
    kalshi_leads_corrs = [v for v in result["kalshi_leads_poly_corr"].values() if v is not None]

    avg_poly_leads = np.mean(poly_leads_corrs) if poly_leads_corrs else 0
    avg_kalshi_leads = np.mean(kalshi_leads_corrs) if kalshi_leads_corrs else 0

    if avg_poly_leads > avg_kalshi_leads + 0.02:
        result["likely_leader"] = "polymarket"
    elif avg_kalshi_leads > avg_poly_leads + 0.02:
        result["likely_leader"] = "kalshi"
    else:
        result["likely_leader"] = "unclear"

    result["avg_poly_leads_corr"] = round(avg_poly_leads, 4)
    result["avg_kalshi_leads_corr"] = round(avg_kalshi_leads, 4)

    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def run_analyzer(
    input_csv: str = "Data/leadlag/price_series.csv",
    output_json: str = "Data/leadlag/analysis_results.json",
    min_move: float = 0.0,
    lookback: int = 1,
) -> list[dict]:
    """Analyze all pairs in the price series CSV."""
    if not os.path.exists(input_csv):
        print(f"Input file not found: {input_csv}")
        return []

    df = pd.read_csv(input_csv)

    # Convert numeric columns
    for col in ["k_mid", "p_mid", "k_best_bid", "k_best_ask", "p_best_bid", "p_best_ask"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"Loaded {len(df)} rows, {df['pair_id'].nunique()} pairs")
    print(f"Time range: {df['timestamp'].min()} -> {df['timestamp'].max()}")

    all_results = []
    for pair_id, df_pair in df.groupby("pair_id"):
        df_pair = df_pair.sort_values("timestamp").reset_index(drop=True)

        # Skip pairs with insufficient data
        valid = df_pair.dropna(subset=["k_mid", "p_mid"])
        if len(valid) < 20:
            print(f"  Skipping {pair_id}: only {len(valid)} valid observations")
            continue

        print(f"\n{'='*60}")
        print(f"Pair: {pair_id}")
        print(f"  Observations: {len(valid)}")
        result = analyze_pair(df_pair, min_move=min_move, lookback=lookback)
        all_results.append(result)

        # --- Pretty-print summary ---
        print(f"  Likely leader: {result['likely_leader']}")
        print(f"  Avg cross-corr (Poly->Kalshi): {result['avg_poly_leads_corr']:.4f}")
        print(f"  Avg cross-corr (Kalshi->Poly): {result['avg_kalshi_leads_corr']:.4f}")

        print(f"\n  Cross-correlation (Poly leads -> Kalshi follows):")
        for lag, corr in result["poly_leads_kalshi_corr"].items():
            corr_str = f"{corr:.4f}" if corr is not None else "N/A"
            print(f"    lag={lag:>3}: {corr_str}")

        print(f"\n  Cross-correlation (Kalshi leads -> Poly follows):")
        for lag, corr in result["kalshi_leads_poly_corr"].items():
            corr_str = f"{corr:.4f}" if corr is not None else "N/A"
            print(f"    lag={lag:>3}: {corr_str}")

        print(f"\n  Directional accuracy (Poly->Kalshi):")
        for h, stats in result["poly_leads_kalshi_directional"].items():
            hr = f"{stats['hit_rate']:.1%}" if stats["hit_rate"] is not None else "N/A"
            print(f"    horizon={h:>3}: {hr} ({stats['hits']}/{stats['signals']} signals)")

        print(f"\n  Directional accuracy (Kalshi->Poly):")
        for h, stats in result["kalshi_leads_poly_directional"].items():
            hr = f"{stats['hit_rate']:.1%}" if stats["hit_rate"] is not None else "N/A"
            print(f"    horizon={h:>3}: {hr} ({stats['hits']}/{stats['signals']} signals)")

        # Granger
        for direction, key in [("Poly->Kalshi", "granger_poly_causes_kalshi"),
                               ("Kalshi->Poly", "granger_kalshi_causes_poly")]:
            gc = result[key]
            if gc and "error" not in gc:
                print(f"\n  Granger causality ({direction}):")
                for lag_order, stats in gc.items():
                    sig = "***" if stats["significant_1pct"] else ("**" if stats["significant_5pct"] else "")
                    print(f"    lag={lag_order}: p={stats['p_value']:.4f} {sig}")
            elif gc and "error" in gc:
                print(f"\n  Granger causality ({direction}): {gc['error']}")

    # Save results — convert numpy types to native Python for JSON
    def _sanitize(obj):
        """Recursively convert numpy types to native Python types."""
        if isinstance(obj, dict):
            return {str(k): _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        return obj

    Path(os.path.dirname(output_json)).mkdir(parents=True, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(_sanitize(all_results), f, indent=2, default=str)
    print(f"\n\nResults saved to {output_json}")

    return all_results


def main():
    parser = argparse.ArgumentParser(description="Lead-lag analyzer")
    parser.add_argument("--input", type=str, default="Data/leadlag/price_series.csv")
    parser.add_argument("--output", type=str, default="Data/leadlag/analysis_results.json")
    parser.add_argument("--min-move", type=float, default=0.0,
                        help="Minimum leader price move to count as a signal (0-1 scale)")
    parser.add_argument("--lookback", type=int, default=1,
                        help="How many steps to measure the price move over (default: 1)")
    args = parser.parse_args()

    run_analyzer(
        input_csv=args.input, 
        output_json=args.output, 
        min_move=args.min_move,
        lookback=args.lookback
    )


if __name__ == "__main__":
    main()
