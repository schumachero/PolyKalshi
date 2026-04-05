import os
import argparse
import pandas as pd


ARBITRAGE_HISTORY_CSV = "Data/history/arbitrage_snapshots.csv"
OUTPUT_PNG = "Data/history/arbitrage_history_plot.png"


def build_arbitrage_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a stable arbitrage identifier.

    Preference order:
    1. kalshi_market + polymarket_market + strategy
    """
    df = df.copy()

    if all(col in df.columns for col in ["kalshi_market", "polymarket_market", "strategy"]):
        df["arbitrage_id"] = (
            df["kalshi_market"].astype(str)
            + "__"
            + df["polymarket_market"].astype(str)
            + "__"
            + df["strategy"].astype(str)
        )

    else:
        raise ValueError(
            "Could not build arbitrage_id."
        )

    return df


def choose_label_column(df: pd.DataFrame) -> str:
    """
    Picks a readable label for legend text.
    """
    if "kalshi_market" in df.columns and "strategy" in df.columns:
        return "label"
    return "arbitrage_id"


def add_readable_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "kalshi_market" in df.columns and "strategy" in df.columns:
        if "polymarket_market" in df.columns:
            df["label"] = (
                df["kalshi_market"].astype(str)
                + " | "
                + df["polymarket_market"].astype(str)
                + " | "
                + df["strategy"].astype(str)
            )
        else:
            df["label"] = (
                df["kalshi_market"].astype(str)
                + " | "
                + df["strategy"].astype(str)
            )
    return df


def assign_presence_segments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create segment IDs so a line breaks when an arbitrage disappears and later reappears.

    We assume one row per arbitrage_id per snapshot_time.
    A new segment begins whenever the current snapshot is not the immediately previous
    observed snapshot for that arbitrage among the global snapshot sequence.
    """
    df = df.copy()

    # Ordered global snapshot sequence
    snapshot_times = sorted(df["snapshot_time"].dropna().unique())
    time_to_index = {t: i for i, t in enumerate(snapshot_times)}
    df["snapshot_idx"] = df["snapshot_time"].map(time_to_index)

    df = df.sort_values(["arbitrage_id", "snapshot_idx"]).reset_index(drop=True)

    # If snapshot gap > 1, the arbitrage disappeared in between, so start a new segment.
    df["prev_snapshot_idx"] = df.groupby("arbitrage_id")["snapshot_idx"].shift(1)
    df["new_segment"] = (
        df["prev_snapshot_idx"].isna()
        | ((df["snapshot_idx"] - df["prev_snapshot_idx"]) > 1)
    ).astype(int)

    df["segment_id"] = df.groupby("arbitrage_id")["new_segment"].cumsum()

    return df


def filter_top_n(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Keep the top N arbitrages ranked by max profit_pct seen in history.
    """
    if top_n is None or top_n <= 0:
        return df

    ranking = (
        df.groupby("arbitrage_id", as_index=False)["profit_pct"]
        .max()
        .rename(columns={"profit_pct": "max_profit_pct"})
        .sort_values("max_profit_pct", ascending=False)
    )

    keep_ids = ranking.head(top_n)["arbitrage_id"].tolist()
    return df[df["arbitrage_id"].isin(keep_ids)].copy()


def plot_arbitrage_history(
    input_csv: str,
    output_png: str,
    top_n: int | None = 15,
    min_profit: float | None = None,
    figsize=(16, 9),
):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[plot_arbitrage_history] Warning: matplotlib not installed. Skipping plot generation.")
        return

    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Missing file: {input_csv}")

    df = pd.read_csv(input_csv)

    if df.empty:
        raise ValueError("arbitrage_snapshots.csv is empty")

    required_cols = ["snapshot_time", "profit_pct"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["snapshot_time", "profit_pct"]).copy()

    df = build_arbitrage_id(df)
    df = add_readable_labels(df)

    if min_profit is not None:
        df = df[df["profit_pct"] >= min_profit].copy()

    if df.empty:
        raise ValueError("No data left after filtering")

    df = assign_presence_segments(df)
    df = filter_top_n(df, top_n=top_n)

    label_col = choose_label_column(df)

    plt.figure(figsize=figsize)

    # Plot each arbitrage as one or more line segments.
    for arbitrage_id, g in df.groupby("arbitrage_id"):
        display_label = g[label_col].iloc[0]

        first_segment = True
        for _, seg in g.groupby("segment_id"):
            seg = seg.sort_values("snapshot_time")
            plt.plot(
                seg["snapshot_time"],
                seg["profit_pct"],
                marker="o",
                linewidth=1.8,
                markersize=3,
                label=display_label if first_segment else None,
            )
            first_segment = False

    plt.title("Arbitrage Opportunities Over Time")
    plt.xlabel("Snapshot Time")
    plt.ylabel("Profit %")
    plt.grid(True, alpha=0.3)

    if top_n is not None and top_n <= 3:
        plt.legend(loc="best", fontsize=8)

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_png), exist_ok=True)
    plt.savefig(output_png, dpi=150)
    plt.close()

    print(f"Saved plot to {output_png}")


def main():
    parser = argparse.ArgumentParser(description="Plot arbitrage history over time")
    parser.add_argument("--input", default=ARBITRAGE_HISTORY_CSV, help="Input arbitrage history CSV")
    parser.add_argument("--output", default=OUTPUT_PNG, help="Output PNG path")
    parser.add_argument("--top-n", type=int, default=15, help="Plot top N arbitrages by max profit")
    parser.add_argument("--min-profit", type=float, default=None, help="Filter rows below this profit_pct")
    args = parser.parse_args()

    plot_arbitrage_history(
        input_csv=args.input,
        output_png=args.output,
        top_n=args.top_n,
        min_profit=args.min_profit,
    )


if __name__ == "__main__":
    main()