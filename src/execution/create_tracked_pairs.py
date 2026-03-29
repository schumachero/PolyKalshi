import os
import re
import argparse
import pandas as pd

PORTFOLIO_CSV = "Data/portfolio.csv"
OUTPUT_CSV = "Data/tracked_pairs.csv"

DEFAULT_MAX_POSITION_USD = 100.0
DEFAULT_MIN_PROFIT_PCT = 1.0
DEFAULT_MIN_LIQUIDITY_USD = 50.0
DEFAULT_COOLDOWN_MINUTES = 30


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def slugify(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def build_pair_id(kalshi_ticker: str, polymarket_ticker: str) -> str:
    return f"{slugify(kalshi_ticker)}__{slugify(polymarket_ticker)}"


def normalize_platform(value: str) -> str:
    v = str(value).strip().lower()
    if "kalshi" in v:
        return "kalshi"
    if "poly" in v:
        return "polymarket"
    return v


def create_tracked_pairs_from_portfolio(
    portfolio_csv: str,
    output_csv: str,
    require_both_legs: bool = True,
    positive_quantity_only: bool = True,
) -> None:
    if not os.path.exists(portfolio_csv):
        raise FileNotFoundError(f"Portfolio file not found: {portfolio_csv}")

    df = pd.read_csv(portfolio_csv)
    print(f"Loaded {len(df)} portfolio rows from {portfolio_csv}")

    required_cols = [
        "Platform", "Ticker", "Title", "Side", "Quantity",
        "Closing_Time", "Matched_Ticker", "Match_Score"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in portfolio CSV: {missing}")

    df = df.copy()
    df["Platform_norm"] = df["Platform"].apply(normalize_platform)
    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    df["Matched_Ticker"] = df["Matched_Ticker"].astype(str).str.strip()
    df["Title"] = df["Title"].astype(str).fillna("").str.strip()
    df["Side"] = df["Side"].astype(str).fillna("").str.strip()
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["Match_Score"] = pd.to_numeric(df["Match_Score"], errors="coerce")

    if positive_quantity_only:
        before = len(df)
        df = df[df["Quantity"] > 0].copy()
        print(f"Kept {len(df)} rows with positive quantity (removed {before - len(df)})")

    kalshi_df = df[df["Platform_norm"] == "kalshi"].copy()
    poly_df = df[df["Platform_norm"] == "polymarket"].copy()

    print(f"Kalshi rows: {len(kalshi_df)}")
    print(f"Polymarket rows: {len(poly_df)}")

    poly_lookup = {row["Ticker"]: row for _, row in poly_df.iterrows()}
    kalshi_lookup = {row["Ticker"]: row for _, row in kalshi_df.iterrows()}

    tracked_rows = []
    used_pairs = set()

    # First pass: iterate through Kalshi holdings and find corresponding Poly holdings
    for _, krow in kalshi_df.iterrows():
        kalshi_ticker = krow["Ticker"]
        poly_ticker = krow["Matched_Ticker"]

        if not poly_ticker:
            continue

        prow = poly_lookup.get(poly_ticker)

        if prow is None and require_both_legs:
            continue

        pair_key = tuple(sorted([kalshi_ticker, poly_ticker]))
        if pair_key in used_pairs:
            continue

        tracked_rows.append({
            "pair_id": build_pair_id(kalshi_ticker, poly_ticker),
            "active": True,
            "kalshi_ticker": kalshi_ticker,
            "kalshi_title": krow["Title"],
            "kalshi_side_held": krow["Side"],
            "kalshi_quantity": float(krow["Quantity"]),
            "polymarket_ticker": poly_ticker,
            "polymarket_title": prow["Title"] if prow is not None else "",
            "polymarket_side_held": prow["Side"] if prow is not None else "",
            "polymarket_quantity": float(prow["Quantity"]) if prow is not None else 0.0,
            "close_time": krow["Closing_Time"] if pd.notna(krow["Closing_Time"]) else (
                prow["Closing_Time"] if prow is not None else ""
            ),
            "match_score": krow["Match_Score"],
            "max_position_per_pair_usd": DEFAULT_MAX_POSITION_USD,
            "min_profit_pct": DEFAULT_MIN_PROFIT_PCT,
            "min_liquidity_usd": DEFAULT_MIN_LIQUIDITY_USD,
            "cooldown_minutes": DEFAULT_COOLDOWN_MINUTES,
            "notes": "created from current portfolio holdings",
        })
        used_pairs.add(pair_key)

    # Optional second pass: catch poly rows whose paired Kalshi row exists but wasn't caught above
    for _, prow in poly_df.iterrows():
        poly_ticker = prow["Ticker"]
        kalshi_ticker = prow["Matched_Ticker"]

        if not kalshi_ticker:
            continue

        krow = kalshi_lookup.get(kalshi_ticker)

        if krow is None and require_both_legs:
            continue

        pair_key = tuple(sorted([kalshi_ticker, poly_ticker]))
        if pair_key in used_pairs:
            continue

        tracked_rows.append({
            "pair_id": build_pair_id(kalshi_ticker, poly_ticker),
            "active": True,
            "kalshi_ticker": kalshi_ticker,
            "kalshi_title": krow["Title"] if krow is not None else "",
            "kalshi_side_held": krow["Side"] if krow is not None else "",
            "kalshi_quantity": float(krow["Quantity"]) if krow is not None else 0.0,
            "polymarket_ticker": poly_ticker,
            "polymarket_title": prow["Title"],
            "polymarket_side_held": prow["Side"],
            "polymarket_quantity": float(prow["Quantity"]),
            "close_time": prow["Closing_Time"] if pd.notna(prow["Closing_Time"]) else (
                krow["Closing_Time"] if krow is not None else ""
            ),
            "match_score": prow["Match_Score"],
            "max_position_per_pair_usd": DEFAULT_MAX_POSITION_USD,
            "min_profit_pct": DEFAULT_MIN_PROFIT_PCT,
            "min_liquidity_usd": DEFAULT_MIN_LIQUIDITY_USD,
            "cooldown_minutes": DEFAULT_COOLDOWN_MINUTES,
            "notes": "created from current portfolio holdings",
        })
        used_pairs.add(pair_key)

    out = pd.DataFrame(tracked_rows)

    if out.empty:
        print("No tracked pairs were created.")
        print("Check that:")
        print("- portfolio rows have positive Quantity")
        print("- Matched_Ticker is populated")
        print("- both legs exist in the portfolio if require_both_legs=True")
        return

    out = out.drop_duplicates(subset=["pair_id"]).copy()

    desired_order = [
        "pair_id",
        "active",
        "kalshi_ticker",
        "kalshi_title",
        "kalshi_side_held",
        "kalshi_quantity",
        "polymarket_ticker",
        "polymarket_title",
        "polymarket_side_held",
        "polymarket_quantity",
        "close_time",
        "match_score",
        "max_position_per_pair_usd",
        "min_profit_pct",
        "min_liquidity_usd",
        "cooldown_minutes",
        "notes",
    ]
    out = out[desired_order]

    ensure_parent_dir(output_csv)
    out.to_csv(output_csv, index=False)

    print(f"Saved {len(out)} tracked pairs to {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Create tracked_pairs.csv from current portfolio holdings")
    parser.add_argument("--portfolio", default=PORTFOLIO_CSV, help="Input portfolio CSV")
    parser.add_argument("--output", default=OUTPUT_CSV, help="Output tracked pairs CSV")
    parser.add_argument(
        "--allow-single-leg",
        action="store_true",
        help="Allow tracked pairs even if only one side exists in portfolio",
    )
    args = parser.parse_args()

    create_tracked_pairs_from_portfolio(
        portfolio_csv=args.portfolio,
        output_csv=args.output,
        require_both_legs=not args.allow_single_leg,
        positive_quantity_only=True,
    )


if __name__ == "__main__":
    main()