import os
import sys
import time
import math
import argparse
from datetime import datetime, timezone

import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(SRC_DIR)

for path in [SRC_DIR, PROJECT_ROOT]:
    if path not in sys.path:
        sys.path.insert(0, path)

from apis.orderbook import (
    get_kalshi_orderbook,
    get_polymarket_orderbook,
)

from execution.kalshi_trade import place_limit_order as kalshi_place_limit_order
from execution.polymarket_trade import place_limit_order as polymarket_place_limit_order

from apis.orderbook import (
    get_kalshi_orderbook,      
    get_polymarket_orderbook,  
)



# =========================
# Configuration
# =========================

TRACKED_PAIRS_CSV = "Data/tracked_pairs.csv"
EXECUTION_LOG_CSV = "Data/portfolio_arb_execution_log.csv"

MIN_PROFIT_PCT = 5.0          # minimum gross arb %
MIN_LIQUIDITY_USD = 5.0      # minimum executable notional
DEFAULT_MAX_TRADE_USD = 20.0
SLEEP_MINUTES = 30

KALSHI_FEE_BUFFER = 0.0       # set if you want a fee cushion
POLY_FEE_BUFFER = 0.0         # set if you want a fee cushion


# =========================
# Helpers
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def append_execution_log(row: dict) -> None:
    ensure_parent_dir(EXECUTION_LOG_CSV)
    df = pd.DataFrame([row])
    write_header = not os.path.exists(EXECUTION_LOG_CSV)
    df.to_csv(EXECUTION_LOG_CSV, mode="a", header=write_header, index=False)


def safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def normalize_book_side(levels):
    out = []
    if not levels:
        return out

    for lvl in levels:
        if isinstance(lvl, dict):
            price = safe_float(lvl.get("price"))
            size = safe_float(lvl.get("size", lvl.get("volume", lvl.get("quantity", 0))))
        else:
            price = safe_float(lvl[0])
            size = safe_float(lvl[1])

        if price > 0 and size > 0:
            out.append({"price": price, "size": size})

    return out


def get_yes_no_books_kalshi(ticker: str) -> dict:
    raw = get_kalshi_orderbook(ticker)

    yes_asks = normalize_book_side(raw.get("yes", {}).get("asks", []))
    no_asks = normalize_book_side(raw.get("no", {}).get("asks", []))

    return {
        "yes_asks": yes_asks,
        "no_asks": no_asks,
    }


def get_outcome_books_polymarket(slug: str, outcome_name: str) -> dict:
    raw = get_polymarket_orderbook(slug)

    outcome_key = str(outcome_name).strip().lower()  # YES -> yes, NO -> no
    buy_levels = normalize_book_side(raw.get(outcome_key, {}).get("asks", []))

    return {"buy_levels": buy_levels}


def walk_books_for_arb(levels_a, levels_b, max_trade_usd):
    """
    levels_a and levels_b are lists sorted best ask first.
    Computes executable arb where we buy both legs.

    Returns the best executable trade found:
    {
        "found": bool,
        "price_a": float,
        "price_b": float,
        "sum_price": float,
        "profit_pct": float,
        "contracts": float,
        "notional_usd": float
    }
    """
    i = 0
    j = 0

    remaining_cap_contracts = float("inf")
    best = None

    levels_a = normalize_book_side(levels_a)
    levels_b = normalize_book_side(levels_b)

    if not levels_a or not levels_b:
        return {"found": False}

    # copy mutable sizes
    a = [{"price": x["price"], "size": x["size"]} for x in levels_a]
    b = [{"price": x["price"], "size": x["size"]} for x in levels_b]

    while i < len(a) and j < len(b):
        pa = a[i]["price"]
        pb = b[j]["price"]

        sum_price = pa + pb + KALSHI_FEE_BUFFER + POLY_FEE_BUFFER
        profit_pct = (1.0 - sum_price) * 100.0

        if sum_price >= 1.0:
            break

        max_affordable_contracts = max_trade_usd / max(sum_price, 1e-9)
        available_contracts = min(a[i]["size"], b[j]["size"], max_affordable_contracts)

        if available_contracts <= 0:
            break

        notional = available_contracts * sum_price

        candidate = {
            "found": True,
            "price_a": pa,
            "price_b": pb,
            "sum_price": sum_price,
            "profit_pct": profit_pct,
            "contracts": available_contracts,
            "notional_usd": notional,
            "depth_a": i,
            "depth_b": j,
        }

        if best is None or candidate["profit_pct"] > best["profit_pct"]:
            best = candidate

        # consume depth
        a[i]["size"] -= available_contracts
        b[j]["size"] -= available_contracts

        if a[i]["size"] <= 1e-9:
            i += 1
        if b[j]["size"] <= 1e-9:
            j += 1

        # if we already hit cap, stop
        if notional >= max_trade_usd - 1e-9:
            break

    return best if best else {"found": False}


def choose_best_arb_for_pair(pair_row: pd.Series) -> dict:
    """
    Checks both:
      1. Kalshi YES + Polymarket NO
      2. Kalshi NO  + Polymarket YES
    """
    kalshi_ticker = pair_row["kalshi_ticker"]
    polymarket_slug = pair_row["polymarket_ticker"]

    max_trade_usd = safe_float(
        pair_row.get("max_position_per_pair_usd", DEFAULT_MAX_TRADE_USD),
        DEFAULT_MAX_TRADE_USD,
    )

    kalshi_books = get_yes_no_books_kalshi(kalshi_ticker)

    poly_no = get_outcome_books_polymarket(polymarket_slug, "NO")
    poly_yes = get_outcome_books_polymarket(polymarket_slug, "YES")

    candidate_yes_no = walk_books_for_arb(
        kalshi_books["yes_asks"],
        poly_no["buy_levels"],
        max_trade_usd=max_trade_usd,
    )

    candidate_no_yes = walk_books_for_arb(
        kalshi_books["no_asks"],
        poly_yes["buy_levels"],
        max_trade_usd=max_trade_usd,
    )

    options = []
    if candidate_yes_no.get("found"):
        candidate_yes_no["kalshi_side"] = "yes"
        candidate_yes_no["polymarket_outcome"] = "NO"
        options.append(candidate_yes_no)

    if candidate_no_yes.get("found"):
        candidate_no_yes["kalshi_side"] = "no"
        candidate_no_yes["polymarket_outcome"] = "YES"
        options.append(candidate_no_yes)

    if not options:
        return {"found": False}

    best = max(options, key=lambda x: x["profit_pct"])
    print(f"\nPAIR DEBUG: {pair_row.get('pair_id')}")
    print("Kalshi YES asks:", kalshi_books["yes_asks"][:3])
    print("Kalshi NO asks:", kalshi_books["no_asks"][:3])
    print("Poly NO asks:", poly_no["buy_levels"][:3])
    print("Poly YES asks:", poly_yes["buy_levels"][:3])
    return best


def place_dual_orders(pair_row: pd.Series, arb: dict) -> dict:
    """
    Places both legs.
    """
    kalshi_ticker = pair_row["kalshi_ticker"]
    polymarket_slug = pair_row["polymarket_ticker"]

    contracts = int(math.floor(arb["contracts"]))
    if contracts <= 0:
        return {"status": "skipped", "message": "contracts rounded to 0"}

    kalshi_price_cents = int(round(arb["price_a"] * 100))
    polymarket_price = round(arb["price_b"], 6)

    # Place Kalshi first
    kalshi_resp = kalshi_place_limit_order(
        ticker=kalshi_ticker,
        side=arb["kalshi_side"],      # "yes" or "no"
        action="buy",
        count=contracts,
        price_cents=kalshi_price_cents,
        time_in_force="fill_or_kill",
    )

    # Place Polymarket second
    poly_resp = polymarket_place_limit_order(
        slug=polymarket_slug,
        outcome=arb["polymarket_outcome"],  # "YES" or "NO"
        size=contracts,
        price=polymarket_price,
        side="BUY",
        order_type="FOK",
    )

    return {
        "status": "success",
        "kalshi_response": kalshi_resp,
        "polymarket_response": poly_resp,
        "contracts": contracts,
        "kalshi_price": arb["price_a"],
        "polymarket_price": arb["price_b"],
        "sum_price": arb["sum_price"],
        "profit_pct": arb["profit_pct"],
        "notional_usd": contracts * arb["sum_price"],
    }


def process_pair(pair_row: pd.Series, dry_run: bool = True) -> None:
    pair_id = pair_row.get("pair_id", f"{pair_row['kalshi_ticker']}__{pair_row['polymarket_ticker']}")
    active = str(pair_row.get("active", "true")).strip().lower() in {"1", "true", "yes", "y"}

    if not active:
        return

    try:
        arb = choose_best_arb_for_pair(pair_row)

        if not arb.get("found"):
            print(f"[{pair_id}] no arb found")
            return

        if arb["profit_pct"] < MIN_PROFIT_PCT:
            print(f"[{pair_id}] arb found but below min profit: {arb['profit_pct']:.3f}%")
            return

        if arb["notional_usd"] < MIN_LIQUIDITY_USD:
            print(f"[{pair_id}] arb found but insufficient liquidity: ${arb['notional_usd']:.2f}")
            return

        print(
            f"[{pair_id}] arb found | "
            f"Kalshi {arb['kalshi_side'].upper()} @ {arb['price_a']:.4f} + "
            f"Poly {arb['polymarket_outcome']} @ {arb['price_b']:.4f} = "
            f"{arb['sum_price']:.4f} | profit {arb['profit_pct']:.3f}% | "
            f"contracts {arb['contracts']:.2f}"
        )

        if dry_run:
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "dry_run_candidate",
                "kalshi_ticker": pair_row["kalshi_ticker"],
                "polymarket_ticker": pair_row["polymarket_ticker"],
                "kalshi_side": arb["kalshi_side"],
                "polymarket_outcome": arb["polymarket_outcome"],
                "kalshi_price": arb["price_a"],
                "polymarket_price": arb["price_b"],
                "sum_price": arb["sum_price"],
                "profit_pct": arb["profit_pct"],
                "contracts": arb["contracts"],
                "notional_usd": arb["notional_usd"],
                "message": "Candidate found; dry run only",
            })
            return

        result = place_dual_orders(pair_row, arb)

        append_execution_log({
            "timestamp": utc_now_iso(),
            "pair_id": pair_id,
            "status": result["status"],
            "kalshi_ticker": pair_row["kalshi_ticker"],
            "polymarket_ticker": pair_row["polymarket_ticker"],
            "kalshi_side": arb["kalshi_side"],
            "polymarket_outcome": arb["polymarket_outcome"],
            "kalshi_price": result.get("kalshi_price"),
            "polymarket_price": result.get("polymarket_price"),
            "sum_price": result.get("sum_price"),
            "profit_pct": result.get("profit_pct"),
            "contracts": result.get("contracts"),
            "notional_usd": result.get("notional_usd"),
            "message": "",
        })

    except Exception as e:
        print(f"[{pair_id}] ERROR: {e}")
        append_execution_log({
            "timestamp": utc_now_iso(),
            "pair_id": pair_id,
            "status": "error",
            "kalshi_ticker": pair_row.get("kalshi_ticker", ""),
            "polymarket_ticker": pair_row.get("polymarket_ticker", ""),
            "message": str(e),
        })


def run_once(tracked_pairs_csv: str, dry_run: bool = True) -> None:
    if not os.path.exists(tracked_pairs_csv):
        raise FileNotFoundError(f"{tracked_pairs_csv} not found")

    df = pd.read_csv(tracked_pairs_csv)
    print(f"Loaded {len(df)} tracked pairs from {tracked_pairs_csv}")

    required_cols = ["kalshi_ticker", "polymarket_ticker"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for _, row in df.iterrows():
        process_pair(row, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Portfolio arbitrage monitor/executor")
    parser.add_argument("--input", default=TRACKED_PAIRS_CSV, help="Tracked pairs CSV")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval-minutes", type=int, default=SLEEP_MINUTES, help="Minutes between refreshes")
    parser.add_argument("--live", action="store_true", help="Actually place orders")
    args = parser.parse_args()

    dry_run = not args.live

    if not args.loop:
        run_once(args.input, dry_run=dry_run)
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1

    while True:
        cycle_start = time.time()
        print(f"\n########## PORTFOLIO ARB ITERATION {iteration} ##########")

        try:
            run_once(args.input, dry_run=dry_run)
        except Exception as e:
            print(f"Fatal iteration error: {e}")

        elapsed = time.time() - cycle_start
        sleep_seconds = max(interval_seconds - elapsed, 0)

        print(f"Iteration {iteration} finished in {elapsed:.1f}s")
        print(f"Sleeping for {sleep_seconds / 60:.2f} minutes... Press Ctrl+C to stop.")

        try:
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break

        iteration += 1


if __name__ == "__main__":
    main()