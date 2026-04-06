import os
import sys
import time
import math
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional
try:
    from apis.portfolio import get_kalshi_balance, get_polymarket_balance
except ImportError:
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from apis.portfolio import get_kalshi_balance, get_polymarket_balance
import pandas as pd

# =========================================================
# Path setup
# =========================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(SRC_DIR)

for path in [CURRENT_DIR, SRC_DIR, PROJECT_ROOT]:
    if path not in sys.path:
        sys.path.insert(0, path)

# =========================================================
# Project imports
# =========================================================

from apis.orderbook import (
    get_kalshi_orderbook,
    get_polymarket_orderbook,
)

from execution.kalshi_trade import place_limit_order as kalshi_place_limit_order
from execution.polymarket_trade import place_limit_order as polymarket_place_limit_order

# =========================================================
# Configuration
# =========================================================

TRACKED_PAIRS_CSV = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs.csv")
EXECUTION_LOG_CSV = os.path.join(PROJECT_ROOT, "Data", "portfolio_arb_execution_log.csv")

DEFAULT_MAX_TRADE_USD = 20.0
DEFAULT_MIN_PROFIT_PCT = 8.0
DEFAULT_MIN_LIQUIDITY_USD = 5.0
DEFAULT_SLEEP_MINUTES = 30

# Fee cushions can be used as conservative safety margins
KALSHI_FEE_BUFFER = 0.0
POLY_FEE_BUFFER = 0.0

# Reverify against live books again immediately before sending orders
DEFAULT_REVERIFY_BOOKS = True

# =========================================================
# Helpers
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def top_of_book_arb(levels_a, levels_b, max_trade_usd, fee_buffer_a=0.0, fee_buffer_b=0.0):
    """
    Only use depth 1 from each book.

    Returns:
    {
        "found": bool,
        "contracts": float,
        "price_a": float,
        "price_b": float,
        "sum_price": float,
        "profit_pct": float,
        "notional_usd": float,
        "gross_profit_usd": float,
        "depth_a_used": 0,
        "depth_b_used": 0,
    }
    """
    a = normalize_book_side(levels_a)
    b = normalize_book_side(levels_b)

    if not a or not b:
        return {"found": False}

    pa = a[0]["price"]
    pb = b[0]["price"]
    sa = a[0]["size"]
    sb = b[0]["size"]

    sum_price = pa + pb + fee_buffer_a + fee_buffer_b
    if sum_price >= 1.0:
        return {"found": False}

    max_affordable_contracts = max_trade_usd / max(sum_price, 1e-12)
    contracts = min(sa, sb, max_affordable_contracts)

    if contracts <= 1e-12:
        return {"found": False}

    notional_usd = contracts * sum_price
    gross_profit_usd = contracts * (1.0 - sum_price)
    profit_pct = (1.0 - sum_price) * 100.0

    return {
        "found": True,
        "contracts": contracts,
        "price_a": pa,
        "price_b": pb,
        "sum_price": sum_price,
        "profit_pct": profit_pct,
        "notional_usd": notional_usd,
        "gross_profit_usd": gross_profit_usd,
        "depth_a_used": 0,
        "depth_b_used": 0,
    }
def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


def append_execution_log(row: dict) -> None:
    ensure_parent_dir(EXECUTION_LOG_CSV)
    df = pd.DataFrame([row])
    write_header = not os.path.exists(EXECUTION_LOG_CSV)
    df.to_csv(EXECUTION_LOG_CSV, mode="a", header=write_header, index=False)


def safe_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def truthy(x) -> bool:
    return normalize_str(x).lower() in {"1", "true", "yes", "y", "on"}

BALANCE_BUFFER_USD = 1.00

def get_kalshi_available_usd() -> float:
    bal = get_kalshi_balance()
    if not bal:
        return 0.0
    return safe_float(bal.get("available_cents", 0)) / 100.0


def get_polymarket_available_usd() -> float:
    wallet = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not wallet:
        return 0.0
    return safe_float(get_polymarket_balance(wallet), 0.0)


def size_trade_to_available_balances(arb: dict) -> dict:
    """
    Shrink contracts so both legs are affordable.
    Assumes both legs are BUY orders.
    """
    original_contracts = int(math.floor(arb["contracts"]))
    if original_contracts <= 0:
        return {
            "ok": False,
            "reason": "contracts rounded to 0",
        }

    kalshi_available_usd = get_kalshi_available_usd()
    polymarket_available_usd = get_polymarket_available_usd()

    kalshi_spendable = max(kalshi_available_usd - BALANCE_BUFFER_USD, 0.0)
    poly_spendable = max(polymarket_available_usd - BALANCE_BUFFER_USD, 0.0)

    max_contracts_kalshi = int(math.floor(kalshi_spendable / max(arb["price_a"], 1e-12)))
    max_contracts_poly = int(math.floor(poly_spendable / max(arb["price_b"], 1e-12)))

    final_contracts = min(
        original_contracts,
        max_contracts_kalshi,
        max_contracts_poly,
    )

    if final_contracts <= 0:
        return {
            "ok": False,
            "reason": (
                f"Insufficient balance | "
                f"Kalshi available=${kalshi_available_usd:.2f}, "
                f"Polymarket available=${polymarket_available_usd:.2f}"
            ),
            "kalshi_available_usd": kalshi_available_usd,
            "polymarket_available_usd": polymarket_available_usd,
        }

    kalshi_required_usd = final_contracts * arb["price_a"]
    polymarket_required_usd = final_contracts * arb["price_b"]

    sized_arb = dict(arb)
    sized_arb["contracts"] = final_contracts
    sized_arb["notional_usd"] = final_contracts * arb["sum_price"]
    sized_arb["gross_profit_usd"] = final_contracts * (1.0 - arb["sum_price"])

    return {
        "ok": True,
        "arb": sized_arb,
        "kalshi_available_usd": kalshi_available_usd,
        "polymarket_available_usd": polymarket_available_usd,
        "kalshi_required_usd": kalshi_required_usd,
        "polymarket_required_usd": polymarket_required_usd,
        "max_contracts_kalshi": max_contracts_kalshi,
        "max_contracts_poly": max_contracts_poly,
    }

def normalize_book_side(levels) -> List[Dict[str, float]]:
    """
    Normalizes orderbook side into:
    [
        {"price": float, "size": float},
        ...
    ]
    """
    out = []
    if not levels:
        return out

    for lvl in levels:
        if isinstance(lvl, dict):
            price = safe_float(lvl.get("price"))
            size = safe_float(lvl.get("size", lvl.get("volume", lvl.get("quantity", 0))))
        else:
            price = safe_float(lvl[0] if len(lvl) > 0 else 0)
            size = safe_float(lvl[1] if len(lvl) > 1 else 0)

        if price > 0 and size > 0:
            out.append({"price": price, "size": size})

    return out


# =========================================================
# Orderbook fetch helpers
# =========================================================

def get_yes_no_books_kalshi(ticker: str) -> dict:
    raw = get_kalshi_orderbook(ticker)

    yes_asks = normalize_book_side(raw.get("yes", {}).get("asks", []))
    no_asks = normalize_book_side(raw.get("no", {}).get("asks", []))

    return {
        "yes_asks": yes_asks,
        "no_asks": no_asks,
        "raw": raw,
    }


def get_outcome_books_polymarket(slug: str, outcome_name: str) -> dict:
    raw = get_polymarket_orderbook(slug)
    outcome_key = normalize_str(outcome_name).lower()  # YES -> yes, NO -> no
    buy_levels = normalize_book_side(raw.get(outcome_key, {}).get("asks", []))

    return {
        "buy_levels": buy_levels,
        "raw": raw,
    }


# =========================================================
# Core depth-aware arb logic
# =========================================================

def consume_dual_books(
    levels_a: List[Dict[str, float]],
    levels_b: List[Dict[str, float]],
    max_trade_usd: float,
    fee_buffer_a: float = 0.0,
    fee_buffer_b: float = 0.0,
) -> dict:
    """
    Depth-aware execution simulation:
    - buy from both books simultaneously
    - walk deeper when best level is exhausted
    - compute weighted average prices over the executable fill
    - stop when:
        * no arb remains at current marginal prices
        * no size remains
        * max_trade_usd cap is reached

    Returns:
    {
        "found": bool,
        "contracts": float,
        "price_a": float,        # weighted avg executed price on leg A
        "price_b": float,        # weighted avg executed price on leg B
        "sum_price": float,
        "profit_pct": float,
        "notional_usd": float,
        "gross_payout_usd": float,
        "gross_profit_usd": float,
        "depth_a_used": int,
        "depth_b_used": int,
        "levels_used": [...]
    }
    """
    a = [{"price": x["price"], "size": x["size"]} for x in normalize_book_side(levels_a)]
    b = [{"price": x["price"], "size": x["size"]} for x in normalize_book_side(levels_b)]

    if not a or not b:
        return {"found": False}

    i = 0
    j = 0

    total_contracts = 0.0
    total_cost_a = 0.0
    total_cost_b = 0.0
    levels_used = []

    while i < len(a) and j < len(b):
        pa = a[i]["price"]
        pb = b[j]["price"]

        marginal_sum_price = pa + pb + fee_buffer_a + fee_buffer_b

        # No arb at this marginal level anymore
        if marginal_sum_price >= 1.0:
            break

        max_affordable_contracts = (max_trade_usd - (total_cost_a + total_cost_b)) / max(marginal_sum_price, 1e-12)
        if max_affordable_contracts <= 1e-12:
            break

        executable_size = min(
            a[i]["size"],
            b[j]["size"],
            max_affordable_contracts,
        )

        if executable_size <= 1e-12:
            break

        total_contracts += executable_size
        total_cost_a += executable_size * pa
        total_cost_b += executable_size * pb

        levels_used.append({
            "depth_a": i,
            "depth_b": j,
            "price_a": pa,
            "price_b": pb,
            "contracts": executable_size,
            "marginal_sum_price": marginal_sum_price,
            "marginal_profit_pct": (1.0 - marginal_sum_price) * 100.0,
        })

        a[i]["size"] -= executable_size
        b[j]["size"] -= executable_size

        if a[i]["size"] <= 1e-12:
            i += 1
        if b[j]["size"] <= 1e-12:
            j += 1

    if total_contracts <= 1e-12:
        return {"found": False}

    avg_price_a = total_cost_a / total_contracts
    avg_price_b = total_cost_b / total_contracts
    avg_sum_price = avg_price_a + avg_price_b + fee_buffer_a + fee_buffer_b
    gross_payout_usd = total_contracts * 1.0
    gross_profit_usd = gross_payout_usd - (total_contracts * avg_sum_price)
    profit_pct = ((1.0 - avg_sum_price) * 100.0)

    return {
        "found": True,
        "contracts": total_contracts,
        "price_a": avg_price_a,
        "price_b": avg_price_b,
        "sum_price": avg_sum_price,
        "profit_pct": profit_pct,
        "notional_usd": total_contracts * avg_sum_price,
        "gross_payout_usd": gross_payout_usd,
        "gross_profit_usd": gross_profit_usd,
        "depth_a_used": max([x["depth_a"] for x in levels_used]) if levels_used else 0,
        "depth_b_used": max([x["depth_b"] for x in levels_used]) if levels_used else 0,
        "levels_used": levels_used,
    }


def choose_best_arb_for_pair(pair_row: pd.Series) -> dict:
    """
    Check both:
      1) Kalshi YES + Polymarket NO
      2) Kalshi NO  + Polymarket YES

    Return best weighted executable arb for this tracked pair only.
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    max_trade_usd = safe_float(
        pair_row.get("max_position_per_pair_usd", DEFAULT_MAX_TRADE_USD),
        DEFAULT_MAX_TRADE_USD,
    )

    kalshi_books = get_yes_no_books_kalshi(kalshi_ticker)
    poly_no = get_outcome_books_polymarket(polymarket_slug, "NO")
    poly_yes = get_outcome_books_polymarket(polymarket_slug, "YES")

    candidate_yes_no = top_of_book_arb(
        kalshi_books["yes_asks"],
        poly_no["buy_levels"],
        max_trade_usd=max_trade_usd,
        fee_buffer_a=KALSHI_FEE_BUFFER,
        fee_buffer_b=POLY_FEE_BUFFER,
    )

    candidate_no_yes = top_of_book_arb(
        kalshi_books["no_asks"],
        poly_yes["buy_levels"],
        max_trade_usd=max_trade_usd,
        fee_buffer_a=KALSHI_FEE_BUFFER,
        fee_buffer_b=POLY_FEE_BUFFER,
    )

    options = []

    if candidate_yes_no.get("found"):
        candidate_yes_no["kalshi_side"] = "yes"
        candidate_yes_no["polymarket_outcome"] = "NO"
        candidate_yes_no["kalshi_ticker"] = kalshi_ticker
        candidate_yes_no["polymarket_ticker"] = polymarket_slug
        options.append(candidate_yes_no)

    if candidate_no_yes.get("found"):
        candidate_no_yes["kalshi_side"] = "no"
        candidate_no_yes["polymarket_outcome"] = "YES"
        candidate_no_yes["kalshi_ticker"] = kalshi_ticker
        candidate_no_yes["polymarket_ticker"] = polymarket_slug
        options.append(candidate_no_yes)

    pair_id = normalize_str(
        pair_row.get("pair_id", f"{kalshi_ticker}__{polymarket_slug}")
    )

    #print(f"\nPAIR DEBUG: {pair_id}")
    #print("Kalshi YES asks:", kalshi_books["yes_asks"][:5])
    #print("Kalshi NO asks: ", kalshi_books["no_asks"][:5])
    #print("Poly NO asks:   ", poly_no["buy_levels"][:5])
    #print("Poly YES asks:  ", poly_yes["buy_levels"][:5])

    if not options:
        return {"found": False}

    # Prefer highest weighted executable profit %
    best = max(
        options,
        key=lambda x: (
            x["profit_pct"],
            x["gross_profit_usd"],
            x["contracts"],
        ),
    )
    return best


# =========================================================
# Optional last-second reverification
# =========================================================

def reverify_pair_live(pair_row: pd.Series, original_arb: dict) -> dict:
    """
    Re-fetch books right before execution and recompute the same side only,
    using top-of-book logic only.
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    max_trade_usd = safe_float(
        pair_row.get("max_position_per_pair_usd", DEFAULT_MAX_TRADE_USD),
        DEFAULT_MAX_TRADE_USD,
    )

    kalshi_books = get_yes_no_books_kalshi(kalshi_ticker)

    if original_arb["kalshi_side"] == "yes" and original_arb["polymarket_outcome"] == "NO":
        poly_books = get_outcome_books_polymarket(polymarket_slug, "NO")
        live = top_of_book_arb(
            kalshi_books["yes_asks"],
            poly_books["buy_levels"],
            max_trade_usd=max_trade_usd,
            fee_buffer_a=KALSHI_FEE_BUFFER,
            fee_buffer_b=POLY_FEE_BUFFER,
        )
        if live.get("found"):
            live["kalshi_side"] = "yes"
            live["polymarket_outcome"] = "NO"
        return live

    if original_arb["kalshi_side"] == "no" and original_arb["polymarket_outcome"] == "YES":
        poly_books = get_outcome_books_polymarket(polymarket_slug, "YES")
        live = top_of_book_arb(
            kalshi_books["no_asks"],
            poly_books["buy_levels"],
            max_trade_usd=max_trade_usd,
            fee_buffer_a=KALSHI_FEE_BUFFER,
            fee_buffer_b=POLY_FEE_BUFFER,
        )
        if live.get("found"):
            live["kalshi_side"] = "no"
            live["polymarket_outcome"] = "YES"
        return live

    return {"found": False}

# =========================================================
# Execution
# =========================================================

def place_dual_orders(pair_row: pd.Series, arb: dict) -> dict:
    """
    Place both legs, but first shrink size to available balances on both venues.
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    balance_check = size_trade_to_available_balances(arb)
    if not balance_check["ok"]:
        return {
            "status": "skipped",
            "message": balance_check["reason"],
        }

    arb = balance_check["arb"]
    contracts = int(math.floor(arb["contracts"]))

    if contracts <= 0:
        return {"status": "skipped", "message": "contracts rounded to 0 after balance sizing"}

    kalshi_price_cents = int(round(arb["price_a"] * 100))
    polymarket_price = round(arb["price_b"], 6)

    print(
        "BALANCE CHECK:",
        {
            "kalshi_available_usd": round(balance_check["kalshi_available_usd"], 2),
            "polymarket_available_usd": round(balance_check["polymarket_available_usd"], 2),
            "kalshi_required_usd": round(balance_check["kalshi_required_usd"], 2),
            "polymarket_required_usd": round(balance_check["polymarket_required_usd"], 2),
            "max_contracts_kalshi": balance_check["max_contracts_kalshi"],
            "max_contracts_poly": balance_check["max_contracts_poly"],
            "contracts_sent": contracts,
        }
    )

    kalshi_resp = kalshi_place_limit_order(
        ticker=kalshi_ticker,
        side=arb["kalshi_side"],
        action="buy",
        count=contracts,
        price_cents=kalshi_price_cents,
        time_in_force="fill_or_kill",
    )

    poly_resp = polymarket_place_limit_order(
        slug=polymarket_slug,
        outcome=arb["polymarket_outcome"],
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
        "gross_profit_usd": contracts * (1.0 - arb["sum_price"]),
        "message": "",
    }

# =========================================================
# Pair processing
# =========================================================

def process_pair(
    pair_row: pd.Series,
    dry_run: bool = True,
    min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT,
    min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD,
    reverify_books: bool = DEFAULT_REVERIFY_BOOKS,
) -> None:
    pair_id = normalize_str(
        pair_row.get(
            "pair_id",
            f"{pair_row['kalshi_ticker']}__{pair_row['polymarket_ticker']}"
        )
    )

    active = truthy(pair_row.get("active", "true"))
    if not active:
        print(f"[{pair_id}] skipped (inactive)")
        return

    try:
        arb = choose_best_arb_for_pair(pair_row)

        if not arb.get("found"):
            print(f"[{pair_id}] no arb found")
            return

        if arb["profit_pct"] < min_profit_pct:
            print(f"[{pair_id}] arb found but below min profit: {arb['profit_pct']:.3f}%")
            return

        if arb["notional_usd"] < min_liquidity_usd:
            print(f"[{pair_id}] arb found but insufficient liquidity: ${arb['notional_usd']:.2f}")
            return

        print(
            f"[{pair_id}] arb found | "
            f"Kalshi {arb['kalshi_side'].upper()} @ {arb['price_a']:.4f} + "
            f"Poly {arb['polymarket_outcome']} @ {arb['price_b']:.4f} = "
            f"{arb['sum_price']:.4f} | "
            f"profit {arb['profit_pct']:.3f}% | "
            f"contracts {arb['contracts']:.4f} | "
            f"notional ${arb['notional_usd']:.2f} | "
            f"gross profit ${arb['gross_profit_usd']:.4f}"
        )

        if reverify_books:
            live_arb = reverify_pair_live(pair_row, arb)

            if not live_arb.get("found"):
                msg = "reverification failed: no live executable arb on same side"
                print(f"[{pair_id}] {msg}")
                append_execution_log({
                    "timestamp": utc_now_iso(),
                    "pair_id": pair_id,
                    "status": "reverify_failed",
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
                    "gross_profit_usd": arb["gross_profit_usd"],
                    "message": msg,
                })
                return

            if live_arb["profit_pct"] < min_profit_pct:
                msg = f"reverified arb below min profit: {live_arb['profit_pct']:.3f}%"
                print(f"[{pair_id}] {msg}")
                return

            if live_arb["notional_usd"] < min_liquidity_usd:
                msg = f"reverified arb below min liquidity: ${live_arb['notional_usd']:.2f}"
                print(f"[{pair_id}] {msg}")
                return

            arb = live_arb
            print(
                f"[{pair_id}] live reverified | "
                f"Kalshi {arb['kalshi_side'].upper()} @ {arb['price_a']:.4f} + "
                f"Poly {arb['polymarket_outcome']} @ {arb['price_b']:.4f} = "
                f"{arb['sum_price']:.4f} | "
                f"profit {arb['profit_pct']:.3f}% | "
                f"contracts {arb['contracts']:.4f}"
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
                "gross_profit_usd": arb["gross_profit_usd"],
                "depth_a_used": arb.get("depth_a_used"),
                "depth_b_used": arb.get("depth_b_used"),
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
            "gross_profit_usd": result.get("gross_profit_usd"),
            "depth_a_used": arb.get("depth_a_used"),
            "depth_b_used": arb.get("depth_b_used"),
            "message": result.get("message", ""),
        })

        print(
            f"[{pair_id}] EXECUTED | "
            f"contracts={result.get('contracts')} | "
            f"sum_price={result.get('sum_price'):.4f} | "
            f"profit={result.get('profit_pct'):.3f}%"
        )

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


# =========================================================
# Run control
# =========================================================

def run_once(
    tracked_pairs_csv: str,
    dry_run: bool = True,
    min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT,
    min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD,
    reverify_books: bool = DEFAULT_REVERIFY_BOOKS,
) -> None:
    if not os.path.exists(tracked_pairs_csv):
        raise FileNotFoundError(f"{tracked_pairs_csv} not found")

    df = pd.read_csv(tracked_pairs_csv)
    print(f"Loaded {len(df)} tracked pairs from {tracked_pairs_csv}")

    required_cols = ["kalshi_ticker", "polymarket_ticker"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for _, row in df.iterrows():
        process_pair(
            pair_row=row,
            dry_run=dry_run,
            min_profit_pct=min_profit_pct,
            min_liquidity_usd=min_liquidity_usd,
            reverify_books=reverify_books,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Tracked-pairs-only portfolio arbitrage monitor/executor"
    )
    parser.add_argument("--input", default=TRACKED_PAIRS_CSV, help="Tracked pairs CSV")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval-minutes", type=int, default=DEFAULT_SLEEP_MINUTES, help="Minutes between refreshes")
    parser.add_argument("--live", action="store_true", help="Actually place orders")
    parser.add_argument("--min-profit-pct", type=float, default=DEFAULT_MIN_PROFIT_PCT, help="Minimum gross arb %%")
    parser.add_argument("--min-liquidity-usd", type=float, default=DEFAULT_MIN_LIQUIDITY_USD, help="Minimum executable notional")
    parser.add_argument("--no-reverify", action="store_true", help="Skip last-second live book reverification")
    args = parser.parse_args()

    dry_run = not args.live
    reverify_books = not args.no_reverify

    if not args.loop:
        run_once(
            tracked_pairs_csv=args.input,
            dry_run=dry_run,
            min_profit_pct=args.min_profit_pct,
            min_liquidity_usd=args.min_liquidity_usd,
            reverify_books=reverify_books,
        )
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1

    while True:
        cycle_start = time.time()
        print(f"\n########## TRACKED-PAIRS ARB ITERATION {iteration} ##########")

        try:
            run_once(
                tracked_pairs_csv=args.input,
                dry_run=dry_run,
                min_profit_pct=args.min_profit_pct,
                min_liquidity_usd=args.min_liquidity_usd,
                reverify_books=reverify_books,
            )
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
