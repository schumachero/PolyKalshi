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

from arbitrage_calculator import (
    calculate_polymarket_fee,
    get_polymarket_fee_category,
)

# =========================================================
# Configuration
# =========================================================

TRACKED_PAIRS_CSV = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs_buy.csv")
EXECUTION_LOG_CSV = os.path.join(PROJECT_ROOT, "Data", "portfolio_arb_execution_log.csv")

DEFAULT_MAX_TRADE_USD = 101
DEFAULT_MIN_PROFIT_PCT = 6.5
DEFAULT_MIN_PROFIT_FLOOR_PCT = 0.3
DEFAULT_MIN_CAGR_PCT = 30
DEFAULT_MIN_LIQUIDITY_USD = 4.0
DEFAULT_SLEEP_MINUTES = 30
REINVESTMENT_LAG_DAYS = 2.0  # Settlement + redeployment buffer

# Kalshi taker fee: 7% * p * (1 - p)  (in dollar terms, 0-1 scale)
KALSHI_FEE_RATE = 0.07

def _kalshi_fee(price: float) -> float:
    """Kalshi taker fee for a single contract at `price` (0-1 scale)."""
    if price <= 0.0 or price >= 1.0:
        return 0.0
    return KALSHI_FEE_RATE * price * (1.0 - price)

# Reverify against live books again immediately before sending orders
DEFAULT_REVERIFY_BOOKS = True

# =========================================================
# Helpers
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def top_of_book_arb(levels_a, levels_b, max_trade_usd, pm_category="Other / General"):
    """
    Only use depth 1 from each book.
    Leg A = Kalshi ask, Leg B = Polymarket ask.
    Fees are computed from actual prices:
      - Kalshi: 7% * p * (1-p)
      - Polymarket: category-based fee from arbitrage_calculator

    Returns:
    {
        "found": bool,
        "contracts": float,
        "price_a": float,       # raw Kalshi ask
        "price_b": float,       # raw Polymarket ask
        "fee_a": float,         # Kalshi fee per contract
        "fee_b": float,         # Polymarket fee per contract
        "sum_price": float,     # total cost incl. fees
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

    fee_a = _kalshi_fee(pa)
    fee_b = calculate_polymarket_fee(pb, pm_category)

    poly_fee_rate = (fee_b / pb) if pb > 0 else 0.0
    adjusted_pb = pb / (1.0 - poly_fee_rate) if poly_fee_rate < 1.0 else pb

    sum_price = pa + adjusted_pb + fee_a
    
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
        "fee_a": fee_a,
        "fee_b": fee_b,
        "sum_price": sum_price,
        "profit_pct": profit_pct,
        "notional_usd": notional_usd,
        "gross_profit_usd": gross_profit_usd,
        "depth_a_used": 0,
        "depth_b_used": 0,
    }

def check_arb_thresholds(
    profit_pct: float,
    pair_row: pd.Series,
    min_profit_pct: float,
    min_profit_floor_pct: float,
    min_cagr_pct: float,
    live_close_time: Optional[str] = None
) -> dict:
    result = {
        "is_acceptable": False,
        "cagr_pct": 0.0,
        "days_to_close": -1.0
    }

    if profit_pct >= min_profit_pct:
        result["is_acceptable"] = True

    if not result["is_acceptable"]:
        # Prioritize live_close_time from API, fall back to CSV
        close_time_str = live_close_time or pair_row.get("close_time", "")
        if close_time_str and not pd.isna(close_time_str):
            try:
                parsed_date = pd.to_datetime(close_time_str, utc=True)
                delta_days = (parsed_date - datetime.now(timezone.utc)).total_seconds() / 86400.0
                result["days_to_close"] = max(delta_days, 0.5)
                # Lag-adjusted CAGR: compound over realistic reinvestment cycles
                effective_days = result["days_to_close"] + REINVESTMENT_LAG_DAYS
                cycles_per_year = 365.0 / effective_days
                decimal_profit = profit_pct / 100.0
                
                # Check for negative value before power to avoid complex numbers in python
                if decimal_profit <= -1.0:
                    result["cagr_pct"] = -100.0
                else:
                    result["cagr_pct"] = ((1.0 + decimal_profit) ** cycles_per_year - 1.0) * 100.0
                
                # Only mark acceptable if it hits both the profit floor and CAGR min
                if profit_pct >= min_profit_floor_pct and result["cagr_pct"] >= min_cagr_pct:
                    result["is_acceptable"] = True
            except Exception:
                pass

    return result

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

    # Polymarket cost incorporates fees directly into the size
    poly_fee_rate = (arb["fee_b"] / arb["price_b"]) if arb["price_b"] > 0 else 0.0
    adjusted_pb = arb["price_b"] / (1.0 - poly_fee_rate) if poly_fee_rate < 1.0 else arb["price_b"]

    max_contracts_kalshi = int(math.floor(kalshi_spendable / max(arb["price_a"] + arb["fee_a"], 1e-12)))
    max_contracts_poly = int(math.floor(poly_spendable / max(adjusted_pb, 1e-12)))

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

    poly_fee_rate = (arb["fee_b"] / arb["price_b"]) if arb["price_b"] > 0 else 0.0
    polymarket_order_size = final_contracts / (1.0 - poly_fee_rate) if poly_fee_rate < 1.0 else final_contracts

    kalshi_required_usd = final_contracts * (arb["price_a"] + arb["fee_a"])
    polymarket_required_usd = polymarket_order_size * arb["price_b"]

    if polymarket_required_usd < 1.0:
        return {
            "ok": False,
            "reason": f"Polymarket required USD ${polymarket_required_usd:.2f} is below the API $1.00 minimum",
            "kalshi_available_usd": kalshi_available_usd,
            "polymarket_available_usd": polymarket_available_usd,
        }

    sized_arb = dict(arb)
    sized_arb["contracts"] = final_contracts
    sized_arb["polymarket_order_size"] = polymarket_order_size
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
        "close_time": raw.get("close_time"),
        "raw": raw,
    }


def get_outcome_books_polymarket(slug: str, outcome_name: str) -> dict:
    raw = get_polymarket_orderbook(slug)
    outcome_key = normalize_str(outcome_name).lower()  # YES -> yes, NO -> no
    buy_levels = normalize_book_side(raw.get(outcome_key, {}).get("asks", []))

    return {
        "buy_levels": buy_levels,
        "close_time": raw.get("close_time"),
        "raw": raw,
    }


# =========================================================
# Core depth-aware arb logic
# =========================================================

def consume_dual_books(
    levels_a: List[Dict[str, float]],
    levels_b: List[Dict[str, float]],
    max_trade_usd: float,
    pm_category: str = "Other / General",
) -> dict:
    """
    Depth-aware execution simulation:
    - buy from both books simultaneously
    - walk deeper when best level is exhausted
    - compute weighted average prices over the executable fill
    - fees are computed per-level from actual prices:
        * Leg A (Kalshi): 7% * p * (1-p)
        * Leg B (Polymarket): category-based fee
    - stop when:
        * no arb remains at current marginal prices (incl. fees)
        * no size remains
        * max_trade_usd cap is reached

    Returns:
    {
        "found": bool,
        "contracts": float,
        "price_a": float,        # weighted avg executed price on leg A
        "price_b": float,        # weighted avg executed price on leg B
        "sum_price": float,      # total cost incl. fees
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
    total_fees = 0.0
    levels_used = []

    while i < len(a) and j < len(b):
        pa = a[i]["price"]
        pb = b[j]["price"]

        fee_a = _kalshi_fee(pa)
        fee_b = calculate_polymarket_fee(pb, pm_category)

        marginal_sum_price = pa + pb + fee_a + fee_b

        # No arb at this marginal level anymore, should it really be 1.0? Should it not be
        # the cut of of acceptable profit? 
        if marginal_sum_price >= 1.0:
            break

        spent_so_far = total_cost_a + total_cost_b + total_fees
        max_affordable_contracts = (max_trade_usd - spent_so_far) / max(marginal_sum_price, 1e-12)
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
        total_fees += executable_size * (fee_a + fee_b)

        levels_used.append({
            "depth_a": i,
            "depth_b": j,
            "price_a": pa,
            "price_b": pb,
            "fee_a": fee_a,
            "fee_b": fee_b,
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
    avg_fee = total_fees / total_contracts
    avg_sum_price = avg_price_a + avg_price_b + avg_fee
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


def _get_pm_category(pair_row: pd.Series) -> str:
    """Derive the Polymarket fee category from the pair row's title fields."""
    poly_title = normalize_str(pair_row.get("polymarket_title", ""))
    kalshi_title = normalize_str(pair_row.get("kalshi_title", ""))
    series_title = ""  # tracked_pairs.csv doesn't have a series column
    # Use poly title if available, fall back to kalshi title for category detection
    market_title = poly_title or kalshi_title
    return get_polymarket_fee_category(market_title, series_title)


def choose_best_arb_for_pair(pair_row: pd.Series) -> dict:
    """
    Check both:
      1) Kalshi YES + Polymarket NO
      2) Kalshi NO  + Polymarket YES

    Return best weighted executable arb for this tracked pair only.
    Fees are computed from actual prices (Kalshi 7% + Polymarket category-based).
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    csv_limit = safe_float(pair_row.get("max_position_per_pair_usd"), 999999)
    max_trade_usd = min(csv_limit, DEFAULT_MAX_TRADE_USD)

    pm_category = _get_pm_category(pair_row)

    kalshi_books = get_yes_no_books_kalshi(kalshi_ticker)
    poly_no = get_outcome_books_polymarket(polymarket_slug, "NO")
    poly_yes = get_outcome_books_polymarket(polymarket_slug, "YES")

    # Use the close_time from the platform if available, fallback to CSV later
    live_close_time = kalshi_books.get("close_time") or poly_no.get("close_time") or poly_yes.get("close_time")

    candidate_yes_no = top_of_book_arb(
        kalshi_books["yes_asks"],
        poly_no["buy_levels"],
        max_trade_usd=max_trade_usd,
        pm_category=pm_category,
    )

    candidate_no_yes = top_of_book_arb(
        kalshi_books["no_asks"],
        poly_yes["buy_levels"],
        max_trade_usd=max_trade_usd,
        pm_category=pm_category,
    )

    options = []

    if candidate_yes_no.get("found"):
        candidate_yes_no["kalshi_side"] = "yes"
        candidate_yes_no["polymarket_outcome"] = "NO"
        candidate_yes_no["kalshi_ticker"] = kalshi_ticker
        candidate_yes_no["polymarket_ticker"] = polymarket_slug
        candidate_yes_no["pm_category"] = pm_category
        candidate_yes_no["live_close_time"] = live_close_time
        options.append(candidate_yes_no)

    if candidate_no_yes.get("found"):
        candidate_no_yes["kalshi_side"] = "no"
        candidate_no_yes["polymarket_outcome"] = "YES"
        candidate_no_yes["kalshi_ticker"] = kalshi_ticker
        candidate_no_yes["polymarket_ticker"] = polymarket_slug
        candidate_no_yes["pm_category"] = pm_category
        candidate_no_yes["live_close_time"] = live_close_time
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
    using top-of-book logic with real fee calculation.
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    csv_limit = safe_float(pair_row.get("max_position_per_pair_usd"), 999999)
    max_trade_usd = min(csv_limit, DEFAULT_MAX_TRADE_USD)

    pm_category = original_arb.get("pm_category", _get_pm_category(pair_row))

    kalshi_books = get_yes_no_books_kalshi(kalshi_ticker)

    if original_arb["kalshi_side"] == "yes" and original_arb["polymarket_outcome"] == "NO":
        poly_books = get_outcome_books_polymarket(polymarket_slug, "NO")
        live = top_of_book_arb(
            kalshi_books["yes_asks"],
            poly_books["buy_levels"],
            max_trade_usd=max_trade_usd,
            pm_category=pm_category,
        )
        if live.get("found"):
            live["kalshi_side"] = "yes"
            live["polymarket_outcome"] = "NO"
            live["pm_category"] = pm_category
        return live

    if original_arb["kalshi_side"] == "no" and original_arb["polymarket_outcome"] == "YES":
        poly_books = get_outcome_books_polymarket(polymarket_slug, "YES")
        live = top_of_book_arb(
            kalshi_books["no_asks"],
            poly_books["buy_levels"],
            max_trade_usd=max_trade_usd,
            pm_category=pm_category,
        )
        if live.get("found"):
            live["kalshi_side"] = "no"
            live["polymarket_outcome"] = "YES"
            live["pm_category"] = pm_category
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
    
    # Polymarket API constraints for BUY orders:
    #   maker_amount (size * price) → max 2 decimal places
    #   taker_amount (size)         → max 2 decimal places (per ROUNDING_CONFIG)
    #
    # The py_clob_client checks decimal places via Decimal(str(float_val)),
    # so we must find a size where the *float* product is clean.
    def _poly_clean_size(raw_size: float, price: float) -> float:
        """Find largest size ≤ raw_size (step 0.01) where float(size*price) has ≤ 2 dp."""
        from decimal import Decimal
        sz = math.floor(raw_size * 100) / 100  # start at floor to 2 dp
        for _ in range(200):  # max 200 steps down ($2 range)
            if sz <= 0:
                return 0.0
            product = sz * price
            dp = abs(Decimal(str(product)).as_tuple().exponent)
            if dp <= 2:
                return sz
            sz = round(sz - 0.01, 2)
        return 0.0
    
    polymarket_order_size = _poly_clean_size(
        arb.get("polymarket_order_size", contracts), polymarket_price
    )
    
    print(
        "BALANCE CHECK:",
        {
            "kalshi_available_usd": round(balance_check["kalshi_available_usd"], 2),
            "polymarket_available_usd": round(balance_check["polymarket_available_usd"], 2),
            "kalshi_required_usd": round(balance_check["kalshi_required_usd"], 2),
            "polymarket_required_usd": round(balance_check["polymarket_required_usd"], 2),
            "max_contracts_kalshi": balance_check["max_contracts_kalshi"],
            "max_contracts_poly": balance_check["max_contracts_poly"],
            "contracts_sent_kalshi": contracts,
            "contracts_sent_poly": polymarket_order_size,
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
        size=polymarket_order_size,
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
    min_profit_floor_pct: float = DEFAULT_MIN_PROFIT_FLOOR_PCT,
    min_cagr_pct: float = DEFAULT_MIN_CAGR_PCT,
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

        eval_res = check_arb_thresholds(
            arb["profit_pct"], pair_row,
            min_profit_pct, min_profit_floor_pct, min_cagr_pct,
            live_close_time=arb.get("live_close_time")
        )

        arb["cagr_pct"] = eval_res["cagr_pct"]
        arb["days_to_close"] = eval_res["days_to_close"]

        if arb["notional_usd"] < min_liquidity_usd:
            print(f"[{pair_id}] arb rejected due to insufficient liquidity: ${arb['notional_usd']:.2f}")
            return

        cagr_str = f" | CAGR {arb['cagr_pct']:.1f}% (days: {arb['days_to_close']:.1f})" if arb["days_to_close"] > 0 else ""
        
        cagr_print_msg = (f"[{pair_id}] arb found | "
                          f"Kalshi {arb['kalshi_side'].upper()} @ {arb['price_a']:.4f} + "
                          f"Poly {arb['polymarket_outcome']} @ {arb['price_b']:.4f} = "
                          f"{arb['sum_price']:.4f} | "
                          f"profit {arb['profit_pct']:.3f}%{cagr_str}")

        if not eval_res["is_acceptable"]:
            print(f"REJECTED: {cagr_print_msg}")
            return
            
        print(
            f"{cagr_print_msg} | "
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
                    "cagr_pct": arb["cagr_pct"],
                    "days_to_close": arb["days_to_close"],
                    "contracts": arb["contracts"],
                    "notional_usd": arb["notional_usd"],
                    "gross_profit_usd": arb["gross_profit_usd"],
                    "message": msg,
                })
                return

            live_eval = check_arb_thresholds(
                live_arb["profit_pct"], pair_row,
                min_profit_pct, min_profit_floor_pct, min_cagr_pct,
                live_close_time=live_arb.get("live_close_time")
            )

            if not live_eval["is_acceptable"]:
                msg = f"reverified arb rejected: {live_arb['profit_pct']:.3f}%"
                if live_eval["days_to_close"] > 0:
                    msg += f" (CAGR: {live_eval['cagr_pct']:.1f}%)"
                print(f"[{pair_id}] {msg}")
                return
            live_arb["cagr_pct"] = live_eval["cagr_pct"]
            live_arb["days_to_close"] = live_eval["days_to_close"]

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
                "cagr_pct": arb.get("cagr_pct", 0.0),
                "days_to_close": arb.get("days_to_close", -1.0),
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
            "cagr_pct": arb.get("cagr_pct", 0.0),
            "days_to_close": arb.get("days_to_close", -1.0),
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
    min_profit_floor_pct: float = DEFAULT_MIN_PROFIT_FLOOR_PCT,
    min_cagr_pct: float = DEFAULT_MIN_CAGR_PCT,
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
            min_profit_floor_pct=min_profit_floor_pct,
            min_cagr_pct=min_cagr_pct,
            min_liquidity_usd=min_liquidity_usd,
            reverify_books=reverify_books,
        )


def main():
    global REINVESTMENT_LAG_DAYS
    parser = argparse.ArgumentParser(
        description="Tracked-pairs-only portfolio arbitrage monitor/executor"
    )
    parser.add_argument("--input", default=TRACKED_PAIRS_CSV, help="Tracked pairs CSV")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval-minutes", type=int, default=DEFAULT_SLEEP_MINUTES, help="Minutes between refreshes")
    parser.add_argument("--live", action="store_true", help="Actually place orders")
    parser.add_argument("--min-profit-pct", type=float, default=DEFAULT_MIN_PROFIT_PCT, help="Minimum gross arb %%")
    parser.add_argument("--min-profit-floor-pct", type=float, default=DEFAULT_MIN_PROFIT_FLOOR_PCT, help="Absolute floor for gross arb %%")
    parser.add_argument("--min-cagr-pct", type=float, default=DEFAULT_MIN_CAGR_PCT, help="Minimum lag-adjusted CAGR %% for lower arbs")
    parser.add_argument("--reinvestment-lag-days", type=float, default=REINVESTMENT_LAG_DAYS, help="Days between market resolution and capital redeployment")
    parser.add_argument("--min-liquidity-usd", type=float, default=DEFAULT_MIN_LIQUIDITY_USD, help="Minimum executable notional")
    parser.add_argument("--no-reverify", action="store_true", help="Skip last-second live book reverification")
    args = parser.parse_args()

    dry_run = not args.live
    reverify_books = not args.no_reverify

    REINVESTMENT_LAG_DAYS = args.reinvestment_lag_days

    if not args.loop:
        run_once(
            tracked_pairs_csv=args.input,
            dry_run=dry_run,
            min_profit_pct=args.min_profit_pct,
            min_profit_floor_pct=args.min_profit_floor_pct,
            min_cagr_pct=args.min_cagr_pct,
            min_liquidity_usd=args.min_liquidity_usd,
            reverify_books=reverify_books,
        )
        print(f"Run finished at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        return

    interval_seconds = max(args.interval_minutes, 1) * 60
    iteration = 1

    while True:
        cycle_start = time.time()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n########## [{now_str}] TRACKED-PAIRS ARB ITERATION {iteration} ##########")

        try:
            run_once(
                tracked_pairs_csv=args.input,
                dry_run=dry_run,
                min_profit_pct=args.min_profit_pct,
                min_profit_floor_pct=args.min_profit_floor_pct,
                min_cagr_pct=args.min_cagr_pct,
                min_liquidity_usd=args.min_liquidity_usd,
                reverify_books=reverify_books,
            )
        except Exception as e:
            print(f"Fatal iteration error: {e}")

        elapsed = time.time() - cycle_start
        sleep_seconds = max(interval_seconds - elapsed, 0)

        print(f"Iteration {iteration} finished in {elapsed:.1f}s at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"Sleeping for {sleep_seconds / 60:.2f} minutes... Press Ctrl+C to stop.")

        try:
            time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break

        iteration += 1


if __name__ == "__main__":
    main()
