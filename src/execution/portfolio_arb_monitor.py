import os
import sys
import time
import math
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional
try:
    from apis.portfolio import (
        get_kalshi_balance, 
        get_polymarket_balance, 
        get_kalshi_positions, 
        get_polymarket_positions
    )
except ImportError:
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from apis.portfolio import (
        get_kalshi_balance, 
        get_polymarket_balance, 
        get_kalshi_positions, 
        get_polymarket_positions
    )
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

# Performance & Swap Configuration
DEFAULT_SWAP_HURDLE_APY = 15.0      # Only swap if new APY is 15% better
DEFAULT_MIN_SWAP_GAIN_USD = 1.00    # Absolute minimum expected profit gain to swap
DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR = 0.20 # 20% limit per pair for diversification
SWAP_FEE_CUSHION_PCT = 1.0         # 1% buffer for fees/slippage when calculating swap feasibility

# =========================================================
# Helpers
# =========================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_date(date_str: str) -> Optional[datetime]:
    if not date_str or str(date_str).strip() == "" or pd.isna(date_str):
        return None
    try:
        dt_str = str(date_str).strip()
        # Handle formats like 2026-03-31 or ISO 2026-03-31T23:59:59Z
        if 'T' in dt_str:
            # fromisoformat handles +00:00; replace Z for compatibility with older Python if needed
            ds = dt_str.replace('Z', '+00:00')
            return datetime.fromisoformat(ds)
        else:
            return datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def calculate_apy(profit_pct: float, close_time_str: str) -> float:
    """
    Returns linear APY.
    """
    close_dt = parse_date(close_time_str)
    if not close_dt:
        return 0.0

    now = datetime.now(timezone.utc)
    delta = close_dt - now
    days = max(delta.total_seconds() / 86400.0, 0.1)  # floor at 0.1 days to avoid div by zero

    return (profit_pct * 365.0) / days


def get_current_exposure(tracked_pairs_df: pd.DataFrame) -> dict:
    """
    Fetch live positions and map them to our tracked pairs.
    """
    print("Fetching live positions for exposure check...")
    try:
        k_pos = get_kalshi_positions()
    except Exception as e:
        print(f"Error fetching Kalshi positions: {e}")
        k_pos = []
    
    try:
        p_pos = get_polymarket_positions()
    except Exception as e:
        print(f"Error fetching Polymarket positions: {e}")
        p_pos = []

    exposure = {}
    
    # Map Kalshi/Poly tickers to pair_id
    k_to_pair = {}
    p_to_pair = {}
    for _, row in tracked_pairs_df.iterrows():
        kt = normalize_str(row.get('kalshi_ticker'))
        pt = normalize_str(row.get('polymarket_ticker'))
        pid = normalize_str(row.get('pair_id', f"{kt}__{pt}"))
        
        k_to_pair[kt] = pid
        p_to_pair[pt] = pid
        exposure[pid] = {
            "pair_id": pid,
            "contracts_kalshi": 0,
            "kalshi_side": "",
            "contracts_poly": 0,
            "polymarket_side": "",
            "value_usd": 0.0,
            "kalshi_ticker": kt,
            "polymarket_ticker": pt,
            "close_time": normalize_str(row.get('close_time', ""))
        }

    total_val = 0.0
    for pos in k_pos:
        tic = normalize_str(pos['ticker'])
        if tic in k_to_pair:
            pid = k_to_pair[tic]
            exposure[pid]["contracts_kalshi"] += pos['quantity']
            exposure[pid]["kalshi_side"] = pos['side'].lower() # yes/no
            val = safe_float(pos.get('market_exposure_cents', 0)) / 100.0
            exposure[pid]["value_usd"] += val
            total_val += val

    for pos in p_pos:
        tic = normalize_str(pos['market_id'])
        if tic in p_to_pair:
            pid = p_to_pair[tic]
            exposure[pid]["contracts_poly"] += pos['size']
            exposure[pid]["polymarket_side"] = pos['side'].upper() # YES/NO
            val = safe_float(pos.get('current_value', 0))
            exposure[pid]["value_usd"] += val
            total_val += val
            
    return {
        "total_portfolio_usd": total_val,
        "pair_exposure": exposure
    }


def calculate_holding_apy(exposure_info: dict) -> float:
    """
    Calculate the APY we get if we hold the CURRENT position to maturity.
    Payout is 1.0 per contract. Cost is current 'value_usd'.
    """
    val = exposure_info['value_usd']
    contracts = max(exposure_info['contracts_kalshi'], exposure_info['contracts_poly'])
    
    if val <= 0 or contracts <= 0:
        return 0.0
    
    # If we hold, we get 'contracts' dollars.
    # Current value is 'val'.
    profit_pct = (contracts / val - 1.0) * 100.0
    return calculate_apy(profit_pct, exposure_info['close_time'])


def liquidate_pair(pair_id: str, exposure: dict, dry_run: bool = True) -> dict:
    """
    Sell all held contracts for a pair.
    Uses aggressive limit orders (selling into Bids).
    """
    info = exposure.get(pair_id)
    if not info:
        return {"status": "error", "message": "Pair not found in exposure"}
    
    k_qty = int(info['contracts_kalshi'])
    p_qty = int(info['contracts_poly'])
    
    if k_qty <= 0 and p_qty <= 0:
        return {"status": "skipped", "message": "No contracts to liquidate"}
        
    if dry_run:
        return {"status": "dry_run", "message": f"Would liquidate {k_qty} {info['kalshi_side']} on Kalshi and {p_qty} {info['polymarket_side']} on Poly"}

    # Kalshi Sell
    k_resp = None
    if k_qty > 0:
        # To sell, we need the BID price
        books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        side_key = f"{info['kalshi_side']}_asks" # Wait, BID is where we sell. 
        # Actually our normalize_book_side helper only gets asks by default in some places.
        raw = books['raw']
        bid_cents = raw.get(info['kalshi_side'], {}).get('yes_bid' if info['kalshi_side'] == 'yes' else 'no_bid') # Simplified
        
        # Better: just use a very low price (1 cent) with IOC/FOK to sell into whatever bids exist
        k_resp = kalshi_place_limit_order(
            ticker=info['kalshi_ticker'],
            side=info['kalshi_side'],
            action="sell",
            count=k_qty,
            price_cents=1, # Sell into any bid
            time_in_force="immediate_or_cancel"
        )

    # Poly Sell
    p_resp = None
    if p_qty > 0:
        p_resp = polymarket_place_limit_order(
            slug=info['polymarket_ticker'],
            outcome=info['polymarket_side'],
            size=p_qty,
            price=0.01, # Sell into any bid
            side="SELL",
            order_type="IOC"
        )
        
    return {
        "status": "success",
        "kalshi_response": k_resp,
        "polymarket_response": p_resp,
        "message": f"Liquidated {k_qty} Kalshi & {p_qty} Poly contracts"
    }


def evaluate_swap_opportunity(
    candidate_arb: dict,
    held_positions: dict,
    total_portfolio_usd: float,
    min_swap_gain_usd: float = DEFAULT_MIN_SWAP_GAIN_USD,
    swap_hurdle_apy: float = DEFAULT_SWAP_HURDLE_APY
) -> Optional[dict]:
    """
    Check if we should sell a held position to buy candidate_arb.
    Returns the pair_id to sell if a swap is advantageous.
    """
    candidate_apy = candidate_arb.get('apy', 0.0)
    
    # 1. Find the "Weakest Link" (Held pair with lowest remaining APY)
    worst_pair_id = None
    worst_apy = 999999.0
    
    for pid, info in held_positions.items():
        if info['value_usd'] < 0.50: # Ignore dust
            continue
            
        h_apy = calculate_holding_apy(info)
        if h_apy < worst_apy:
            worst_apy = h_apy
            worst_pair_id = pid
            
    if not worst_pair_id:
        return None
    
    # 2. Check APY Hurdle: New must be at least X% APY better than old
    if candidate_apy < worst_apy + swap_hurdle_apy:
        return None
        
    # 3. Check Fee/Slippage Adjusted Absolute Gain
    # Loss on liquidation = current_value * SWAP_FEE_CUSHION_PCT
    # Expected profit on new = candidate_arb['gross_profit_usd']
    
    worst_info = held_positions[worst_pair_id]
    liquidation_loss = worst_info['value_usd'] * (SWAP_FEE_CUSHION_PCT / 100.0)
    
    # Absolute gain improvement
    net_gain = candidate_arb['gross_profit_usd'] - liquidation_loss
    
    if net_gain < min_swap_gain_usd:
        return None
        
    return {
        "sell_pair_id": worst_pair_id,
        "sell_value_usd": worst_info['value_usd'],
        "net_gain_usd": net_gain,
        "apy_improvement": candidate_apy - worst_apy,
        "worst_apy": worst_apy
    }

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


def size_trade_to_available_balances(arb: dict, held_positions: Optional[dict] = None) -> dict:
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

    close_time = normalize_str(pair_row.get("close_time", ""))
    for opt in options:
        opt["pair_id"] = pair_id
        opt["close_time"] = close_time
        opt["apy"] = calculate_apy(opt["profit_pct"], close_time)

    # Prefer highest weighted executable APY
    best = max(
        options,
        key=lambda x: (
            x["apy"],
            x["profit_pct"],
            x["gross_profit_usd"],
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

def place_dual_orders(
    pair_row: pd.Series, 
    arb: dict, 
    total_portfolio_usd: float = 0.0,
    current_pair_value_usd: float = 0.0
) -> dict:
    """
    Place both legs, but first shrink size to available balances on both venues.
    Also respects concentration limits.
    """
    kalshi_ticker = normalize_str(pair_row["kalshi_ticker"])
    polymarket_slug = normalize_str(pair_row["polymarket_ticker"])

    # Concentration check
    max_alloc_pct = DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR
    if total_portfolio_usd > 10.0: # Only enforce if portfolio has some weight
        potential_total = current_pair_value_usd + arb['notional_usd']
        if potential_total > (total_portfolio_usd * max_alloc_pct):
            # Recalculate allowed additional notional
            allowed_additional = max(0.0, (total_portfolio_usd * max_alloc_pct) - current_pair_value_usd)
            if allowed_additional < 1.0: # Too small
                 return {"status": "skipped", "message": f"Concentration limit reached: Pair already occupies {current_pair_value_usd/total_portfolio_usd:.1%} of portfolio"}
            
            # Shrink arb to allowed_additional
            shrink_factor = allowed_additional / arb['notional_usd']
            arb['contracts'] *= shrink_factor
            arb['notional_usd'] = arb['contracts'] * arb['sum_price']
            arb['gross_profit_usd'] = arb['contracts'] * (1.0 - arb['sum_price'])

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

def get_candidate_for_pair(
    pair_row: pd.Series,
    min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT,
    min_liquidity_usd: float = DEFAULT_MIN_LIQUIDITY_USD,
) -> Optional[dict]:
    pair_id = normalize_str(
        pair_row.get(
            "pair_id",
            f"{pair_row['kalshi_ticker']}__{pair_row['polymarket_ticker']}"
        )
    )

    active = truthy(pair_row.get("active", "true"))
    if not active:
        return None

    try:
        arb = choose_best_arb_for_pair(pair_row)

        if not arb.get("found"):
            return None

        if arb["profit_pct"] < min_profit_pct:
            return None

        if arb["notional_usd"] < min_liquidity_usd:
            return None

        # Add additional metadata for execution
        arb["pair_row"] = pair_row
        return arb

    except Exception as e:
        print(f"[{pair_id}] Error finding candidate: {e}")
        return None


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

    # 1. Fetch current exposure & cash
    exposure = get_current_exposure(df)
    total_portfolio_usd = exposure['total_portfolio_usd']
    
    # We fetch available cash to see if we can buy without selling
    kalshi_cash = get_kalshi_available_usd()
    poly_cash = get_polymarket_available_usd()
    total_cash = kalshi_cash + poly_cash
    
    print(f"ACCOUNT STATE: Kalshi=${kalshi_cash:.2f}, Poly=${poly_cash:.2f} | Positions=${total_portfolio_usd:.2f}")
    total_nav = total_portfolio_usd + total_cash

    # 2. Collect and rank all candidates
    candidates = []
    for _, row in df.iterrows():
        cand = get_candidate_for_pair(row, min_profit_pct, min_liquidity_usd)
        if cand:
            candidates.append(cand)
            
    if not candidates:
        print("No viable arbitrage opportunities found at this time.")
        return

    # Sort candidates by APY (highest first)
    candidates.sort(key=lambda x: x['apy'], reverse=True)
    best_cand = candidates[0]
    pair_id = best_cand['pair_id']
    
    print(f"BEST CANDIDATE: [{pair_id}] Profit={best_cand['profit_pct']:.2f}% | APY={best_cand['apy']:.1f}%")

    # 3. Decision Logic: Buy vs Swap
    # If we have at least $5 cash on both sides, try to just BUY
    if kalshi_cash >= 2.5 and poly_cash >= 2.5:
        print(f"[{pair_id}] Attempting direct BUY...")
        
        target_arb = best_cand
        if reverify_books:
            live = reverify_pair_live(best_cand['pair_row'], best_cand)
            if not live.get('found'):
                print(f"[{pair_id}] Reverification failed. Skipping.")
                return
            target_arb = live
            # Re-calculate APY for live data
            target_arb['apy'] = calculate_apy(target_arb['profit_pct'], target_arb.get('close_time', ''))

        if dry_run:
            print(f"[{pair_id}] Dry run: Would place orders.")
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "dry_run_buy",
                "apy": target_arb['apy'],
                "message": "Candidate found for direct buy"
            })
            return

        result = place_dual_orders(
            target_arb['pair_row'], 
            target_arb,
            total_portfolio_usd=total_nav,
            current_pair_value_usd=exposure['pair_exposure'].get(pair_id, {}).get('value_usd', 0.0)
        )
        
        append_execution_log({
            "timestamp": utc_now_iso(),
            "pair_id": pair_id,
            "status": result["status"],
            "kalshi_ticker": target_arb["kalshi_ticker"],
            "polymarket_ticker": target_arb["polymarket_ticker"],
            "kalshi_side": target_arb["kalshi_side"],
            "polymarket_outcome": target_arb["polymarket_outcome"],
            "kalshi_price": result.get("kalshi_price"),
            "polymarket_price": result.get("polymarket_price"),
            "profit_pct": result.get("profit_pct"),
            "apy": target_arb['apy'],
            "contracts": result.get("contracts"),
            "message": f"Direct buy: {result.get('message', '')}",
        })
        return

    # 4. Swap Evaluation
    print("Low cash - evaluating for advantageous swaps...")
    swap = evaluate_swap_opportunity(best_cand, exposure['pair_exposure'], total_nav)
    
    if swap:
        sell_id = swap['sell_pair_id']
        msg = f"SWAP: Sell {sell_id} ({swap['worst_apy']:.1f}% APY) -> Buy {pair_id} ({best_cand['apy']:.1f}% APY) | Net Gain: ${swap['net_gain_usd']:.2f}"
        print(msg)
        
        if dry_run:
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": pair_id,
                "status": "dry_run_swap",
                "message": msg
            })
            return

        # 4a. Liquidate
        sell_res = liquidate_pair(sell_id, exposure['pair_exposure'], dry_run=False)
        print(f"Liquidation result: {sell_res['message']}")
        
        if sell_res['status'] != 'success':
            print("Liquidation failed. Aborting swap.")
            return
            
        # 4b. Re-verify buy leg after liquidation
        # Note: Balance might take seconds to update, but most exchange balances update instantly upon fill
        target_arb = best_cand
        if reverify_books:
            live = reverify_pair_live(best_cand['pair_row'], best_cand)
            if not live.get('found'):
                print(f"[{pair_id}] Post-liquidation reverify failed.")
                return
            target_arb = live

        # 4c. Execute Buy
        result = place_dual_orders(
            target_arb['pair_row'], 
            target_arb,
            total_portfolio_usd=total_nav,
            current_pair_value_usd=0.0 # Just liquidated or starting fresh
        )
        
        append_execution_log({
            "timestamp": utc_now_iso(),
            "pair_id": pair_id,
            "status": f"swap_{result['status']}",
            "message": f"Swap from {sell_id} | {result.get('message', '')}",
            "apy": target_arb.get('apy')
        })
    else:
        print("No advantageous swaps found (or cash too low and no swap hurdle cleared).")


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
