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
from notifications.telegram_bot import send_telegram_message

# =========================================================
# Configuration
# =========================================================

TRACKED_PAIRS_CSV = os.path.join(PROJECT_ROOT, "Data", "tracked_pairs.csv")
EXECUTION_LOG_CSV = os.path.join(PROJECT_ROOT, "Data", "portfolio_arb_execution_log.csv")

DEFAULT_MAX_TRADE_USD = 20.0 # <<< should probably be increased.
DEFAULT_MIN_PROFIT_PCT = 8.0
DEFAULT_MIN_LIQUIDITY_USD = 5.0
DEFAULT_SLEEP_MINUTES = 30

# Fee cushions can be used as conservative safety margins
KALSHI_FEE_BUFFER = 0.0
POLY_FEE_BUFFER = 0.0

# Reverify against live books again immediately before sending orders
DEFAULT_REVERIFY_BOOKS = True

# Performance & Swap Configuration
DEFAULT_MIN_SWAP_GAIN_USD = 0.05    # Marginal safety floor
DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR = 0.50 # 50% limit per pair for diversification
SWAP_FEE_CUSHION_PCT = 1.0         # 1% buffer for fees/slippage when calculating swap feasibility
DEFAULT_MIN_SWAP_APY_DELTA = 15.0  # Only swap if new APY is 15% better (hurdle)
HANGING_LEG_REBALANCE_MAX_COST = 1.00 # Allow completing a hedge even if it costs $1.02 total (2% insurance loss)
SLIPPAGE_PROTECTION_FLOOR_PCT = 5.0 # Notify/Prevent liquidation if best bid is >10% below cost basis

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
        # If get_kalshi_positions returns empty but there was no exception, 
        # it might be valid or it might have failed silently.
        # We'll assume success if no exception was raised, 
        # but the API helper should ideally return None on true failure.
    except Exception as e:
        print(f"Error fetching Kalshi positions: {e}")
        return {"success": False, "error": f"Kalshi position fetch failed: {e}"}
    
    try:
        p_pos = get_polymarket_positions()
    except Exception as e:
        print(f"Error fetching Polymarket positions: {e}")
        return {"success": False, "error": f"Polymarket position fetch failed: {e}"}

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
            "avg_price_kalshi": 0.0,
            "contracts_poly": 0,
            "polymarket_side": "",
            "avg_price_poly": 0.0,
            "value_usd": 0.0,
            "kalshi_ticker": kt,
            "polymarket_ticker": pt,
            "close_time": normalize_str(row.get('close_time', ""))
        }

    total_val = 0.0
    k_mapped = 0
    p_mapped = 0
    
    for pos in k_pos:
        tic = normalize_str(pos['ticker'])
        if tic in k_to_pair:
            pid = k_to_pair[tic]
            exposure[pid]["contracts_kalshi"] += pos['quantity']
            exposure[pid]["kalshi_side"] = pos['side'].lower()
            exposure[pid]["avg_price_kalshi"] = safe_float(pos.get('avg_price_cents', 0)) / 100.0
            k_val = safe_float(pos.get('market_exposure_cents', 0)) / 100.0
            exposure[pid]["value_usd"] += k_val
            total_val += k_val
            k_mapped += 1
        else:
            if pos['quantity'] > 0:
                print(f"  [!] Unmapped Kalshi position: {pos['ticker']} ({pos['quantity']} units)")

    for pos in p_pos:
        tic = normalize_str(pos['market_id'])
        if tic in p_to_pair:
            pid = p_to_pair[tic]
            exposure[pid]["contracts_poly"] += pos['size']
            exposure[pid]["polymarket_side"] = pos['side'].upper()
            exposure[pid]["avg_price_poly"] = safe_float(pos.get('avg_price', 0))
            p_val = safe_float(pos.get('current_value', 0))
            exposure[pid]["value_usd"] += p_val
            total_val += p_val
            p_mapped += 1
        else:
            if pos['size'] > 0.01:
                print(f"  [!] Unmapped Polymarket position: {pos['market_id']} ({pos['size']} units)")
            
    print(f"Mapped {k_mapped}/{len(k_pos)} Kalshi and {p_mapped}/{len(p_pos)} Polymarket positions.")
    
    return {
        "success": True, # Assume success if we reached here
        "total_portfolio_usd": total_val,
        "pair_exposure": exposure
    }


def calculate_holding_apy(exposure_info: dict) -> float:
    """
    Calculate the APY we get if we hold the CURRENT position to maturity.
    Payout is 1.0 per contract. Cost is current 'value_usd'.
    """
    val = exposure_info['value_usd']
    if val <= 0:
        return 0.0
        
    # We use the SMALLER quantity of the two legs as the 'balanced' arb quantity
    # Any excess is a 'hanging leg' and doesn't contribute to the arb payout logic here
    contracts = min(exposure_info['contracts_kalshi'], exposure_info['contracts_poly'])
    
    if contracts <= 0:
        return 0.0
        
    profit_pct = (contracts / val - 1.0) * 100.0
    return calculate_apy(profit_pct, exposure_info['close_time'])


def liquidate_pair(
    pair_id: str, 
    exposure: dict, 
    k_target_qty: int,
    p_target_qty: int,
    k_min_price_cents: int,
    p_min_price: float,
    dry_run: bool = True
) -> dict:
    """
    Sell verified quantities of held contracts for a pair.
    Uses limit orders at the Best Bid to minimize slippage.
    """
    info = exposure.get(pair_id)
    if not info:
        return {"status": "error", "message": "Pair not found in exposure"}
    
    if k_target_qty <= 0 and p_target_qty <= 0:
        return {"status": "skipped", "message": "No contracts to liquidate"}
        
    if dry_run:
        return {"status": "dry_run", "message": f"Would liquidate {k_target_qty} Kalshi (min {k_min_price_cents}c) and {p_target_qty} Poly (min ${p_min_price})"}

    # Kalshi Sell
    k_resp = None
    if k_target_qty > 0:
        # Fetch actual best bid to use as limit
        books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        if books is None:
             return {"status": "error", "message": "Kalshi book fetch failed during liquidation"}
        
        bids = books['yes_bids'] if info['kalshi_side'] == 'yes' else books['no_bids']
        
        if not bids:
             return {"status": "error", "message": "No bids found on Kalshi to sell into"}
        
        best_bid_cents = int(round(bids[0]['price'] * 100))
        if best_bid_cents < k_min_price_cents:
             return {"status": "error", "message": f"Kalshi bid {best_bid_cents}c fell below min {k_min_price_cents}c"}

        k_resp = kalshi_place_limit_order(
            ticker=info['kalshi_ticker'],
            side=info['kalshi_side'],
            action="sell",
            count=k_target_qty,
            price_cents=best_bid_cents, 
            time_in_force="immediate_or_cancel"
        )

    # Poly Sell
    p_resp = None
    if p_target_qty > 0:
        books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        if books is None:
             return {"status": "error", "message": "Polymarket book fetch failed during liquidation"}
             
        bids = books['sell_levels']
        
        if not bids:
            return {"status": "error", "message": "No bids found on Polymarket to sell into"}
            
        best_bid = bids[0]['price']
        if best_bid < p_min_price:
             return {"status": "error", "message": f"Poly bid ${best_bid} fell below min ${p_min_price}"}

        p_resp = polymarket_place_limit_order(
            slug=info['polymarket_ticker'],
            outcome=info['polymarket_side'],
            size=p_target_qty,
            price=best_bid, 
            side="SELL",
            order_type="IOC"
        )
        
    return {
        "status": "success",
        "kalshi_response": k_resp,
        "polymarket_response": p_resp,
        "message": f"Liquidated {k_target_qty} Kalshi & {p_target_qty} Poly contracts"
    }


def evaluate_swap_opportunity(
    candidate_arb: dict,
    held_positions: dict,
    total_portfolio_usd: float,
    min_swap_gain_usd: float = DEFAULT_MIN_SWAP_GAIN_USD,
    swap_hurdle_apy: float = DEFAULT_MIN_SWAP_APY_DELTA
) -> Optional[dict]:
    """
    Check if we should sell a held position to buy candidate_arb.
    Returns the liquidation plan (sell_pair_id, qty, prices) if a swap is advantageous.
    """
    candidate_apy = candidate_arb.get('apy', 0.0)
    
    # 1. Find the "Weakest Link" (Held pair with lowest remaining APY)
    worst_pair_id = None
    worst_apy = 999999.0
    best_liquidation_plan = {}
    
    for pid, info in held_positions.items():
        if info['value_usd'] < 0.50: # Ignore dust
            continue
            
        # LIVE DEPTH CHECK: How much can we actually sell at a profitable price?
        k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        
        if k_books is None or p_books is None:
            continue
            
        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        p_bids = p_books['sell_levels']
        
        if not k_bids or not p_bids:
            continue
            
        # Cost Basis for No-Loss Check
        avg_cost_basis = info['avg_price_kalshi'] + info['avg_price_poly']
        
        # Find executable qty that stays above cost basis (to avoid realized loss on liquidation)
        k_depth = get_executable_depth(k_bids, info['avg_price_kalshi'] - 0.01) # liberal depth check
        p_depth = get_executable_depth(p_bids, info['avg_price_poly'] - 0.01)
        
        liquidable_qty = int(min(k_depth, p_depth, info['contracts_kalshi'], info['contracts_poly']))
        
        if liquidable_qty <= 0:
            continue
            
        # Weighted average exit price for this quantity
        avg_k_exit = calculate_average_fill_price(k_bids, liquidable_qty)
        avg_p_exit = calculate_average_fill_price(p_bids, liquidable_qty)
        current_unit_value = avg_k_exit + avg_p_exit
        
        # [REMOVED STRICT COST BASIS FLOOR] 
        # Per user feedback, we allow selling at a loss if the total swap result is positive.

        # Remaining APY if we HOLD
        h_apy = calculate_holding_apy({
            'value_usd': liquidable_qty * current_unit_value,
            'contracts_kalshi': liquidable_qty,
            'contracts_poly': liquidable_qty,
            'close_time': info['close_time']
        })
        
        # PRIORITY LOCK: If a position is > 0.98, it's a high-priority liquidation target
        if current_unit_value >= 0.99:
            h_apy = -10.0 
        elif current_unit_value >= 0.98:
            h_apy = -1.0
            
        if h_apy < worst_apy:
            worst_apy = h_apy
            worst_pair_id = pid
            best_liquidation_plan = {
                "pair_id": pid,
                "qty": liquidable_qty,
                "k_price_cents": int(round(avg_k_exit * 100)),
                "p_price": avg_p_exit,
                "value_usd": liquidable_qty * current_unit_value,
                "cost_basis_total": liquidable_qty * avg_cost_basis
            }
            
    if not worst_pair_id:
        return None
    
    # 2. Check APY Hurdle: New must be at least X% APY better than old
    if candidate_apy < worst_apy + swap_hurdle_apy:
        return None
        
    # 3. Check Fee/Slippage Adjusted Absolute Gain
    # We want to ensure the SWAP as a whole is profitable.
    # Gain = (Profit from New Entry) - (Realized Loss/Fee from Exit)
    
    liquidation_notional = best_liquidation_plan['value_usd']
    liquidation_cost = best_liquidation_plan['cost_basis_total']
    
    exit_pnl = liquidation_notional - liquidation_cost
    
    # Entrance calculation: how many contracts of the candidate can we buy with liquidation_notional?
    new_contracts = liquidation_notional / candidate_arb['sum_price']
    new_expected_profit = new_contracts * (1.0 - candidate_arb['sum_price'])
    
    # Total Swap PnL = Exit PnL + New Expected Profit
    total_swap_pnl = exit_pnl + new_expected_profit
    
    # We also subtract a cushion for slippage/fees during execution
    execution_buffer = liquidation_notional * (SWAP_FEE_CUSHION_PCT / 100.0)
    net_swap_gain = total_swap_pnl - execution_buffer
    
    if net_swap_gain < min_swap_gain_usd:
        print(f"Swap rejected: Net gain ${net_swap_gain:.2f} < min ${min_swap_gain_usd:.2f}")
        return None
        
    return {
        "sell_pair_id": worst_pair_id,
        "sell_qty": best_liquidation_plan['qty'],
        "sell_k_price_cents": best_liquidation_plan['k_price_cents'],
        "sell_p_price": best_liquidation_plan['p_price'],
        "sell_value_usd": liquidation_notional,
        "net_gain_usd": net_swap_gain,
        "apy_improvement": candidate_apy - worst_apy,
        "worst_apy": worst_apy
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

def get_yes_no_books_kalshi(ticker: str) -> Optional[dict]:
    raw = get_kalshi_orderbook(ticker)
    if raw is None:
        return None

    # ASKS (We buy from these)
    yes_asks = normalize_book_side(raw.get("yes", {}).get("asks", []))
    no_asks = normalize_book_side(raw.get("no", {}).get("asks", []))

    # BIDS (We sell into these)
    yes_bids = normalize_book_side(raw.get("yes", {}).get("bids", []))
    no_bids = normalize_book_side(raw.get("no", {}).get("bids", []))

    return {
        "yes_asks": yes_asks,
        "no_asks": no_asks,
        "yes_bids": yes_bids,
        "no_bids": no_bids,
        "raw": raw,
    }


def get_outcome_books_polymarket(slug: str, outcome_name: str) -> Optional[dict]:
    raw = get_polymarket_orderbook(slug)
    if raw is None:
        return None
        
    outcome_key = normalize_str(outcome_name).lower()  # YES -> yes, NO -> no
    
    buy_levels = normalize_book_side(raw.get(outcome_key, {}).get("asks", []))   # Asks
    sell_levels = normalize_book_side(raw.get(outcome_key, {}).get("bids", []))  # Bids

    return {
        "buy_levels": buy_levels,
        "sell_levels": sell_levels,
        "raw": raw,
    }


# =========================================================
# Liquidation Helpers
# =========================================================

def get_executable_depth(levels: List[Dict[str, float]], floor_price: float) -> float:
    """
    Returns the total quantity available in the book at or above the floor_price.
    Assumes levels are sorted by price (best bids first).
    """
    total_qty = 0.0
    for lvl in levels:
        if lvl["price"] >= floor_price:
            total_qty += lvl["size"]
        else:
            break
    return total_qty


def calculate_average_fill_price(levels: List[Dict[str, float]], quantity: float) -> float:
    """
    Calculate the weighted average price for a given quantity.
    """
    if quantity <= 0:
        return 0.0
    
    remaining = quantity
    total_val = 0.0
    for lvl in levels:
        fill = min(remaining, lvl["size"])
        total_val += fill * lvl["price"]
        remaining -= fill
        if remaining <= 1e-12:
            break
    
    return total_val / (quantity - remaining) if (quantity - remaining) > 0 else 0.0


# =========================================================
# Hanging Leg Protector
# =========================================================

# =========================================================
# Maturity & Exit Logic
# =========================================================

def check_and_exit_matured_positions(exposure: dict, dry_run: bool = True) -> List[dict]:
    """
    Scan all held positions. If combined bid >= 0.9995, liquidate.
    Only liquidates the quantity available at or above that price level.
    """
    results = []
    for pid, info in exposure['pair_exposure'].items():
        if info['value_usd'] < 0.20: # Ignore dust
            continue
            
        print(f"[{pid}] Checking for maturity exit...")
        
        # Fetch live bids
        k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        
        if k_books is None or p_books is None:
            continue
            
        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        p_bids = p_books['sell_levels']
        
        if not k_bids or not p_bids:
            continue
            
        # Target: Combined bid >= 0.9995
        # We walk both books to see how many contracts we can dump at >= 0.9995 total
        # Simplified: Check top of book first for speed
        best_k_bid = k_bids[0]['price']
        best_p_bid = p_bids[0]['price']
        combined = best_k_bid + best_p_bid
        
        if combined >= 0.9995:
            # We found a maturity exit!
            # Determine max qty we can sell at this combined price or better
            # We use the min depth of both best bids
            qty_to_sell = min(k_bids[0]['size'], p_bids[0]['size'], info['contracts_kalshi'], info['contracts_poly'])
            qty_to_sell = int(math.floor(qty_to_sell))
            
            if qty_to_sell <= 0:
                continue
                
            print(f"[{pid}] MATURITY DETECTED: Combined Bid {combined:.4f} >= 0.9995. Selling {qty_to_sell} units.")
            
            res = liquidate_pair(
                pid, 
                exposure['pair_exposure'], 
                k_target_qty=qty_to_sell,
                p_target_qty=qty_to_sell,
                k_min_price_cents=int(round(best_k_bid * 100)),
                p_min_price=best_p_bid,
                dry_run=dry_run
            )
            if not dry_run and res.get('status') == 'success':
                 send_telegram_message(f"✅ <b>Maturity Exit</b>: {pid}\nQuantity: {qty_to_sell}\nCombined Bid: {combined:.4f}")

            res['pair_id'] = pid
            res['exit_type'] = "maturity"
            results.append(res)
            
    return results


def find_unbalanced_pairs(exposure: dict) -> List[dict]:
    """
    Search for pairs where the number of contracts on Kalshi and Poly differ by > 1.
    """
    unbalanced = []
    for pid, info in exposure['pair_exposure'].items():
        k_qty = info['contracts_kalshi']
        p_qty = info['contracts_poly']
        
        if abs(k_qty - p_qty) > 1.0:
            unbalanced.append({
                "pair_id": pid,
                "k_qty": k_qty,
                "p_qty": p_qty,
                "diff": k_qty - p_qty,
                "info": info
            })
    return unbalanced


def resolve_hanging_leg(unbalanced_info: dict, dry_run: bool = True) -> dict:
    """
    Tiered logic for fixing hanging legs:
    1. Profit Finish: buy missing leg if total < $1.00
    2. No-loss Liquidate: sell lonely leg if bid >= purchase price
    3. Breakeven Finish: buy missing leg if total <= HANGING_LEG_REBALANCE_MAX_COST
    4. Emergency Liquidate: sell lonely leg if loss <= SLIPPAGE_PROTECTION_FLOOR_PCT
    5. High-Priority Alert: keep position and alert user
    """
    pid = unbalanced_info['pair_id']
    info = unbalanced_info['info']
    diff = unbalanced_info['diff'] # >0 means k_qty > p_qty (Poly leg is missing)
    
    missing_venue = "Poly" if diff > 0 else "Kalshi"
    qty_to_fix = int(abs(diff))
    
    print(f"[{pid}] Hanging leg detected: {missing_venue} is missing {qty_to_fix} units.")
    
    if dry_run:
        return {"status": "dry_run", "message": f"Would attempt to fix {missing_venue} leg for {pid} using tiered logic"}

    # Fetch books for both venues to check hedge cost AND liquidation price
    k_books = get_yes_no_books_kalshi(info['kalshi_ticker'])
    p_books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
    
    if k_books is None or p_books is None:
        return {"status": "error", "message": "Failed to fetch books for tiered hanging leg resolution"}
        
    if missing_venue == "Poly":
        held_venue = "Kalshi"
        held_cost = info['avg_price_kalshi']
        # Missing side is Poly (Check Asks to buy)
        poly_asks = p_books['buy_levels']
        if not poly_asks:
             return {"status": "error", "message": "No liquidity to fix Poly leg"}
        current_ask = poly_asks[0]['price']
        
        # Held side is Kalshi (Check Bids to sell)
        k_bids = k_books['yes_bids'] if info['kalshi_side'] == 'yes' else k_books['no_bids']
        if not k_bids:
             return {"status": "error", "message": "No liquidity to liquidate Kalshi leg"}
        current_bid = k_bids[0]['price']
    else:
        held_venue = "Poly"
        held_cost = info['avg_price_poly']
        # Missing side is Kalshi (Check Asks to buy)
        k_asks = k_books['yes_asks'] if info['kalshi_side'] == 'yes' else k_books['no_asks']
        if not k_asks:
             return {"status": "error", "message": "No liquidity to fix Kalshi leg"}
        current_ask = k_asks[0]['price']
        
        # Held side is Poly (Check Bids to sell)
        poly_bids = p_books['sell_levels']
        if not poly_bids:
             return {"status": "error", "message": "No liquidity to liquidate Polymarket leg"}
        current_bid = poly_bids[0]['price']

    total_unit_cost = held_cost + current_ask
    
    # --- Tier 1: Profitable Hedge ---
    if total_unit_cost < 1.00:
        print(f"[{pid}] Tier 1: Profitable Hedge found. Total cost: ${total_unit_cost:.4f}")
        if missing_venue == "Poly":
            resp = polymarket_place_limit_order(slug=info['polymarket_ticker'], outcome=info['polymarket_side'], size=qty_to_fix, price=current_ask, side="BUY", order_type="IOC")
        else:
            resp = kalshi_place_limit_order(ticker=info['kalshi_ticker'], side=info['kalshi_side'], action="buy", count=qty_to_fix, price_cents=int(round(current_ask*100)), time_in_force="immediate_or_cancel")
        send_telegram_message(f"💰 <b>Hanging Leg Fixed (PROFIT)</b>: {pid}\nAction: Bought {qty_to_fix} on {missing_venue}\nTotal cost: ${total_unit_cost:.4f}")
        return {"status": "success", "message": f"Profitable hedge executed: {resp}"}

    # --- Tier 2: No-Loss Liquidation ---
    if current_bid >= held_cost:
        print(f"[{pid}] Tier 2: No-Loss Liquidation found. Bid: ${current_bid:.4f} >= Cost: ${held_cost:.4f}")
        if held_venue == "Kalshi":
             res = liquidate_pair(pid, {pid: info}, k_target_qty=qty_to_fix, p_target_qty=0, k_min_price_cents=int(round(held_cost*100)), p_min_price=0.01, dry_run=False)
        else:
             res = liquidate_pair(pid, {pid: info}, k_target_qty=0, p_target_qty=qty_to_fix, k_min_price_cents=1, p_min_price=held_cost, dry_run=False)
        
        if res.get('status') == 'success':
             send_telegram_message(f"♻️ <b>Hanging Leg Liquidated (NO-LOSS)</b>: {pid}\nAction: Sold {qty_to_fix} on {held_venue}\nPrice: ${current_bid:.4f} (Cost: ${held_cost:.4f})")
             return res
             
    # --- Tier 3: Breakeven Hedge ---
    if total_unit_cost <= HANGING_LEG_REBALANCE_MAX_COST:
        print(f"[{pid}] Tier 3: Breakeven Hedge found. Total cost: ${total_unit_cost:.4f}")
        if missing_venue == "Poly":
            resp = polymarket_place_limit_order(slug=info['polymarket_ticker'], outcome=info['polymarket_side'], size=qty_to_fix, price=current_ask, side="BUY", order_type="IOC")
        else:
            resp = kalshi_place_limit_order(ticker=info['kalshi_ticker'], side=info['kalshi_side'], action="buy", count=qty_to_fix, price_cents=int(round(current_ask*100)), time_in_force="immediate_or_cancel")
        send_telegram_message(f"⚡ <b>Hanging Leg Fixed (BREAKEVEN)</b>: {pid}\nAction: Bought {qty_to_fix} on {missing_venue}\nTotal cost: ${total_unit_cost:.4f}")
        return {"status": "success", "message": f"Breakeven hedge executed: {resp}"}

    # --- Tier 4: Emergency Liquidation ---
    floor_pct = SLIPPAGE_PROTECTION_FLOOR_PCT
    min_bid = held_cost * (1 - floor_pct / 100.0)
    if current_bid >= min_bid:
        loss_pct = (1 - current_bid / held_cost) * 100.0
        print(f"[{pid}] Tier 4: Emergency Liquidation. Bid: ${current_bid:.4f} vs Min: ${min_bid:.4f} (Loss: {loss_pct:.1f}%)")
        if held_venue == "Kalshi":
             res = liquidate_pair(pid, {pid: info}, k_target_qty=qty_to_fix, p_target_qty=0, k_min_price_cents=int(round(current_bid*100)), p_min_price=0.01, dry_run=False)
        else:
             res = liquidate_pair(pid, {pid: info}, k_target_qty=0, p_target_qty=qty_to_fix, k_min_price_cents=1, p_min_price=current_bid, dry_run=False)

        if res.get('status') == 'success':
             send_telegram_message(f"⚠️ <b>Hanging Leg EMERGENCY LIQUIDATION</b>: {pid}\nAction: Sold {qty_to_fix} on {held_venue}\nPrice: ${current_bid:.4f} (Loss: {loss_pct:.1f}%)")
             return res

    # --- Tier 5: Hold & Alert ---
    print(f"[{pid}] Tier 5: Hold & Alert. No safe exit found. Bid: ${current_bid:.4f} < Min: ${min_bid:.4f}")
    send_telegram_message(f"🚨 <b>Hanging Leg CRITICAL ALERT</b>: {pid}\nNo safe exit for lonely {held_venue} leg.\nBest Bid: ${current_bid:.4f} | Minimum Floor: ${min_bid:.4f}\nHolding position to avoid >{floor_pct}% loss.")
    return {"status": "error", "message": f"No safe exit found for {pid}. Best bid ${current_bid:.4f} < ${min_bid:.4f}"}



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

    if kalshi_books is None or poly_no is None or poly_yes is None:
        return {"found": False}

    candidate_yes_no = consume_dual_books(
        kalshi_books["yes_asks"],
        poly_no["buy_levels"],
        max_trade_usd=max_trade_usd,
        fee_buffer_a=KALSHI_FEE_BUFFER,
        fee_buffer_b=POLY_FEE_BUFFER,
    )

    candidate_no_yes = consume_dual_books(
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
    if kalshi_books is None:
         return {"found": False}

    if original_arb["kalshi_side"] == "yes" and original_arb["polymarket_outcome"] == "NO":
        poly_books = get_outcome_books_polymarket(polymarket_slug, "NO")
        if poly_books is None:
             return {"found": False}
             
        live = consume_dual_books(
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
        if poly_books is None:
             return {"found": False}
             
        live = consume_dual_books(
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

    # Detect hanging legs: if one succeeds but the other fails
    # Note: we check for 'error' in response dictionaries
    k_ok = kalshi_resp and (kalshi_resp.get("status") == "success" or "order_id" in str(kalshi_resp))
    p_ok = poly_resp and (poly_resp.get("status") == "success" or "orderID" in str(poly_resp))

    status = "success"
    message = ""
    if not k_ok or not p_ok:
        if k_ok or p_ok:
            status = "partial"
            message = f"Hanging leg detected! Kalshi={'OK' if k_ok else 'FAIL'}, Poly={'OK' if p_ok else 'FAIL'}"
            print(f"!!! {message} !!!")
        else:
            status = "error"
            message = "Both legs failed to execute."

    return {
        "status": status,
        "kalshi_response": kalshi_resp,
        "polymarket_response": poly_resp,
        "contracts": contracts,
        "kalshi_price": arb["price_a"],
        "polymarket_price": arb["price_b"],
        "sum_price": arb["sum_price"],
        "profit_pct": arb["profit_pct"],
        "notional_usd": contracts * arb["sum_price"],
        "gross_profit_usd": contracts * (1.0 - arb["sum_price"]),
        "message": message,
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
    verbose: bool = False
) -> None:
    if not os.path.exists(tracked_pairs_csv):
        raise FileNotFoundError(f"{tracked_pairs_csv} not found")

    df = pd.read_csv(tracked_pairs_csv)
    print(f"Loaded {len(df)} tracked pairs from {tracked_pairs_csv}")

    # 1. Fetch current exposure & cash
    exposure = get_current_exposure(df)
    if not exposure.get("success"):
        print("CRITICAL: Failed to fetch complete exposure data. Aborting this iteration to avoid risk.")
        return

    # =========================================================
    # Phase 0: Hanging Leg Protection
    # =========================================================
    unbalanced = find_unbalanced_pairs(exposure)
    if unbalanced:
        print(f"Found {len(unbalanced)} unbalanced positions. Resolving...")
        for item in unbalanced:
            res = resolve_hanging_leg(item, dry_run=dry_run)
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": item['pair_id'],
                "status": f"fix_{res['status']}",
                "message": res.get("message", "")
            })
        
        # Re-fetch exposure after fixes before proceeding to new arbs
        exposure = get_current_exposure(df)

    # =========================================================
    # Phase 0.5: Maturity Exits (0.9995)
    # =========================================================
    matured = check_and_exit_matured_positions(exposure, dry_run=dry_run)
    if matured:
        print(f"Executed {len(matured)} maturity exits.")
        for res in matured:
            append_execution_log({
                "timestamp": utc_now_iso(),
                "pair_id": res['pair_id'],
                "status": f"exit_{res['status']}",
                "message": f"Maturity exit (0.9995): {res.get('message', '')}"
            })
        # Re-fetch exposure after exits
        exposure = get_current_exposure(df)

    total_portfolio_usd = exposure['total_portfolio_usd']
    
    # We fetch available cash to see if we can buy without selling
    kalshi_cash = get_kalshi_available_usd()
    poly_cash = get_polymarket_available_usd()
    
    # SAFETY: If cash is exactly 0.0, it MIGHT be an API failure. 
    # (Though unlikely to be exactly 0.0 on both, it's safer to check)
    if kalshi_cash == 0.0 and poly_cash == 0.0:
        # Check if balance API actually failed
        test_bal = get_kalshi_balance()
        if not test_bal:
             print("CRITICAL: Kalshi balance fetch failed. Aborting iteration for safety.")
             return

    total_cash = kalshi_cash + poly_cash
    
    print(f"ACCOUNT STATE: Kalshi=${kalshi_cash:.2f}, Poly=${poly_cash:.2f} | Positions=${total_portfolio_usd:.2f}")
    total_nav = total_portfolio_usd + total_cash

    # 2. Collect and rank all candidates
    candidates = []
    for _, row in df.iterrows():
        cand = get_candidate_for_pair(row, min_profit_pct, min_liquidity_usd)
        if cand:
            candidates.append(cand)
        elif verbose:
            # Diagnostics for why a candidate was rejected
            tmp_cand = get_candidate_for_pair(row, min_profit_pct=0.0, min_liquidity_usd=0.0)
            if tmp_cand:
                if tmp_cand['profit_pct'] < min_profit_pct:
                    print(f"  [Diag] {tmp_cand['pair_id']} rejected: Profit {tmp_cand['profit_pct']:.2f}% < hurdle {min_profit_pct}%")
                elif tmp_cand['notional_usd'] < min_liquidity_usd:
                    print(f"  [Diag] {tmp_cand['pair_id']} rejected: Liquidity ${tmp_cand['notional_usd']:.2f} < min ${min_liquidity_usd}")
            
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
        msg = f"SWAP: Sell {swap['sell_qty']} units of {sell_id} ({swap['worst_apy']:.1f}% APY) -> Buy {swap['sell_qty']} units of {pair_id} ({best_cand['apy']:.1f}% APY) | Net Gain: ${swap['net_gain_usd']:.2f}"
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
        sell_res = liquidate_pair(
            sell_id, 
            exposure['pair_exposure'], 
            k_target_qty=swap['sell_qty'],
            p_target_qty=swap['sell_qty'],
            k_min_price_cents=swap['sell_k_price_cents'],
            p_min_price=swap['sell_p_price'],
            dry_run=False
        )
        print(f"Liquidation result: {sell_res['message']}")
        
        if sell_res['status'] != 'success':
            msg = f"Liquidation failed for {sell_id}. Aborting swap."
            print(msg)
            send_telegram_message(f"❌ <b>Swap Failed</b>: {pid}\nReason: Liquidation of {sell_id} failed.\nMessage: {sell_res.get('message')}")
            return
            
        # 4b. Re-verify buy leg after liquidation
        send_telegram_message(f"🔄 <b>Swap Step 1/2 Done</b>: Liquidated {sell_id}.\nNext: Buying {pair_id}...")

        target_arb = best_cand
        target_arb['contracts'] = swap['sell_qty'] # MATCHED REBALANCING
        target_arb['notional_usd'] = target_arb['contracts'] * target_arb['sum_price']
        target_arb['gross_profit_usd'] = target_arb['contracts'] * (1.0 - target_arb['sum_price'])

        if reverify_books:
            live = reverify_pair_live(best_cand['pair_row'], best_cand)
            if not live.get('found'):
                msg = f"[{pair_id}] Post-liquidation reverify failed. Staying in cash to avoid double fees."
                print(msg)
                send_telegram_message(f"⚠️ <b>Swap Step 2/2 FAILED</b>: {pair_id} books moved post-liquidation.\nStaying in cash to avoid re-buying fees.")
                return
            
            # Use live price but keep matched size
            live['contracts'] = swap['sell_qty']
            live['notional_usd'] = live['contracts'] * live['sum_price']
            live['gross_profit_usd'] = live['contracts'] * (1.0 - live['sum_price'])
            target_arb = live

        # 4c. Execute Buy
        result = place_dual_orders(
            target_arb['pair_row'], 
            target_arb,
            total_portfolio_usd=total_nav,
            current_pair_value_usd=0.0 # Just liquidated or starting fresh
        )
        
        if result['status'] == 'success':
             send_telegram_message(f"✅ <b>Swap COMPLETE</b>: {sell_id} -> {pair_id}\nQuantity: {result['contracts']}\nAPY Improvement: {swap['apy_improvement']:.1f}%")
        else:
             send_telegram_message(f"⚠️ <b>Swap Step 2/2 PARTIAL/FAIL</b>: {pair_id}\nStatus: {result['status']}\nMessage: {result.get('message')}\nInvestigate hanging legs!")

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
    parser.add_argument("--cooldown", type=int, default=30, help="Wait N seconds between cycles")
    parser.add_argument("--verbose", action="store_true", help="Print detailed diagnostic rejection reasons")
    parser.add_argument("--no-reverify", action="store_true", help="Skip last-second live book reverification")
    args = parser.parse_args()

    dry_run = not args.live
    reverify_books = not args.no_reverify

    if not args.loop:
        run_once(
            args.input, 
            dry_run=not args.live, 
            min_profit_pct=args.min_profit_pct,
            min_liquidity_usd=args.min_liquidity_usd,
            reverify_books=not args.no_reverify,
            verbose=args.verbose
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
