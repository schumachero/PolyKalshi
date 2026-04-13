import os
import sys
import time
import math
import argparse
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

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

try:
    from apis.portfolio import (
        get_kalshi_balance, 
        get_polymarket_balance, 
        get_kalshi_positions, 
        get_polymarket_positions
    )
except ImportError:
    from apis.portfolio import (
        get_kalshi_balance, 
        get_polymarket_balance, 
        get_kalshi_positions, 
        get_polymarket_positions
    )

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
LOCKS_DIR = os.path.join(PROJECT_ROOT, "Data", "locks")

DEFAULT_MAX_TRADE_USD = 20.0 
DEFAULT_MIN_PROFIT_PCT = 8.0
DEFAULT_MIN_LIQUIDITY_USD = 5.0
DEFAULT_SLEEP_MINUTES = 30

KALSHI_FEE_BUFFER = 0.0
POLY_FEE_BUFFER = 0.0
DEFAULT_REVERIFY_BOOKS = True

DEFAULT_MIN_SWAP_GAIN_USD = 0.05    
DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR = 0.50 
SWAP_FEE_CUSHION_PCT = 1.0         
DEFAULT_MIN_SWAP_APY_DELTA = 15.0  
HANGING_LEG_REBALANCE_MAX_COST = 1.00 
SLIPPAGE_PROTECTION_FLOOR_PCT = 5.0 

BALANCE_BUFFER_USD = 1.00

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
        if 'T' in dt_str:
            ds = dt_str.replace('Z', '+00:00')
            return datetime.fromisoformat(ds)
        else:
            return datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def calculate_apy(profit_pct: float, close_time_str: str) -> float:
    close_dt = parse_date(close_time_str)
    if not close_dt:
        return 0.0
    now = datetime.now(timezone.utc)
    delta = close_dt - now
    days = max(delta.total_seconds() / 86400.0, 0.1) 
    return (profit_pct * 365.0) / days

def calculate_holding_apy(exposure_info: dict) -> float:
    """
    Calculate the APY we get if we hold the CURRENT position to maturity.
    Payout is 1.0 per contract. Cost is current 'value_usd'.
    """
    val = exposure_info['value_usd']
    if val <= 0:
        return 0.0
    contracts = min(exposure_info['contracts_kalshi'], exposure_info['contracts_poly'])
    if contracts <= 0:
        return 0.0
    profit_pct = (contracts / val - 1.0) * 100.0
    return calculate_apy(profit_pct, exposure_info['close_time'])

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

def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)

def append_execution_log(row: dict) -> None:
    ensure_parent_dir(EXECUTION_LOG_CSV)
    df = pd.DataFrame([row])
    write_header = not os.path.exists(EXECUTION_LOG_CSV)
    
    # Atomic file append using a simple lock
    lock_path = EXECUTION_LOG_CSV + ".lock"
    import fcntl
    try:
        with open(lock_path, "w") as lock_f:
            fcntl.flock(lock_f, fcntl.LOCK_EX)
            df.to_csv(EXECUTION_LOG_CSV, mode="a", header=write_header, index=False)
            fcntl.flock(lock_f, fcntl.LOCK_UN)
    except Exception as e:
        print(f"Log lock error: {e}. Attempting direct write.")
        df.to_csv(EXECUTION_LOG_CSV, mode="a", header=write_header, index=False)

# =========================================================
# Concurrency / Locking
# =========================================================

class PairLock:
    """Simple file-based lock to prevent race conditions across processes."""
    def __init__(self, pair_id: str, timeout_sec: int = 30):
        self.pid = pair_id
        os.makedirs(LOCKS_DIR, exist_ok=True)
        self.lock_file = os.path.join(LOCKS_DIR, f"{pair_id}.lock")
        self.timeout = timeout_sec
        self.locked = False

    def __enter__(self):
        start = time.time()
        while time.time() - start < self.timeout:
            try:
                # Exclusive creation - fails if exists
                fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                self.locked = True
                return True
            except FileExistsError:
                # Check for stale lock (older than 60s)
                try:
                    mtime = os.path.getmtime(self.lock_file)
                    if time.time() - mtime > 60:
                        os.remove(self.lock_file)
                        print(f"Removed stale lock for {self.pid}")
                except: pass
                time.sleep(0.5)
        print(f"Lock timeout for {self.pid}")
        return False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked and os.path.exists(self.lock_file):
            os.remove(self.lock_file)

# =========================================================
# Book Depth / Math Helpers
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
# Shared State Class
# =========================================================

class MarketState:
    def __init__(self, tracked_pairs_df: pd.DataFrame):
        self.df = tracked_pairs_df
        self.exposure = {}
        self.total_portfolio_usd = 0.0
        self.ks_cash = 0.0
        self.pm_cash = 0.0
        self.total_nav = 0.0
        self.success = False

    def refresh(self):
        print("Refreshing market state (exposure + balances)...")
        exp_res = get_current_exposure(self.df)
        if not exp_res.get("success"):
            self.success = False
            return False

        self.exposure = exp_res["pair_exposure"]
        self.total_portfolio_usd = exp_res["total_portfolio_usd"]
        
        self.ks_cash = get_kalshi_available_usd()
        self.pm_cash = get_polymarket_available_usd()
        
        # Safety check for fail
        if self.ks_cash == 0.0 and self.pm_cash == 0.0:
            test = get_kalshi_balance()
            if not test:
                 print("CRIITICAL: Balance fetch failed.")
                 self.success = False
                 return False

        self.total_nav = self.total_portfolio_usd + self.ks_cash + self.pm_cash
        self.success = True
        print(f"State updated: Nav=${self.total_nav:.2f} (Cash=${self.ks_cash+self.pm_cash:.2f}, Pos=${self.total_portfolio_usd:.2f})")
        return True

# =========================================================
# Orderbook fetch helpers
# =========================================================

def normalize_book_side(levels) -> List[Dict[str, float]]:
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

def get_yes_no_books_kalshi(ticker: str) -> Optional[dict]:
    raw = get_kalshi_orderbook(ticker)
    if raw is None: return None
    return {
        "yes_asks": normalize_book_side(raw.get("yes", {}).get("asks", [])),
        "no_asks": normalize_book_side(raw.get("no", {}).get("asks", [])),
        "yes_bids": normalize_book_side(raw.get("yes", {}).get("bids", [])),
        "no_bids": normalize_book_side(raw.get("no", {}).get("bids", [])),
        "raw": raw,
    }

def get_outcome_books_polymarket(slug: str, outcome_name: str) -> Optional[dict]:
    raw = get_polymarket_orderbook(slug)
    if raw is None: return None
    okey = normalize_str(outcome_name).lower()
    return {
        "buy_levels": normalize_book_side(raw.get(okey, {}).get("asks", [])),
        "sell_levels": normalize_book_side(raw.get(okey, {}).get("bids", [])),
        "raw": raw,
    }

# =========================================================
# Exposure helpers
# =========================================================

def get_current_exposure(tracked_pairs_df: pd.DataFrame) -> dict:
    try:
        k_pos = get_kalshi_positions()
        p_pos = get_polymarket_positions()
    except Exception as e:
        print(f"Exposure fetch error: {e}")
        return {"success": False, "error": str(e)}

    exposure = {}
    k_to_pair = {}
    p_to_pair = {}
    for _, row in tracked_pairs_df.iterrows():
        kt = normalize_str(row.get('kalshi_ticker'))
        pt = normalize_str(row.get('polymarket_ticker'))
        pid = normalize_str(row.get('pair_id', f"{kt}__{pt}"))
        k_to_pair[kt] = pid
        p_to_pair[pt] = pid
        exposure[pid] = {
            "pair_id": pid, "contracts_kalshi": 0, "kalshi_side": "", "avg_price_kalshi": 0.0,
            "contracts_poly": 0, "polymarket_side": "", "avg_price_poly": 0.0,
            "value_usd": 0.0, "kalshi_ticker": kt, "polymarket_ticker": pt,
            "close_time": normalize_str(row.get('close_time', ""))
        }

    total_val = 0.0
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
            
    return {"success": True, "total_portfolio_usd": total_val, "pair_exposure": exposure}

# =========================================================
# Cash helpers
# =========================================================

def get_kalshi_available_usd() -> float:
    bal = get_kalshi_balance()
    if not bal: return 0.0
    return safe_float(bal.get("available_cents", 0)) / 100.0

def get_polymarket_available_usd() -> float:
    wallet = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not wallet: return 0.0
    return safe_float(get_polymarket_balance(wallet), 0.0)

# =========================================================
# Trading helpers
# =========================================================

def size_trade_to_available_balances(arb: dict, ks_cash: float, pm_cash: float) -> dict:
    original_contracts = int(math.floor(arb["contracts"]))
    if original_contracts <= 0: return {"ok": False, "reason": "contracts rounded to 0"}

    ks_spendable = max(ks_cash - BALANCE_BUFFER_USD, 0.0)
    pm_spendable = max(pm_cash - BALANCE_BUFFER_USD, 0.0)

    mc_ks = int(math.floor(ks_spendable / max(arb["price_a"], 1e-12)))
    mc_pm = int(math.floor(pm_spendable / max(arb["price_b"], 1e-12)))

    final_contracts = min(original_contracts, mc_ks, mc_pm)

    if final_contracts <= 0:
        return {"ok": False, "reason": f"Low balance: KS=${ks_cash:.2f}, PM=${pm_cash:.2f}"}

    sized = dict(arb)
    sized["contracts"] = final_contracts
    sized["notional_usd"] = final_contracts * arb["sum_price"]
    return {"ok": True, "arb": sized}

def place_dual_orders(pair_row: pd.Series, arb: dict, state: MarketState) -> dict:
    pid = arb['pair_id']
    with PairLock(pid) as locked:
        if not locked:
            return {"status": "skipped", "message": f"Pair {pid} is locked by another process"}
            
        # RE-FETCH CASH SECONDS BEFORE TRADING
        state.ks_cash = get_kalshi_available_usd()
        state.pm_cash = get_polymarket_available_usd()
        state.total_nav = state.total_portfolio_usd + state.ks_cash + state.pm_cash

    # Concentration
    if state.total_nav > 10.0:
        alloc = DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR
        curr_val = state.exposure.get(arb['pair_id'], {}).get('value_usd', 0.0)
        potential = curr_val + arb['notional_usd']
        if potential > (state.total_nav * alloc):
            allowed = max(0.0, (state.total_nav * alloc) - curr_val)
            if allowed < 1.0: return {"status": "skipped", "message": "Concentration limit"}
            factor = allowed / arb['notional_usd']
            arb['contracts'] *= factor
            arb['notional_usd'] = arb['contracts'] * arb['sum_price']

    sizing = size_trade_to_available_balances(arb, state.ks_cash, state.pm_cash)
    if not sizing["ok"]: return {"status": "skipped", "message": sizing["reason"]}
    
    arb = sizing["arb"]
    contracts = int(math.floor(arb["contracts"]))
    if contracts <= 0: return {"status": "skipped", "message": "Zero contracts"}

    k_resp = kalshi_place_limit_order(ticker=normalize_str(pair_row["kalshi_ticker"]), side=arb["kalshi_side"], action="buy", count=contracts, price_cents=int(round(arb["price_a"]*100)), time_in_force="fill_or_kill")
    p_resp = polymarket_place_limit_order(slug=normalize_str(pair_row["polymarket_ticker"]), outcome=arb["polymarket_outcome"], size=contracts, price=round(arb["price_b"], 6), side="BUY", order_type="FOK")

    k_ok = k_resp and (k_resp.get("status") == "success" or "order_id" in str(k_resp))
    p_ok = p_resp and (p_resp.get("status") == "success" or "orderID" in str(p_resp))

    status = "success"
    msg = ""
    if not k_ok or not p_ok:
        if k_ok or p_ok:
            status = "partial"
            msg = f"Hanging leg: K={k_ok}, P={p_ok}"
        else:
            status = "error"
            msg = "Both failed"

    return {
        "status": status, "contracts": contracts, "price_a": arb["price_a"], "price_b": arb["price_b"],
        "sum_price": arb["sum_price"], "profit_pct": arb["profit_pct"], "message": msg
    }

def liquidate_pair(pair_id: str, info: dict, k_qty: int, p_qty: int, k_min_cents: int, p_min: float, dry_run: bool = True) -> dict:
    if dry_run: return {"status": "dry_run", "message": f"Would liquidate {k_qty}K, {p_qty}P"}
    
    with PairLock(pair_id) as locked:
        if not locked:
            return {"status": "error", "message": f"Pair {pair_id} is locked by another process"}
            
        k_resp = None
    if k_qty > 0:
        books = get_yes_no_books_kalshi(info['kalshi_ticker'])
        bids = books['yes_bids'] if info['kalshi_side'] == 'yes' else books['no_bids']
        if not bids: return {"status": "error", "message": "No Kalshi bids"}
        best = int(round(bids[0]['price'] * 100))
        if best < k_min_cents: return {"status": "error", "message": "Price slip"}
        k_resp = kalshi_place_limit_order(ticker=info['kalshi_ticker'], side=info['kalshi_side'], action="sell", count=k_qty, price_cents=best, time_in_force="immediate_or_cancel")

    p_resp = None
    if p_qty > 0:
        books = get_outcome_books_polymarket(info['polymarket_ticker'], info['polymarket_side'])
        bids = books['sell_levels']
        if not bids: return {"status": "error", "message": "No Poly bids"}
        best = bids[0]['price']
        if best < p_min: return {"status": "error", "message": "Price slip"}
        p_resp = polymarket_place_limit_order(slug=info['polymarket_ticker'], outcome=info['polymarket_side'], size=p_qty, price=best, side="SELL", order_type="IOC")

    return {"status": "success", "message": f"Liquidated {k_qty}K, {p_qty}P"}
