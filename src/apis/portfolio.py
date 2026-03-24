"""
portfolio.py — Read-Only Portfolio Viewer for Kalshi & Polymarket

IMPORTANT: This module is intentionally GET-only.
No order placement, cancellation, or any write operations exist here.

Required environment variables (.env):
    KALSHI_ACCESS_KEY          - Your Kalshi key ID
    KALSHI_RSA_PRIVATE_KEY     - Your RSA private key PEM (newlines as \\n)
    POLYMARKET_WALLET_ADDRESS  - Your Polygon wallet address (public, no private key needed)
"""

import os
import base64
import datetime
import math
import requests
import csv
from dotenv import load_dotenv

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False

load_dotenv()

# =========================
# Configuration
# =========================

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_DATA_API = "https://data-api.polymarket.com"
REQUEST_TIMEOUT = 15

# =========================
# Kalshi Auth (read-only GET requests only)
# =========================

_KALSHI_ACCESS_KEY = os.getenv("KALSHI_ACCESS_KEY")
_KALSHI_RSA_KEY_STR = os.getenv("KALSHI_RSA_PRIVATE_KEY")
_priv_key = None

if _KALSHI_RSA_KEY_STR and _CRYPTO_AVAILABLE:
    try:
        _pem = _KALSHI_RSA_KEY_STR.replace("\\n", "\n")
        _priv_key = serialization.load_pem_private_key(
            _pem.encode(), password=None, backend=default_backend()
        )
    except Exception as e:
        print(f"[portfolio] Warning: Could not load Kalshi RSA key: {e}")


def _kalshi_auth_headers(method: str, path: str) -> dict:
    """Generate Kalshi RSA-PSS signed request headers."""
    if not _priv_key or not _KALSHI_ACCESS_KEY:
        return {}
    path_no_query = path.split("?")[0]
    timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
    msg = (timestamp + method + path_no_query).encode("utf-8")
    sig = _priv_key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": _KALSHI_ACCESS_KEY,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
    }


def _kalshi_get(endpoint: str, params: dict = None) -> dict | None:
    """Authenticated GET request to Kalshi API. GET only — never writes."""
    path = f"/trade-api/v2{endpoint}"
    url = f"{KALSHI_BASE}{endpoint}"
    headers = _kalshi_auth_headers("GET", path)
    try:
        r = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 401:
            print("[portfolio] Kalshi auth failed — check KALSHI_ACCESS_KEY and KALSHI_RSA_PRIVATE_KEY in .env")
        else:
            print(f"[portfolio] Kalshi HTTP error {e.response.status_code if e.response else '?'}: {e}")
        return None
    except Exception as e:
        print(f"[portfolio] Kalshi request error: {e}")
        return None


# =========================
# Kalshi Portfolio Functions
# =========================

def get_kalshi_balance() -> dict:
    """
    Fetch account balance from Kalshi.
    Returns: {"available_cents": int, "portfolio_value_cents": int} or empty dict on failure.
    """
    data = _kalshi_get("/portfolio/balance")
    if not data:
        return {}
    return {
        "available_cents": data.get("balance", 0),
        "portfolio_value_cents": data.get("portfolio_value", 0),
    }


def get_kalshi_positions() -> list[dict]:
    """
    Fetch all open (unsettled) positions from Kalshi.
    Returns a list of position dicts with keys:
        ticker, title, side, quantity, avg_price_cents, current_value_cents
    """
    positions = []
    cursor = None

    while True:
        params = {"limit": 200, "settlement_status": "unsettled"}
        if cursor:
            params["cursor"] = cursor

        data = _kalshi_get("/portfolio/positions", params=params)
        if not data:
            break
        
        # print(f"[portfolio] Kalshi positions data: {data}")

        for pos in data.get("market_positions", []):
            # Kalshi v2 now uses 'position_fp' for quantity (string)
            # and '_dollars' suffix for dollar-denominated values (strings)
            raw_pos = float(pos.get("position_fp", 0) or 0)
            yes_qty = raw_pos if raw_pos > 0 else 0
            no_qty  = -raw_pos if raw_pos < 0 else 0
            
            qty = yes_qty or no_qty
            if qty <= 0:
                continue

            # Standardize to cents for internal consistency if needed
            exp_cents = int(float(pos.get("market_exposure_dollars", 0) or 0) * 100)
            pnl_cents = int(float(pos.get("realized_pnl_dollars", 0) or 0) * 100)
            traded_cents = int(float(pos.get("total_traded_dollars", 0) or 0) * 100)

            positions.append({
                "ticker":              pos.get("ticker", ""),
                "title":               pos.get("market_title", pos.get("ticker", "")),
                "side":                "YES" if yes_qty > 0 else "NO",
                "quantity":            int(qty),
                "avg_price_cents":     0, # Kalshi doesn't easily expose avg price per market in this call
                "realized_pnl_cents":  pnl_cents,
                "total_traded_cents":  traded_cents,
                "close_time":          pos.get("close_time", ""),
                "market_exposure_cents": exp_cents,
            })

        cursor = data.get("cursor")
        if not cursor:
            break

    return positions


# =========================
# Polymarket Portfolio Functions
# =========================

def get_polymarket_positions(wallet_address: str = None) -> list[dict]:
    """
    Fetch open positions from Polymarket Data API using wallet address.
    No authentication needed — public endpoint indexed by wallet address.

    Args:
        wallet_address: Polygon wallet (0x...). Falls back to POLYMARKET_WALLET_ADDRESS env var.

    Returns a list of position dicts with keys:
        market_id, title, side, size, avg_price, current_price, pnl
    """
    addr = wallet_address or os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not addr:
        print("[portfolio] No wallet address — set POLYMARKET_WALLET_ADDRESS in .env")
        return []

    positions = []
    limit = 100
    offset = 0

    while True:
        try:
            url = f"{POLYMARKET_DATA_API}/positions"
            r = requests.get(
                url,
                params={
                    "user": addr, 
                    "sizeThreshold": "0.01",
                    "limit": limit,
                    "offset": offset
                },
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            raw = r.json()
        except Exception as e:
            print(f"[portfolio] Polymarket positions error: {e}")
            break

        if not raw:
            break

        for pos in raw:
            size = float(pos.get("size", 0) or 0)
            if size <= 0:
                continue

            outcome = pos.get("outcome", "")

            positions.append({
                "market_id":     pos.get("market", ""),
                "condition_id":  pos.get("conditionId", ""),
                "title":         pos.get("title", ""),
                "side":          "YES" if outcome.lower() in ("yes", "1", "true") else "NO",
                "size":          size,
                "avg_price":     float(pos.get("avgPrice", 0) or 0),
                "current_price": float(pos.get("curPrice", 0) or 0),
                "initial_value": float(pos.get("initialValue", 0) or 0),
                "current_value": float(pos.get("currentValue", 0) or 0),
                "pnl":           float(pos.get("cashPnl", 0) or 0),
                "end_date":      pos.get("endDate", ""),
            })
        
        if len(raw) < limit:
            break
        offset += limit

    return positions


def get_polymarket_balance(addr: str) -> float:
    """
    Fetch USDC.e balance on Polygon for the wallet address.
    USDC.e is the primary collateral for Polymarket.
    """
    if not addr:
        return 0.0
    
    rpc = "https://polygon-bor-rpc.publicnode.com"
    usdc_contract = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    
    # eth_call data: selector 0x70a08231 (balanceOf) + 64-char padded address
    clean_addr = addr.lower().replace("0x", "")
    data = f"0x70a08231000000000000000000000000{clean_addr}"
    
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": usdc_contract, "data": data}, "latest"],
        "id": 1
    }
    
    try:
        r = requests.post(rpc, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        res = r.json()
        if "result" in res and res["result"] != "0x":
            val_hex = res["result"]
            val_int = int(val_hex, 16)
            return val_int / 1_000_000  # USDC.e has 6 decimals
    except Exception as e:
        print(f"[portfolio] Could not fetch Polymarket USDC balance: {e}")
        
    return 0.0


# =========================
# Pretty Printer
# =========================

def _cents_to_dollars(cents: int) -> str:
    return f"${cents / 100:.2f}"


def save_portfolio_to_csv(rows: list[dict], filename: str = "Data/portfolio.csv"):
    """
    Saves the combined portfolio data to a CSV file.
    """
    if not rows:
        return
    
    keys = rows[0].keys()
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(rows)
        print(f"\nPortfolio saved to {filename}")
    except Exception as e:
        print(f"\nError saving CSV: {e}")


def print_portfolio_summary(wallet_address: str = None):
    """
    Print a human-readable summary of both Kalshi and Polymarket portfolios.
    Also calculates totals and saves to CSV.
    """
    print("\n" + "=" * 60)
    print("  PORTFOLIO SUMMARY")
    print("=" * 60)

    all_rows = []
    total_value_usd = 0.0

    # --- Kalshi ---
    print("\nKALSHI")
    print("-" * 40)
    k_total = 0.0
    if not _KALSHI_ACCESS_KEY:
        print("  ⚠  No Kalshi keys found. Set KALSHI_ACCESS_KEY + KALSHI_RSA_PRIVATE_KEY in .env")
    else:
        bal = get_kalshi_balance()
        if bal:
            k_cash = bal.get('available_cents', 0) / 100
            k_val = bal.get('portfolio_value_cents', 0) / 100
            k_total = k_val # Kalshi's portfolio_value usually includes cash + positions
            total_value_usd += k_total
            print(f"  Cash available : ${_cents_to_dollars(int(k_cash*100))[1:]}")
            print(f"  Portfolio value: ${_cents_to_dollars(int(k_val*100))[1:]}")
            
            all_rows.append({
                "Platform": "Kalshi",
                "Ticker": "CASH",
                "Title": "Available Cash",
                "Side": "N/A",
                "Quantity": k_cash,
                "Price": 1.0,
                "Value_USD": k_cash,
                "P&L_USD": 0.0,
                "Closing_Time": "N/A"
            })
        else:
            print("  Could not fetch balance (auth error?)")

        k_positions = get_kalshi_positions()
        if k_positions:
            print(f"\n  Open positions ({len(k_positions)}):")
            print(f"  {'Ticker':<35} {'Side':<5} {'Qty':>6}  {'Value':>10}  {'Close Time'}")
            print(f"  {'-'*35} {'-'*5} {'-'*6}  {'-'*10}  {'-'*20}")
            for p in k_positions:
                val_cents = p["market_exposure_cents"]
                val_usd = val_cents / 100
                close = p["close_time"][:10] if p["close_time"] else "N/A"
                print(f"  {p['ticker']:<35} {p['side']:<5} {p['quantity']:>6}  {_cents_to_dollars(val_cents):>10}  {close}")
                
                all_rows.append({
                    "Platform": "Kalshi",
                    "Ticker": p["ticker"],
                    "Title": p["title"],
                    "Side": p["side"],
                    "Quantity": p["quantity"],
                    "Price": "N/A",  # Kalshi API is tricky with avg price
                    "Value_USD": val_usd,
                    "P&L_USD": p["realized_pnl_cents"] / 100,
                    "Closing_Time": close
                })
        else:
            print("\n  No open positions found.")

    # --- Polymarket ---
    print("\n\nPOLYMARKET")
    print("-" * 40)
    p_total = 0.0
    addr = wallet_address or os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not addr:
        print("  ⚠  No wallet address. Set POLYMARKET_WALLET_ADDRESS in .env")
    else:
        print(f"  Wallet: {addr}")
        
        # Cash balance
        p_cash = get_polymarket_balance(addr)
        print(f"  Cash available : ${p_cash:.2f}")
        total_value_usd += p_cash
        
        all_rows.append({
            "Platform": "Polymarket",
            "Ticker": "CASH",
            "Title": "USDC.e Balance",
            "Side": "N/A",
            "Quantity": p_cash,
            "Price": 1.0,
            "Value_USD": p_cash,
            "P&L_USD": 0.0,
            "Closing_Time": "N/A"
        })

        p_positions = get_polymarket_positions(addr)
        if p_positions:
            print(f"\n  Open positions ({len(p_positions)}):")
            print(f"  {'Title':<45} {'Side':<5} {'Size':>8}  {'Value':>10}  {'P&L':>8}  {'Ends'}")
            print(f"  {'-'*45} {'-'*5} {'-'*8}  {'-'*10}  {'-'*8}  {'-'*12}")
            for p in p_positions:
                title = (p["title"] or p["market_id"])[:44]
                end = p["end_date"][:10] if p["end_date"] else "N/A"
                pnl = f"+{p['pnl']:.2f}" if p["pnl"] >= 0 else f"{p['pnl']:.2f}"
                cur_val = p["current_value"]
                p_total += cur_val
                print(f"  {title:<45} {p['side']:<5} {p['size']:>8.2f}  ${cur_val:>9.2f}  {pnl:>8}  {end}")
                
                all_rows.append({
                    "Platform": "Polymarket",
                    "Ticker": p["market_id"],
                    "Title": p["title"],
                    "Side": p["side"],
                    "Quantity": p["size"],
                    "Price": p["current_price"],
                    "Value_USD": cur_val,
                    "P&L_USD": p["pnl"],
                    "Closing_Time": end
                })
            
            print(f"\n  Polymarket Total Value: ${p_total:.2f}")
            total_value_usd += p_total
        else:
            print("\n  No open positions found (or wallet has no activity).")

    print("\n" + "=" * 60)
    print(f"  TOTAL PORTFOLIO VALUE: ${total_value_usd:.2f}")
    print("=" * 60 + "\n")

    if all_rows:
        save_portfolio_to_csv(all_rows)


if __name__ == "__main__":
    print_portfolio_summary()
