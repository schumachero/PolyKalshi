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
import pandas as pd
from dotenv import load_dotenv

# Import semantic matching logic
try:
    from matching.semantic_matching import generate_semantic_matches
except ImportError:
    # Handle the case where src is not in path or relative import issues
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from matching.semantic_matching import generate_semantic_matches

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


def get_kalshi_market_details(ticker: str) -> dict:
    """Fetch rules and better title for a Kalshi market."""
    data = _kalshi_get(f"/markets/{ticker}")
    if not data:
        return {}
    m = data.get("market", {})
    rules = (m.get("rules_primary") or "") + "\n" + (m.get("rules_secondary") or "")
    return {
        "title": m.get("title", ticker),
        "rules": rules.strip(),
        "yes_bid": float(m.get("yes_bid_dollars", 0) or 0),
        "no_bid": float(m.get("no_bid_dollars", 0) or 0)
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
            ticker = pos.get("ticker", "")
            # Kalshi v2 now uses 'position_fp' for quantity (string)
            raw_pos = float(pos.get("position_fp", 0) or 0)
            yes_qty = raw_pos if raw_pos > 0 else 0
            no_qty  = -raw_pos if raw_pos < 0 else 0
            
            qty = yes_qty or no_qty
            if qty <= 0:
                continue

            side = "YES" if yes_qty > 0 else "NO"
            
            # Enrich with rules and better title/prices
            details = get_kalshi_market_details(ticker)
            
            # Use the bid price for the side we hold to get current market value
            cur_price = details.get("yes_bid", 0) if side == "YES" else details.get("no_bid", 0)
            
            # Standardize to cents for internal consistency if needed
            exp_cents = int(float(pos.get("market_exposure_dollars", 0) or 0) * 100)
            pnl_cents = int(float(pos.get("realized_pnl_dollars", 0) or 0) * 100)
            traded_cents = int(float(pos.get("total_traded_dollars", 0) or 0) * 100)

            positions.append({
                "ticker":              ticker,
                "title":               details.get("title", pos.get("market_title", ticker)),
                "rules":               details.get("rules", ""),
                "side":                side,
                "quantity":            int(qty),
                "avg_price_cents":     int((exp_cents / qty)) if qty > 0 else 0, 
                "current_price":       cur_price,
                "realized_pnl_cents":  pnl_cents,
                "total_traded_cents":  traded_cents,
                "close_time":          pos.get("close_time", ""),
                "market_exposure_cents": exp_cents,
            })

        cursor = data.get("cursor")
        if not cursor:
            break

    return positions


def get_polymarket_market_details(market_id_or_slug: str) -> dict:
    """Fetch question and rules (description) for a Polymarket market using slug or ID."""
    url = "https://gamma-api.polymarket.com/markets"
    # Try slug first, then ID if it looks like a number
    params = {"slug": market_id_or_slug} if not market_id_or_slug.isdigit() else {"id": market_id_or_slug}
    
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data:
            return {}
        m = data[0] if isinstance(data, list) else data
        return {
            "title": m.get("question", m.get("title", "")),
            "rules": (m.get("description", "") + "\nRules: " + (m.get("rules") or "")).strip()
        }
    except Exception as e:
        print(f"[portfolio] Polymarket market detail error for {market_id_or_slug}: {e}")
        return {}


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
            slug = pos.get("slug", "")

            # Enrich with rules
            details = get_polymarket_market_details(slug)

            positions.append({
                "market_id":     slug,
                "condition_id":  pos.get("conditionId", ""),
                "title":         details.get("title", pos.get("title", "")),
                "rules":         details.get("rules", ""),
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
    Ensures all rows have the same keys for DictWriter.
    """
    if not rows:
        return
    
    # Get all unique keys from all rows to ensure consistent columns
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    
    # Sort keys for consistent column order
    key_order = [
        "Platform", "Ticker", "Title", "Side", "Quantity", "Price", 
        "Value_USD", "P&amp;L_USD", "Closing_Time", "Rules", 
        "Matched_Ticker", "Match_Score"
    ]
    # Add any missing keys at the end
    fieldnames = [k for k in key_order if k in all_keys] + sorted(list(all_keys - set(key_order)))

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=fieldnames)
            dict_writer.writeheader()
            dict_writer.writerows(rows)
        print(f"\nPortfolio saved to {filename}")
    except Exception as e:
        print(f"\nError saving CSV: {e}")


def print_portfolio_summary(wallet_address: str = None):
    """
    Print a human-readable summary of both Kalshi and Polymarket portfolios.
    Also calculates totals, runs semantic matching, and saves to CSV.
    """
    print("\n" + "=" * 60)
    print("  PORTFOLIO SUMMARY")
    print("=" * 60)

    all_rows = []
    total_value_usd = 0.0

    k_pos_data = [] # For semantic matching
    p_pos_data = [] # For semantic matching

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
            k_total = k_val
            total_value_usd += k_total
            print(f"  Cash available : ${_cents_to_dollars(int(k_cash*100))[1:]}")
            print(f"  Portfolio value: ${_cents_to_dollars(int(k_val*100))[1:]} (includes cash)")

            all_rows.append({
                "Platform": "Kalshi",
                "Ticker": "CASH",
                "Title": "Available Cash",
                "Side": "N/A",
                "Quantity": k_cash,
                "Price": 1.0,
                "Value_USD": k_cash,
                "P&L_USD": 0.0,
                "Closing_Time": "N/A",
                "Rules": ""
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
                
                row = {
                    "Platform": "Kalshi",
                    "Ticker": p["ticker"],
                    "Title": p["title"],
                    "Side": p["side"],
                    "Quantity": p["quantity"],
                    "Price": "N/A",  # Kalshi API is tricky with avg price
                    "Value_USD": val_usd,
                    "P&L_USD": p["realized_pnl_cents"] / 100,
                    "Closing_Time": close,
                    "Rules": p["rules"]
                }
                all_rows.append(row)
                k_pos_data.append({
                    "market_ticker": p["ticker"],
                    "market_title": p["title"],
                    "rules_text": p["rules"],
                    "close_time": p["close_time"]
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
            "Closing_Time": "N/A",
            "Rules": ""
        })

        p_positions = get_polymarket_positions(addr)
        if p_positions:
            print(f"\n  Open positions ({len(p_positions)}):")
            print(f"  {'Title':<45} {'Side':<5} {'Size':>8}  {'Value':>10}  {'P&L':>8}  {'Ends'}")
            print(f"  {'-'*45} {'-'*5} {'-'*8}  {'-'*10}  {'-'*8}  {'-'*12}")
            for p in p_positions:
                title_short = (p["title"] or p["market_id"])[:44]
                end = p["end_date"][:10] if p["end_date"] else "N/A"
                pnl = f"+{p['pnl']:.2f}" if p["pnl"] >= 0 else f"{p['pnl']:.2f}"
                cur_val = p["current_value"]
                p_total += cur_val
                print(f"  {title_short:<45} {p['side']:<5} {p['size']:>8.2f}  ${cur_val:>9.2f}  {pnl:>8}  {end}")
                
                row = {
                    "Platform": "Polymarket",
                    "Ticker": p["market_id"],
                    "Title": p["title"],
                    "Side": p["side"],
                    "Quantity": p["size"],
                    "Price": p["current_price"],
                    "Value_USD": cur_val,
                    "P&L_USD": p["pnl"],
                    "Closing_Time": end,
                    "Rules": p["rules"]
                }
                all_rows.append(row)
                p_pos_data.append({
                    "market_ticker": p["market_id"],
                    "market_title": p["title"],
                    "rules_text": p["rules"],
                    "close_time": p["end_date"],
                    "status": "active" # For generate_semantic_matches filtering
                })
            
            print(f"\n  Polymarket Total Value: ${p_total:.2f}")
            total_value_usd += p_total
        else:
            print("\n  No open positions found (or wallet has no activity).")

    # --- Semantic Matching ---
    if k_pos_data and p_pos_data:
        print("\n" + "-" * 40)
        print("  Running Semantic Matching on Portfolio...")
        try:
            k_df = pd.DataFrame(k_pos_data)
            p_df = pd.DataFrame(p_pos_data)
            matches_df = generate_semantic_matches(k_df, p_df, threshold=0.3)
            
            if not matches_df.empty:
                print(f"  Found {len(matches_df)} potential overlaps:")
                # Create match maps for quick lookup
                # kalshi_ticker -> (poly_ticker, score)
                k_matches = {}
                p_matches = {}
                for _, m in matches_df.iterrows():
                    kt = m['kalshi_market_ticker']
                    pt = m['polymarket_market_ticker']
                    score = m['semantic_score']
                    
                    if kt not in k_matches or score > k_matches[kt][1]:
                        k_matches[kt] = (pt, score)
                    if pt not in p_matches or score > p_matches[pt][1]:
                        p_matches[pt] = (kt, score)

                # Update all_rows with match info
                for r in all_rows:
                    ticker = r["Ticker"]
                    if r["Platform"] == "Kalshi" and ticker in k_matches:
                        r["Matched_Ticker"] = k_matches[ticker][0]
                        r["Match_Score"] = k_matches[ticker][1]
                        print(f"    [!] Kalshi {ticker} matches Polymarket {k_matches[ticker][0]} (Score: {k_matches[ticker][1]})")
                    elif r["Platform"] == "Polymarket" and ticker in p_matches:
                        r["Matched_Ticker"] = p_matches[ticker][0]
                        r["Match_Score"] = p_matches[ticker][1]
        except Exception as e:
            print(f"  Error during semantic matching: {e}")

    print("\n" + "=" * 60)
    print(f"  TOTAL PORTFOLIO VALUE: ${total_value_usd:.2f}")
    print("=" * 60 + "\n")

    if all_rows:
        save_portfolio_to_csv(all_rows)


if __name__ == "__main__":
    print_portfolio_summary()
