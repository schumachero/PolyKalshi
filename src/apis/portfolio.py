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

        for pos in data.get("market_positions", []):
            # Kalshi reports quantity as number of contracts; price in cents
            yes_qty = pos.get("position", 0)  # positive = long YES
            no_qty  = -yes_qty if yes_qty < 0 else 0
            yes_qty = max(yes_qty, 0)

            positions.append({
                "ticker":              pos.get("ticker", ""),
                "title":               pos.get("market_title", pos.get("ticker", "")),
                "side":                "YES" if yes_qty > 0 else "NO",
                "quantity":            yes_qty or no_qty,
                "avg_price_cents":     pos.get("fees_paid", 0),   # Kalshi doesn't expose avg; placeholder
                "realized_pnl_cents":  pos.get("realized_pnl", 0),
                "total_traded_cents":  pos.get("total_traded", 0),
                "close_time":          pos.get("close_time", ""),
                "market_exposure_cents": pos.get("market_exposure", 0),
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

    try:
        url = f"{POLYMARKET_DATA_API}/positions"
        r = requests.get(
            url,
            params={"user": addr, "sizeThreshold": "0.01"},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        print(f"[portfolio] Polymarket positions error: {e}")
        return []

    positions = []
    for pos in raw:
        size = float(pos.get("size", 0) or 0)
        if size <= 0:
            continue

        token_ids = pos.get("asset", "")
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

    return positions


# =========================
# Pretty Printer
# =========================

def _cents_to_dollars(cents: int) -> str:
    return f"${cents / 100:.2f}"


def print_portfolio_summary(wallet_address: str = None):
    """
    Print a human-readable summary of both Kalshi and Polymarket portfolios.
    """
    print("\n" + "=" * 60)
    print("  PORTFOLIO SUMMARY")
    print("=" * 60)

    # --- Kalshi ---
    print("\n📊 KALSHI")
    print("-" * 40)
    if not _KALSHI_ACCESS_KEY:
        print("  ⚠  No Kalshi keys found. Set KALSHI_ACCESS_KEY + KALSHI_RSA_PRIVATE_KEY in .env")
    else:
        bal = get_kalshi_balance()
        if bal:
            print(f"  Cash available : {_cents_to_dollars(bal.get('available_cents', 0))}")
            print(f"  Portfolio value: {_cents_to_dollars(bal.get('portfolio_value_cents', 0))}")
        else:
            print("  Could not fetch balance (auth error?)")

        positions = get_kalshi_positions()
        if positions:
            print(f"\n  Open positions ({len(positions)}):")
            print(f"  {'Ticker':<35} {'Side':<5} {'Qty':>6}  {'Exposure':>10}  {'Close Time'}")
            print(f"  {'-'*35} {'-'*5} {'-'*6}  {'-'*10}  {'-'*20}")
            for p in positions:
                exp = _cents_to_dollars(p["market_exposure_cents"])
                close = p["close_time"][:10] if p["close_time"] else "N/A"
                print(f"  {p['ticker']:<35} {p['side']:<5} {p['quantity']:>6}  {exp:>10}  {close}")
        else:
            print("\n  No open positions found.")

    # --- Polymarket ---
    print("\n\n📊 POLYMARKET")
    print("-" * 40)
    addr = wallet_address or os.getenv("POLYMARKET_WALLET_ADDRESS", "")
    if not addr:
        print("  ⚠  No wallet address. Set POLYMARKET_WALLET_ADDRESS in .env")
    else:
        print(f"  Wallet: {addr}")
        positions = get_polymarket_positions(addr)
        if positions:
            print(f"\n  Open positions ({len(positions)}):")
            print(f"  {'Title':<45} {'Side':<5} {'Size':>8}  {'Avg$':>6}  {'P&L':>8}  {'Ends'}")
            print(f"  {'-'*45} {'-'*5} {'-'*8}  {'-'*6}  {'-'*8}  {'-'*12}")
            for p in positions:
                title = (p["title"] or p["market_id"])[:44]
                end = p["end_date"][:10] if p["end_date"] else "N/A"
                pnl = f"+{p['pnl']:.2f}" if p["pnl"] >= 0 else f"{p['pnl']:.2f}"
                print(f"  {title:<45} {p['side']:<5} {p['size']:>8.2f}  {p['avg_price']:>6.3f}  {pnl:>8}  {end}")
        else:
            print("\n  No open positions found (or wallet has no activity).")

    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    print_portfolio_summary()
