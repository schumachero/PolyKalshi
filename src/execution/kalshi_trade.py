import os
import uuid
import base64
import datetime
import requests
from dotenv import load_dotenv

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

load_dotenv()

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
REQUEST_TIMEOUT = 15

KALSHI_ACCESS_KEY = os.getenv("KALSHI_ACCESS_KEY")
KALSHI_RSA_PRIVATE_KEY = os.getenv("KALSHI_RSA_PRIVATE_KEY")

_priv_key = None
if KALSHI_RSA_PRIVATE_KEY:
    pem = KALSHI_RSA_PRIVATE_KEY.replace("\\n", "\n")
    _priv_key = serialization.load_pem_private_key(
        pem.encode(),
        password=None,
        backend=default_backend(),
    )


def _auth_headers(method: str, path: str) -> dict:
    if not KALSHI_ACCESS_KEY or _priv_key is None:
        raise ValueError("Missing KALSHI_ACCESS_KEY or KALSHI_RSA_PRIVATE_KEY in .env")

    path_no_query = path.split("?")[0]
    timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
    message = f"{timestamp}{method}{path_no_query}".encode("utf-8")

    signature = _priv_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": KALSHI_ACCESS_KEY,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "Content-Type": "application/json",
    }


def place_limit_order(
    ticker: str,
    side: str,
    action: str,
    count: int,
    price_cents: int,
    time_in_force: str = "fill_or_kill",
    client_order_id: str | None = None,
) -> dict:
    """
    Place a simple Kalshi limit order.

    side: yes or no
    action: buy or sell
    price_cents: 1..99
    """
    side = side.lower()
    action = action.lower()

    if side not in {"yes", "no"}:
        raise ValueError("side must be 'yes' or 'no'")
    if action not in {"buy", "sell"}:
        raise ValueError("action must be 'buy' or 'sell'")
    if count <= 0:
        raise ValueError("count must be > 0")
    if not (1 <= price_cents <= 99):
        raise ValueError("price_cents must be between 1 and 99")

    payload = {
        "ticker": ticker,
        "type": "limit",
        "side": side,
        "action": action,
        "count": int(count),
        "time_in_force": time_in_force,
        "client_order_id": client_order_id or str(uuid.uuid4()),
    }

    if side == "yes":
        payload["yes_price"] = int(price_cents)
    else:
        payload["no_price"] = int(price_cents)

    endpoint = "/portfolio/orders"
    path = f"/trade-api/v2{endpoint}"
    url = f"{KALSHI_BASE}{endpoint}"

    r = requests.post(
        url,
        json=payload,
        headers=_auth_headers("POST", path),
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()