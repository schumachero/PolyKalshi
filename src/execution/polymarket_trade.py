import json
import os
import requests
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
REQUEST_TIMEOUT = 15

POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY")
POLY_FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS")
POLY_SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "1"))  # 0=EOA, 1/2=proxy-style setups


def build_client() -> ClobClient:
    if not POLY_PRIVATE_KEY:
        raise ValueError("Missing POLY_PRIVATE_KEY in .env")
    if not POLY_FUNDER_ADDRESS:
        raise ValueError("Missing POLY_FUNDER_ADDRESS in .env")

    print(f"[DEBUG] Building Polymarket Client | Signer: {POLY_FUNDER_ADDRESS} | Signature Type: {POLY_SIGNATURE_TYPE}")
    client = ClobClient(
        host=HOST,
        chain_id=CHAIN_ID,
        key=POLY_PRIVATE_KEY,
        signature_type=POLY_SIGNATURE_TYPE,
        funder=POLY_FUNDER_ADDRESS,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_market_by_identifier(identifier: str) -> dict:
    identifier = str(identifier).strip()
    params = {}
    if identifier.isdigit():
        params["id"] = identifier
    else:
        params["slug"] = identifier

    r = requests.get(GAMMA_URL, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"No Polymarket market found for identifier={identifier}")
    return data[0] if isinstance(data, list) else data


def _parse_clob_token_ids(raw_value) -> list[str]:
    if isinstance(raw_value, list):
        return [str(x) for x in raw_value]
    if isinstance(raw_value, str):
        parsed = json.loads(raw_value)
        return [str(x) for x in parsed]
    raise ValueError("Unexpected clobTokenIds format")


def get_token_id_from_identifier(identifier: str, outcome: str) -> str:
    market = get_market_by_identifier(identifier)
    token_ids = _parse_clob_token_ids(market.get("clobTokenIds"))

    outcome = outcome.upper()
    if outcome == "YES":
        return token_ids[0]
    if outcome == "NO":
        return token_ids[1]
    raise ValueError("outcome must be YES or NO")


def place_limit_order(
    slug: str,
    outcome: str,
    size: float,
    price: float,
    side: str = "BUY",
    order_type: str = "GTC",
) -> dict:
    """
    Place a simple Polymarket limit order.

    outcome: YES or NO
    side: BUY or SELL
    order_type: GTC, FOK, or FAK
    """
    if size <= 0:
        raise ValueError("size must be > 0")
    if not (0 < price < 1):
        raise ValueError("price must be between 0 and 1")

    side = side.upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    order_type = order_type.upper()
    if order_type not in {"GTC", "FOK", "FAK"}:
        raise ValueError("order_type must be GTC, FOK, or FAK")

    client = build_client()
    token_id = get_token_id_from_identifier(slug, outcome)

    order_args = OrderArgs(
        token_id=token_id,
        price=float(price),
        size=float(size),
        side=BUY if side == "BUY" else SELL,
    )

    signed_order = client.create_order(order_args)
    return client.post_order(signed_order, getattr(OrderType, order_type))