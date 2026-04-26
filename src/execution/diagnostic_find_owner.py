import json
import requests
from eth_abi import decode

# The Polymarket Proxy Wallet address
PROXY_ADDRESS = "0xa78342da292ad314cc972863818f414723330a48"
# The storage slot for the owner (keccak256("owner"))
OWNER_SLOT = "0x734a2a5caf82146a5ddd5263d9af379f9f72724959f0567ddc9df2c40cf2cc20"
# Polygon RPC
RPC_URL = "https://polygon-rpc.com"

def get_proxy_owner():
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getStorageAt",
        "params": [
            PROXY_ADDRESS,
            OWNER_SLOT,
            "latest"
        ],
        "id": 1
    }
    
    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        result = response.json().get("result")
        
        if result and result != "0x" and result != "0x0000000000000000000000000000000000000000000000000000000000000000":
            # The address is in the last 20 bytes of the 32-byte slot
            owner_address = "0x" + result[-40:]
            print(f"TRUE OWNER of {PROXY_ADDRESS}: {owner_address}")
            return owner_address
        else:
            print(f"Storage slot is empty for {PROXY_ADDRESS}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    get_proxy_owner()
