import sys
import os

def setup_env():
    print("=== PolyKalshi Environment Setup ===")
    print("This script will help you create your .env file with the correct formatting.\n")

    # 1. Kalshi Access Key ID
    kalshi_key_id = input("1. Enter your Kalshi Access Key ID: ").strip()

    # 2. Kalshi RSA Private Key (Multi-line)
    print("\n2. Paste your FULL Kalshi RSA Private Key (including BEGIN/END lines).")
    print("   To finish, press Enter, then Ctrl+Z (Windows) or Ctrl+D (Mac/Linux) on a new line, and Enter again:\n")
    
    lines = sys.stdin.readlines()
    raw_key = "".join(lines).strip()
    
    if not raw_key:
        print("Error: No key provided.")
        return

    # Format key: replace literal newlines with \n for .env string
    formatted_key = raw_key.replace("\r", "").replace("\n", "\\n")

    # 3. Polymarket Wallet Address
    poly_wallet = input("\n3. Enter your Polymarket Wallet Address (0x...): ").strip()

    # Create .env content
    env_content = f"""# PolyKalshi Environment Variables
KALSHI_ACCESS_KEY={kalshi_key_id}
KALSHI_RSA_PRIVATE_KEY={formatted_key}
POLYMARKET_WALLET_ADDRESS={poly_wallet}
"""

    # Write to .env
    env_path = os.path.join(os.getcwd(), ".env")
    
    try:
        with open(env_path, "w") as f:
            f.write(env_content)
        print(f"\n✅ Success! Your .env file has been created at: {env_path}")
        print("Now you can run the portfolio check:")
        print("  .\\.venv\\Scripts\\python.exe src/apis/portfolio.py")
    except Exception as e:
        print(f"\n❌ Error writing .env file: {e}")

if __name__ == "__main__":
    setup_env()
