import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from execution.polymarket_trade import place_limit_order

def main():
    slug = "will-bitcoin-dip-to-65k-in-april-2026-355-765"
    outcome = "YES"
    size = 5          # Number of contracts/shares
    price = 0.05      # Price per share

    print(f"Attempting to buy {size} contracts of {slug} (Outcome: {outcome}) at ${price} each...")
    print(f"Total order value: ${size * price:.2f} USD")
    print("-" * 50)

    try:
        response = place_limit_order(
            slug=slug,
            outcome=outcome,
            size=size,
            price=price,
            side="BUY",
            order_type="FOK", # Fill Or Kill
        )
        print("Success! Response from Polymarket:")
        print(response)
    except Exception as e:
        print(f"Failed to place order! Error:\n{e}")

if __name__ == "__main__":
    main()
