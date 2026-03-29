
def calculate_kalshi_fee(price):
    """Replicates the logic implemented in src/frontend_dashboard.py"""
    if price <= 0 or price >= 1.0:
        return 0
    return 0.07 * price * (1.0 - price)

def test_fee_scenarios():
    test_cases = [
        {"price": 0.50, "expected_fee": 0.0175},
        {"price": 0.10, "expected_fee": 0.0063},
        {"price": 0.90, "expected_fee": 0.0063},
        {"price": 0.25, "expected_fee": 0.013125},
        {"price": 0.75, "expected_fee": 0.013125},
    ]

    print(f"{'Price':<10} | {'Expected Fee':<15} | {'Actual Fee':<15} | {'Result'}")
    print("-" * 55)

    for case in test_cases:
        price = case["price"]
        expected = case["expected_fee"]
        actual = calculate_kalshi_fee(price)
        
        # Use a small epsilon for float comparison
        result = "PASS" if abs(actual - expected) < 1e-9 else "FAIL"
        print(f"${price:<9.2f} | ${expected:<14.4f} | ${actual:<14.4f} | {result}")
        
    print("\nVerification of Net Pricing Logic:")
    for price in [0.50, 0.10]:
        fee = calculate_kalshi_fee(price)
        net_bid = price - fee
        net_ask = price + fee
        print(f"Base Price: ${price:.2f}")
        print(f"  Net Bid (Selling): ${net_bid:.4f} (Base - Fee)")
        print(f"  Net Ask (Buying):  ${net_ask:.4f} (Base + Fee)")

if __name__ == "__main__":
    test_fee_scenarios()
