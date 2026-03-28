from kalshi_trade import place_limit_order as kalshi_place_limit_order
from polymarket_trade import place_limit_order as polymarket_place_limit_order


def main():
    kalshi_response = kalshi_place_limit_order(
        ticker="KXNETANYAHUPARDON-26-JUL01",
        side="yes",
        action="buy",
        count=10,
        price_cents=19,
        time_in_force="fill_or_kill",
    )

    polymarket_response = polymarket_place_limit_order(
        slug="will-nicols-maduro-be-the-leader-of-venezuela-end-of-2026",
        outcome="NO",
        size=10,
        price=0.836,
        side="BUY",
        order_type="FOK",
    )

    print("Kalshi response:")
    print(kalshi_response)
    #print()
    print("Polymarket response:")
    print(polymarket_response)

if __name__ == "__main__":
    main()