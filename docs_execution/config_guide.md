# Configuration Parameter Guide

This document explains the impact of every configuration constant in `arb_base.py`.

## Core Hurdles
- `DEFAULT_MIN_PROFIT_PCT` (1.5): The minimum gross yield required to even consider a trade.
- `DEFAULT_MIN_LIQUIDITY_USD` (5.0): The minimum size of an arbitrage required. Prevents the bot from wasting fees on "penny arbs."
- `DEFAULT_MIN_SWAP_APY_DELTA` (15.0): The "Hurdle Rate." A swap will only happen if the new trade is at least 15% APY better than the one being sold.

## Risk Management
- `DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR` (0.50): Your "Concentration Limit." No single pair can exceed 50% of your total NAV. If a trade would exceed this, it is automatically shrunk.
- `BALANCE_BUFFER_USD` (0.50): Leaves a small amount of cash in your account to prevent "insufficient funds" errors due to rounding.
- `SLIPPAGE_PROTECTION_FLOOR_PCT` (5.0): If selling a leg (hanging leg or swap), the bot will refuse to sell if the bid is more than 5% below your cost basis. It will notify Telegram instead.

## Exit Strategy
- `DEFAULT_MATURITY_EXIT_PRICE` (0.9995): Use 0.9995 as the "all-out" trigger. This maximizes capital velocity.
- `HANGING_LEG_REBALANCE_MAX_COST` (1.00): How much you are willing to pay to fix a mistake. By default, it will only complete a hedge if the total combined cost is $1.00 or less (no-loss rebalance). Above this, it sells the lone leg.

## Reliability
- `DEFAULT_REVERIFY_BOOKS` (True): ALWAYS performs a second orderbook check a few milliseconds before pulling the trigger. Essential for avoiding stale data.
- `SWAP_FEE_CUSHION_PCT` (1.0): Subtracts 1% from expected swap gains to account for exchange fees and slippage when deciding if a swap is "worth it."
