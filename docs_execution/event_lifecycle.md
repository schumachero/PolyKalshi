# Event Lifecycle Guide

This guide explains how the modular arbitrage system treats each specific event it encounters. Events are now processed in discrete phases (**Hanging Leg** -> **Maturity** -> **Buy** -> **Swap**) that can be run together or independently.

## 1. The Arb Discovery Event
1. **Fetch**: Books for all tracked pairs are pulled concurrently.
2. **Depth Check**: The bot calculates exactly how many contracts meet the profit hurdle.
3. **Rank**: All candidates are sorted by APY.
4. **Action**: The bot selects the #1 candidate and moves to **Decision Logic**.

## 2. The Direct Buy Event
*Condition: Cash is available on both Kalshi and Polymarket.*
1. **Verify**: Re-checks orderbooks to ensure prices haven't moved.
2. **Size**: Shrinks the order if it violates concentration limits (50%) or exceeds available cash.
3. **Leg 1**: Sends `Fill or Kill` to Kalshi. 
4. **Leg 2**: If Kalshi fills, sends `FOK` to Polymarket.
5. **Log**: Records the result in `Data/execution_log.csv`.

## 3. The Swap Opportunity Event
*Condition: Cash is low, but a new opportunity has much higher APY than an existing holding.*
1. **Evaluation**: Checks if `New APY > (Old APY + 15%)` AND `Net Gain > $0.05`.
2. **Liquidation**: Sells the "Weakest Link" (lowest APY holding) at the Best Bid.
3. **Step 1/2**: If sell succeeds, proceeds to buy.
4. **Step 2/2**: Attempts to buy the NEW pair using the released cash.
5. **Failure Handing**: If Step 2 fails, it stays in cash and alerts Telegram.

## 4. The Maturity Exit Event (0.9995)
*Condition: A position's combined market value (Kalshi Bid + Poly Bid) is >= $0.9995.*
1. **Trigger**: Detects that nearly all profit has been realized.
2. **Action**: Liquidates both legs immediately at the best available bids.
3. **Rationale**: Exiting early at 0.9995 releases capital for the next 100%+ APY trade, rather than waiting weeks for the final 0.05c profit.

## 5. The Hanging Leg Event
*Condition: `contracts_kalshi != contracts_poly` in the live exposure check.*
1. **Identify**: Determines which venue is missing contracts.
2. **Tiered Resolution**: 
    - **Step A: Profit Hedge**: Completes the arb if total cost < $1.00.
    - **Step B: No-Loss Liquidate**: Sells the lonely leg if bid >= purchase price.
    - **Step C: Breakeven Hedge**: Completes the arb if total cost <= $1.00.
    - **Step D: Emergency Liquidation**: Sells the lonely leg even at a loss (up to 5%).
    - **Step E: Alert & Hold**: If loss > 5%, it aborts and sends a High-Priority Telegram Alert.
3. **Alert**: Always sends a Telegram message detailing which tier was activated.
