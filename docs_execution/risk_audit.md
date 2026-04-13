# Arbitrage Execution - Risk Audit

This document details the potential risks and edge cases identifying in the current Kalshi-Polymarket arbitrage execution system.

## 1. Execution Path Risks

### ⚠️ Direct Buy (Phase 2)
**Sequence**: Kalshi (Buy FOK) -> Polymarket (Buy FOK).
*   **The "Hanging Leg" Risk**: If Kalshi fills and Polymarket fails (due to book movement or API timeout), you are left with a 1-sided position.
*   **Mitigation**: The bot currently uses `FOK` (Fill or Kill) for both. If Poly fails, it detects the "partial" success and flags it on Telegram.

### ⚠️ Swap (Phase 4)
**Sequence**: Liquidate Old (Best Bid) -> Re-verify New -> Kalshi (Buy FOK) -> Polymarket (Buy FOK).
*   **The "Cash Trap" Risk**: If you liquidate your old position but the buy-leg of the new position fails, you are now in cash. 
*   **Trade-off**: The code is designed to **stay in cash** rather than "reversion" (re-buying what you just sold). Re-buying would incur double fees and potentially "ping-pong" your capital into a loss.
*   **Mitigation**: The bot sends a High-Priority Telegram alert if it gets stuck in cash during a swap.

## 2. Market & Network Edge Cases

### 🕒 API Timeout on Leg 2
If the monitor sends an order to Polymarket but the network hangs:
- **Condition**: Kalshi filled, Poly state unknown.
- **Bot Behavior**: The bot treats this as a "partial" success. 
- **Danger**: You might think the trade failed, but it actually filled on Poly.
- **Prevention**: The monitor re-checks exposure at the start of every loop to "discover" these hidden fills.

### 🚤 Rapid Book Evaporation
Between the `choose_best_arb` check and the actual order:
- **Risk**: The 90c ask becomes 92c. 
- **Mitigation**: `DEFAULT_REVERIFY_BOOKS = True` performs a fresh fetch milliseconds before the order to ensure the math still works.

### 📉 Settlement Mismatch
- **Case**: Kalshi resolves a market at 10:00 AM, but Polymarket resolves it at 10:30 AM.
- **Implication**: For 30 minutes, the bot will see a "Hanging Leg" (Kalshi is 0, Poly is >0).
- **Behavior**: The bot will attempt to "fix" it by selling the Poly side. 
- **Guardrail**: We use `0.9995` target exits to capture value *before* settlement.

## 3. Financial Guardrails
- **Concentration**: `DEFAULT_MAX_PORTFOLIO_PCT_PER_PAIR` (50%) prevents any single arb from taking more than half your money.
- **No-Loss Selling**: During maturity exits, the bot will NOT sell if the current bid is below the target price, unless it's a "Hanging Leg" resolution where directional risk is deemed more dangerous than a small loss (capped at 5%).
