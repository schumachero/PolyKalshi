# Independent & Modular Execution Guide

Following the modularization of the arbitrage system, you can now run specific trading actions independently. This allows for more granular control over your capital and execution frequency.

## The Modular Architecture

The system is split into four primary action modules, all powered by `arb_base.py`:
1.  **`arb_hanging_leg.py`**: Fixes unbalanced / "lonely" legs.
2.  **`arb_sell.py`**: Scans for maturity exits (0.9995 profit taking).
3.  **`arb_buy.py`**: Executes direct buys when cash is available.
4.  **`arb_swap.py`**: Performs APY-based swaps (rebalancing).

---

## 1. Running the Orchestrator
The main `portfolio_arb_monitor.py` script now acts as an orchestrator. It runs all modules in sequence.

### Sequential Discovery
By default, the monitor runs in this specific order:
`Hanging Legs` -> `Maturity Exits` -> `Direct Buys` -> `APY Swaps`

### Selective Execution
You can use the new `--only` and `--skip` flags to control the loop:
```bash
# Example: Run ONLY hanging leg and maturity exit checks
python3 src/execution/portfolio_arb_monitor.py --live --loop --only hanging,sell

# Example: Run everything EXCEPT swaps
python3 src/execution/portfolio_arb_monitor.py --live --loop --skip swap
```

---

## 2. Running Independent Modules
You can run any module as a standalone process. This is highly recommended for **Hanging Leg Resolution**, which should ideally run at a higher frequency than the rest of the bot.

### Hanging Leg Monitor (High Frequency)
```bash
# Recommended for use in a dedicated terminal window
python3 src/execution/arb_hanging_leg.py --live
```

### Maturity / Exit Monitor
```bash
python3 src/execution/arb_sell.py --live
```

---

## 3. Shared State & Logging
- **Market State**: All modules utilize the `MarketState` engine in `arb_base.py`. They fetch fresh exposure and balance data at the start of every cycle.
- **Unified Logs**: All modules write to the same `Data/portfolio_arb_execution_log.csv`. This ensures your dashboard and history tracking remain consistent regardless of how many scripts you are running simultaneously.
- **Telegram Alerts**: All modules share the same Telegram notification credentials and logic.

---

## Recommended Deployment Strategy
For maximum safety and performance, many users prefer this "Split-Window" setup:

1.  **Window 1**: `arb_hanging_leg.py` (Fastest loop, high priority)
2.  **Window 2**: `portfolio_arb_monitor.py --skip hanging` (Standard loop, handles everything else)
