import math
import sys
import os

# Lägg till src i path så vi kan importera din riktiga kod
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
try:
    from execution.portfolio_exit_executor import DEFAULT_CUTOFF_CENTS
except ImportError:
    DEFAULT_CUTOFF_CENTS = 0.9999 # Fallback if import fails

def calculate_mock_kalshi_fee(price):
    return 0.07 * price * (1.0 - price)

def calculate_mock_poly_fee(price):
    # Justerat för att matcha din kalkylerade avgift (ca 0.01 - 0.04 beroende på pris)
    return 0.04 * (price * (1.0 - price))

def simulate_exit_scenario(name, k_holdings, p_holdings, k_bid, p_bid, k_vol, p_vol):
    cutoff = DEFAULT_CUTOFF_CENTS
    print(f"\n--- SCENARIO: {name} ---")
    
    # 1. Beräkna avgifter
    k_fee = calculate_mock_kalshi_fee(k_bid)
    p_fee = calculate_mock_poly_fee(p_bid)
    
    k_net = k_bid - k_fee
    p_net = p_bid - p_fee
    
    sum_raw = k_bid + p_bid
    sum_net = k_net + p_net
    
    # Resultat per kontrakt jämfört med 1.00
    profit_per_contract = sum_net - 1.00 # Negativt betyder att vi går back totalt på affären
    
    print(f"Bids: Kalshi {k_bid:.4f} (Fee: {k_fee:.4f}), Poly {p_bid:.4f} (Fee: {p_fee:.4f})")
    print(f"Total RAW: {sum_raw:.4f} | Total NET: {sum_net:.4f} | Cutoff: {cutoff:.4f}")
    
    # 2. Beslut om sälj
    if sum_net < cutoff:
        print(f"RESULTAT: [SKIP] Netto {sum_net:.4f} är lägre än din cutoff {cutoff:.4f}.")
        return
    
    # 3. Beräkna storlek
    executable = int(math.floor(min(k_holdings, p_holdings, k_vol, p_vol)))
    if executable < 4:
        print(f"RESULTAT: [SKIP] Volym {executable} för låg.")
        return
        
    # 4. Kolla Polymarket $1 minimum
    poly_value = executable * p_bid
    if poly_value < 1.0:
        print(f"RESULTAT: [ABORT] Polymarket-värde under $1 (${poly_value:.2f}).")
        return
        
    print(f"RESULTAT: [SELL] Exekverar sälj av {executable} kontrakt!")
    print(f"Vinst/Förlust per kontrakt (Net): {profit_per_contract:.4f}")
    print(f"Total utbetalning: ${executable * sum_net:.2f}")

# --- KÖR TESTER MED DIN NYA CUTOFF ---
print(f"Använder CURRENT_CUTOFF från koden: {DEFAULT_CUTOFF_CENTS}")

simulate_exit_scenario("Hög vinst", 100, 100, 0.99, 0.02, 500, 500)
simulate_exit_scenario("Gränsfall (Bör bli SKIP nu)", 100, 100, 0.97, 0.025, 500, 500)
simulate_exit_scenario("BTC Tight (Bör bli SKIP nu)", 500, 500, 0.9650, 0.0300, 1000, 1000)
simulate_exit_scenario("Galen Spread (Garanterat sälj)", 100, 100, 0.9990, 0.05, 500, 500)
