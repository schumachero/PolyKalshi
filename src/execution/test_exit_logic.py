import math
import sys
import os

# Lägg till projektets rotmapp i Python Path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from src.execution.portfolio_exit_executor import DEFAULT_CUTOFF_CENTS
    from src.arbitrage_calculator import calculate_polymarket_fee
except ImportError:
    # Om importer misslyckas på grund av filstrukturen
    sys.path.append('src')
    from execution.portfolio_exit_executor import DEFAULT_CUTOFF_CENTS
    from arbitrage_calculator import calculate_polymarket_fee

def calculate_mock_kalshi_fee(price):
    return 0.07 * price * (1.0 - price)

# Vi använder nu den RIKTIGA kalkylatorn för Poly-avgifter!

def simulate_exit_scenario(name, k_holdings, p_holdings, k_bid, p_bid, k_vol, p_vol):
    cutoff = DEFAULT_CUTOFF_CENTS
    print(f"\n--- SCENARIO: {name} ---")
    
    # 1. Beräkna avgifter
    k_fee = calculate_mock_kalshi_fee(k_bid)
    # Vi använder kategorin 'Crypto' för att matcha dina BTC-marknader
    p_fee = calculate_polymarket_fee(p_bid, "Crypto")
    
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
print(f"Använder CURRENT_CUTOFF från koden: {DEFAULT_CUTOFF_CENTS}\n")

simulate_exit_scenario("A: Garanterat sälj (Hög marginal)", 100, 100, 0.9990, 0.05, 500, 500)
simulate_exit_scenario("B: För få kontrakt (2 st)", 2, 2, 0.99, 0.05, 500, 500)
simulate_exit_scenario("C: Under $1 på Poly (10 st @ $0.05)", 10, 10, 0.99, 0.05, 500, 500)
simulate_exit_scenario("E: Poly Pris 1.9 cent ($0.0190)", 200, 200, 0.9850, 0.0190, 500, 500)
simulate_exit_scenario("D: BTC Tight (Skip pga din cutoff)", 500, 500, 0.9650, 0.0300, 1000, 1000)
