import sys
import os

# Ensure we can import from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.equity_system import EquityTracker

def get_float_input(prompt):
    while True:
        try:
            val = input(prompt)
            # Allow cancellation
            if val.strip() == "":
                return None
            return float(val)
        except ValueError:
            print("Vänligen ange ett giltigt nummer (eller tryck Enter för att avbryta).")

def print_status(tracker):
    print("\n--- NUVARANDE PORTFÖLJSTATUS ---")
    val = get_float_input("Ange portföljens nuvarande totala värde (sek/usd/etc): ")
    if val is None:
        return
        
    status = tracker.get_status(val)
    
    print("\n" + "="*60)
    print(f"Total Värde: {status['total_portfolio_value']:.2f}")
    print(f"Totalt Antal Andelar: {status['total_shares']:.4f}")
    print(f"Pris per Andel (NAV): {status['price_per_share']:.4f}")
    print("=" * 60)
    
    for name, data in status["investor_status"].items():
        if data['shares'] > 1e-9:
            print(f"Investerare: {name}")
            print(f"  Ägarandel: {data['percentage']:.2f}%")
            print(f"  Nuvarande Värde: {data['value']:.2f}")
            print(f"  Andelar:   {data['shares']:.4f}")
            print(f"  Investerat:{data['total_invested']:.2f} | Uttaget: {data['total_withdrawn']:.2f}")
            
            # Simple color for profit
            if data['profit'] >= 0:
                print(f"  Vinst:     +\033[92m{data['profit']:.2f}\033[0m")
            else:
                print(f"  Förlust:   \033[91m{data['profit']:.2f}\033[0m")
            print("-" * 60)

def buy_in(tracker):
    print("\n--- KÖP IN (Insättning) ---")
    print("Vid första insättningen spelar portföljens värde ingen roll (sätt tex till 0).")
    
    name = input("Investerarens namn (enter för att avbryta): ").strip()
    if not name:
        return
        
    amount = get_float_input(f"Hur mycket sätter {name} in? ")
    if amount is None: return
    
    portfolio_val = get_float_input("Vad var portföljens värde INNAN insättningen (exklusive dessa pengar)? ")
    if portfolio_val is None: return
    
    try:
        shares, price = tracker.buy_in(name, amount, portfolio_val)
        print(f"\n✅ Framgång! {name} köpte {shares:.4f} andelar till priset {price:.4f} per andel.")
    except Exception as e:
        print(f"\n❌ Fel: {e}")

def sell_out(tracker):
    print("\n--- SÄLJ AV (Uttag) ---")
    name = input("Investerarens namn (enter för att avbryta): ").strip()
    if not name:
        return
        
    amount = get_float_input(f"Vilket belopp i värde vill {name} ta ut? ")
    if amount is None: return
    
    portfolio_val = get_float_input("Vad är portföljens värde INNAN detta uttag (inklusive dessa pengar)? ")
    if portfolio_val is None: return
    
    try:
        shares, price = tracker.sell_out(name, amount, portfolio_val)
        print(f"\n✅ Framgång! {name} sålde {shares:.4f} andelar (pris: {price:.4f}/andel) för att få ut {amount:.2f}.")
    except Exception as e:
        print(f"\n❌ Fel: {e}")

def main():
    tracker_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Data', 'equity_balances.json'))
    print(f"Laddar data från: {tracker_path}")
    tracker = EquityTracker(data_file=tracker_path)
    
    while True:
        print("\n=== PORTFÖLJ AKTIESYSTEM ===")
        print("1. Visa portföljstatus")
        print("2. Registrera köp/insättning")
        print("3. Registrera sälj/uttag")
        print("4. Avsluta")
        
        choice = input("\nVälj ett alternativ (1-4): ").strip()
        if choice == '1':
            print_status(tracker)
        elif choice == '2':
            buy_in(tracker)
        elif choice == '3':
            sell_out(tracker)
        elif choice == '4':
            print("Avslutar...")
            break
        else:
            print("Ogiltigt val. Försök igen.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAvslutar...")
