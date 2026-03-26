import os
import csv
import datetime
import subprocess
import pandas as pd
import sys

# Configuration
PORTFOLIO_SCRIPT = os.path.join("src", "apis", "portfolio.py")
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
HISTORY_CSV = os.path.join("Data", "portfolio_history.csv")

def run_portfolio_fetcher():
    """
    Kör portfolio.py för att hämta senaste data från Kalshi och Polymarket.
    Detta skapar Data/portfolio.csv.
    """
    print(f"Hämtar senaste portföljdata genom att köra {PORTFOLIO_SCRIPT}...")
    try:
        # Kör skriptet tyst för att inte skräpa ner terminalen (om det inte körs i interaktivt läge)
        subprocess.run([sys.executable, PORTFOLIO_SCRIPT], check=True, stdout=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Fel vid körning av portföljskriptet: {e}")
        return False
    except Exception as e:
        print(f"Ett oväntat fel uppstod vid hämtning: {e}")
        return False

def calculate_current_total():
    """
    Läser portfolio.csv och räknar ut det totala värdet.
    Summan inkluderar både CASH och aktuella positionsvärden.
    """
    if not os.path.exists(PORTFOLIO_CSV):
        print(f"Fel: Hittade inte {PORTFOLIO_CSV}. Se till att portfolio.py fungerar.")
        return None

    try:
        df = pd.read_csv(PORTFOLIO_CSV)
        if df.empty:
            print("Portföljen är tom i CSV-filen.")
            return 0.0
        
        # Vi summerar Value_USD-kolumnen för att få totalt kapital.
        total = df["Value_USD"].sum()
        return total
    except Exception as e:
        print(f"Fel vid läsning av CSV eller beräkning av totalvärde: {e}")
        return None

def log_to_history(total_value):
    """
    Sparar det totala värdet med en tidsstämpel i historikfilen (append).
    """
    if total_value is None:
        return

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = ["Timestamp", "Total_Value_USD"]
    
    file_exists = os.path.isfile(HISTORY_CSV)
    
    try:
        # Säkerställ att Data-mappen finns
        os.makedirs(os.path.dirname(HISTORY_CSV), exist_ok=True)
        
        with open(HISTORY_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Skriv rubrik om det är en ny fil
            if not file_exists:
                writer.writerow(header)
            writer.writerow([now, f"{total_value:.2f}"])
        
        print(f"Historik uppdaterad i {HISTORY_CSV}:")
        print(f"  Tid: {now}")
        print(f"  Värde: ${total_value:.2f}")
    except Exception as e:
        print(f"Kunde inte skriva till historikfilen: {e}")

def main():
    # 1. Hämta den senaste datan från API:erna (via portfolio.py)
    if run_portfolio_fetcher():
        # 2. Beräkna totalt värde från den nyskapade CSV-filen
        total = calculate_current_total()
        # 3. Logga resultatet till historiken
        log_to_history(total)

if __name__ == "__main__":
    main()
