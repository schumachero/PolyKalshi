import pandas as pd
import matplotlib.pyplot as plt
import os

# Konfiguration
HISTORY_CSV = os.path.join("Data", "portfolio_history.csv")

def plot_history():
    """
    Läser historikdata från CSV och skapar en graf över portföljvärdet över tid.
    """
    if not os.path.exists(HISTORY_CSV):
        print(f"Fel: Hittade inte historikfilen {HISTORY_CSV}. Kör log_portfolio_history.py först.")
        return

    try:
        # 1. Läs in data
        df = pd.read_csv(HISTORY_CSV)
        
        if df.empty:
            print("Historikfilen är tom.")
            return

        # 2. Förbered data
        # Konvertera tidsstämpel-strängen till riktiga datetime-objekt
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        # Sortera efter tid ifall rader lagts till i fel ordning
        df = df.sort_values('Timestamp')

        # 3. Skapa grafen
        plt.style.use('ggplot') # Använd en snygg stil
        fig, ax = plt.subplots(figsize=(12, 7))

        # Plotta linjen
        ax.plot(df['Timestamp'], df['Total_Value_USD'], 
                marker='o', 
                linestyle='-', 
                color='#2980b9', 
                linewidth=2.5, 
                markersize=8,
                label='Total Portfolio Value')

        # Fyll ytan under linjen för en "area chart" effekt
        ax.fill_between(df['Timestamp'], df['Total_Value_USD'], color='#3498db', alpha=0.1)

        # 4. Formatering och design
        ax.set_title('Portföljens värdeutveckling över tid', fontsize=18, fontweight='bold', pad=20)
        ax.set_xlabel('Tidpunkt', fontsize=13, labelpad=10)
        ax.set_ylabel('Värde (USD)', fontsize=13, labelpad=10)
        
        # Lägg till dollar-tecken på Y-axeln
        from matplotlib.ticker import StrMethodFormatter
        ax.yaxis.set_major_formatter(StrMethodFormatter('${x:,.2f}'))

        # Rotera datumetiketter för bättre läsbarhet
        plt.xticks(rotation=45)
        
        ax.grid(True, linestyle='--', alpha=0.6)
        
        # Visa det senaste värdet som en textnotis i grafen
        last_val = df.iloc[-1]['Total_Value_USD']
        last_date = df.iloc[-1]['Timestamp']
        ax.annotate(f'Aktuellt: ${last_val:.2f}', 
                    xy=(last_date, last_val), 
                    xytext=(10, 10), 
                    textcoords='offset points',
                    fontsize=12,
                    fontweight='bold',
                    color='#c0392b',
                    arrowprops=dict(arrowstyle='->', color='#c0392b'))

        plt.tight_layout()
        
        print(f"Genererar graf baserat på {len(df)} mätpunkter...")
        print("Öppnar ett fönster för att visa grafen...")
        plt.show()

    except Exception as e:
        print(f"Ett fel uppstod vid skapande av graf: {e}")

if __name__ == "__main__":
    plot_history()
