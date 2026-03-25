import os
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import sys

def main():
    """
    Orchestrator script that:
    1. Runs the portfolio fetching script.
    2. Loads the resulting portfolio data.
    3. Generates a visual summary using Matplotlib.
    """
    # Define paths
    portfolio_script = os.path.join("src", "apis", "portfolio.py")
    csv_path = os.path.join("Data", "portfolio.csv")

    # 1. Execute portfolio.py to update Data/portfolio.csv
    print(f"--- Running {portfolio_script} ---")
    try:
        # Use sys.executable to ensure we use the same Python environment
        subprocess.run([sys.executable, portfolio_script], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running portfolio script: {e}")
        return

    # 2. Read the generated CSV
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Ensure portfolio.py runs correctly.")
        return

    print(f"--- Loading data from {csv_path} ---")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if df.empty:
        print("Portfolio data is empty. Nothing to visualize.")
        return

    # Data Preparation
    # Exclude Cash for the bar charts but keep for total calculation
    cash_df = df[df['Ticker'] == 'CASH']
    positions_df = df[df['Ticker'] != 'CASH'].copy()
    
    # Create a shortened title for better display
    positions_df['Display_Title'] = positions_df['Title'].apply(
        lambda x: (str(x)[:40] + '...') if len(str(x)) > 43 else str(x)
    )

    # Split by platform
    kalshi_pos = positions_df[positions_df['Platform'] == 'Kalshi'].sort_values('Value_USD', ascending=True)
    poly_pos = positions_df[positions_df['Platform'] == 'Polymarket'].sort_values('Value_USD', ascending=True)

    # Calculate Totals
    total_k = df[df['Platform'] == 'Kalshi']['Value_USD'].sum()
    total_p = df[df['Platform'] == 'Polymarket']['Value_USD'].sum()
    grand_total = total_k + total_p
    
    cash_k = cash_df[cash_df['Platform'] == 'Kalshi']['Value_USD'].sum()
    cash_p = cash_df[cash_df['Platform'] == 'Polymarket']['Value_USD'].sum()

    # 3. Visualization
    print("--- Generating Visualization ---")
    plt.style.use('ggplot')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
    
    # Colors
    color_kalshi = '#1f77b4' # Muted blue
    color_poly = '#e377c2'   # Muted pink/purple

    # Kalshi Horizontal Bar Chart
    if not kalshi_pos.empty:
        bars1 = ax1.barh(kalshi_pos['Display_Title'], kalshi_pos['Value_USD'], color=color_kalshi, alpha=0.8)
        ax1.set_title(f'Kalshi Positions\nTotal: ${total_k:.2f} (Cash: ${cash_k:.2f})', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Value in USD')
        # Add labels on the bars
        for bar in bars1:
            width = bar.get_width()
            ax1.text(width + (grand_total * 0.01), bar.get_y() + bar.get_height()/2, 
                     f'${width:.2f}', va='center', fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'No Kalshi Positions', ha='center', va='center')

    # Polymarket Horizontal Bar Chart
    if not poly_pos.empty:
        bars2 = ax2.barh(poly_pos['Display_Title'], poly_pos['Value_USD'], color=color_poly, alpha=0.8)
        ax2.set_title(f'Polymarket Positions\nTotal: ${total_p:.2f} (Cash: ${cash_p:.2f})', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Value in USD')
        # Add labels on the bars
        for bar in bars2:
            width = bar.get_width()
            ax2.text(width + (grand_total * 0.01), bar.get_y() + bar.get_height()/2, 
                     f'${width:.2f}', va='center', fontweight='bold')
    else:
        ax2.text(0.5, 0.5, 'No Polymarket Positions', ha='center', va='center')

    # Global Title
    plt.suptitle(f'PolyKalshi Combined Portfolio Summary\nGrand Total Value: ${grand_total:.2f}', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.93])

    # Show the plot
    print("--- Opening Visualization Window ---")
    plt.show()

if __name__ == "__main__":
    main()
