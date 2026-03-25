import os
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import sys

import textwrap

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
    
    # Create a wrapped title for better display, including Quantity and Side
    def process_title(row):
        qty = row['Quantity']
        # If integer, show as int, else keep float
        qty_str = f"{int(qty)}" if float(qty).is_integer() else f"{float(qty):.2f}"
        side = str(row['Side']).upper()
        base_title = f"[{qty_str} {side}] {str(row['Title'])}"
        # Wrap to ~35 characters per line for better fitting in smaller window
        return "\n".join(textwrap.wrap(base_title, width=35))

    positions_df['Display_Title'] = positions_df.apply(process_title, axis=1)

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
    # Reduced figure size so it fits without fullscreen
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 9))
    
    # Color logic: Green for YES, Red for NO
    def get_colors(df_subset):
        return ['#2ecc71' if s == 'YES' else '#e74c3c' for s in df_subset['Side']]

    # Kalshi Horizontal Bar Chart
    if not kalshi_pos.empty:
        bars1 = ax1.barh(kalshi_pos['Display_Title'], kalshi_pos['Value_USD'], 
                         color=get_colors(kalshi_pos), alpha=0.8)
        ax1.set_title(f'Kalshi Positions\nTotal: ${total_k:.2f}', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Value in USD', fontsize=11)
        ax1.tick_params(axis='y', labelsize=9)
        # Add labels on the bars
        for bar in bars1:
            width = bar.get_width()
            ax1.text(width + (grand_total * 0.005), bar.get_y() + bar.get_height()/2, 
                     f'${width:.2f}', va='center', fontweight='bold', fontsize=10)
    else:
        ax1.text(0.5, 0.5, 'No Kalshi Positions', ha='center', va='center')

    # Polymarket Horizontal Bar Chart
    if not poly_pos.empty:
        bars2 = ax2.barh(poly_pos['Display_Title'], poly_pos['Value_USD'], 
                         color=get_colors(poly_pos), alpha=0.8)
        ax2.set_title(f'Polymarket Positions\nTotal: ${total_p:.2f}', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Value in USD', fontsize=11)
        ax2.tick_params(axis='y', labelsize=9)
        # Add labels on the bars
        for bar in bars2:
            width = bar.get_width()
            ax2.text(width + (grand_total * 0.005), bar.get_y() + bar.get_height()/2, 
                     f'${width:.2f}', va='center', fontweight='bold', fontsize=10)
    else:
        ax2.text(0.5, 0.5, 'No Polymarket Positions', ha='center', va='center')

    # Global Title
    plt.suptitle(f'PolyKalshi Combined Portfolio Summary\nGrand Total: ${grand_total:.2f} (Total Cash: ${cash_k + cash_p:.2f})', 
                 fontsize=18, fontweight='bold', y=0.98)
    
    # Finer control for smaller window
    fig.subplots_adjust(wspace=0.45, left=0.18, right=0.92)
    plt.tight_layout(rect=[0, 0.03, 1, 0.90])

    # Show the plot
    print("--- Opening Visualization Window ---")
    plt.show()

if __name__ == "__main__":
    main()
