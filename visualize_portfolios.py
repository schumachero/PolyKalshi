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
    
    # 1. Separate platforms
    k_df = positions_df[positions_df['Platform'] == 'Kalshi'].copy()
    p_df = positions_df[positions_df['Platform'] == 'Polymarket'].copy()
    
    # 2. Create a stable PairID to align them
    def get_pair_key(row):
        t = str(row['Ticker'])
        m = str(row.get('Matched_Ticker', ''))
        # If matched, the ID is the sorted tuple of both tickers
        if pd.isna(m) or m.lower() in ['nan', '', 'none']:
            return tuple(sorted([t]))
        return tuple(sorted([t, m]))

    k_df['PairID'] = k_df.apply(get_pair_key, axis=1)
    p_df['PairID'] = p_df.apply(get_pair_key, axis=1)
    
    # 3. Aggregate into unique pairs
    all_pids = sorted(list(set(k_df['PairID'].tolist() + p_df['PairID'].tolist())))
    pair_list = []
    
    for pid in all_pids:
        k_row = k_df[k_df['PairID'] == pid]
        p_row = p_df[p_df['PairID'] == pid]
        
        # Extract data, preferring Kalshi for the primary title
        k_val = k_row['Value_USD'].sum() if not k_row.empty else 0
        p_val = p_row['Value_USD'].sum() if not p_row.empty else 0
        
        title = k_row['Title'].iloc[0] if not k_row.empty else p_row['Title'].iloc[0]
        
        pair_list.append({
            'PairID': pid,
            'Title': title,
            'K_Val': k_val,
            'P_Val': p_val,
            'K_Qty': k_row['Quantity'].iloc[0] if not k_row.empty else 0,
            'P_Qty': p_row['Quantity'].iloc[0] if not p_row.empty else 0,
            'K_Side': k_row['Side'].iloc[0] if not k_row.empty else '',
            'P_Side': p_row['Side'].iloc[0] if not p_row.empty else '',
            'MaxVal': max(k_val, p_val)
        })
    
    # Sort by MaxVal ascending so largest is at the top in barh
    aligned_df = pd.DataFrame(pair_list).sort_values('MaxVal', ascending=True)
    
    # Wrap Y-labels
    aligned_df['Y_Label'] = aligned_df['Title'].apply(lambda x: "\n".join(textwrap.wrap(str(x), width=30)))

    # Calculate Totals
    total_k = df[df['Platform'] == 'Kalshi']['Value_USD'].sum()
    total_p = df[df['Platform'] == 'Polymarket']['Value_USD'].sum()
    grand_total = total_k + total_p
    
    cash_k = cash_df[cash_df['Platform'] == 'Kalshi']['Value_USD'].sum()
    cash_p = cash_df[cash_df['Platform'] == 'Polymarket']['Value_USD'].sum()

    # 3. Visualization
    print("--- Generating Aligned Visualization ---")
    plt.style.use('ggplot')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10))
    
    # Colors: Green for YES, Red for NO, Gray for missing
    def side_color(side):
        if side == 'YES': return '#2ecc71'
        if side == 'NO': return '#e74c3c'
        return '#bdc3c7'

    # Helper for bar labels [Qty SIDE]
    def get_qty_label(qty, side, value):
        if value <= 0: return ""
        q_val = float(qty)
        qty_str = f"{int(q_val)}" if q_val.is_integer() else f"{q_val:.2f}"
        return f"[{qty_str} {side}]  ${value:.2f}"

    # Kalshi Plot
    colors_k = [side_color(s) for s in aligned_df['K_Side']]
    bars1 = ax1.barh(aligned_df['Y_Label'], aligned_df['K_Val'], color=colors_k, alpha=0.8)
    ax1.set_title(f'Kalshi\nBets: ${total_k - cash_k:.2f}', fontsize=16, fontweight='bold')
    ax1.tick_params(axis='y', labelsize=11)
    
    for i, bar in enumerate(bars1):
        v = aligned_df.iloc[i]['K_Val']
        label = get_qty_label(aligned_df.iloc[i]['K_Qty'], aligned_df.iloc[i]['K_Side'], v)
        if v > 0:
            ax1.text(v + (grand_total * 0.005), bar.get_y() + bar.get_height()/2, label, 
                     va='center', fontsize=11, fontweight='bold')

    # Polymarket Plot
    colors_p = [side_color(s) for s in aligned_df['P_Side']]
    bars2 = ax2.barh(aligned_df['Y_Label'], aligned_df['P_Val'], color=colors_p, alpha=0.8)
    ax2.set_title(f'Polymarket\nBets: ${total_p - cash_p:.2f}', fontsize=16, fontweight='bold')
    ax2.tick_params(axis='y', labelsize=11) # Re-enabled and increased for visibility
    
    for i, bar in enumerate(bars2):
        v = aligned_df.iloc[i]['P_Val']
        label = get_qty_label(aligned_df.iloc[i]['P_Qty'], aligned_df.iloc[i]['P_Side'], v)
        if v > 0:
            ax2.text(v + (grand_total * 0.005), bar.get_y() + bar.get_height()/2, label, 
                     va='center', fontsize=11, fontweight='bold')

    # Global Title with breakdown
    total_cash = cash_k + cash_p
    total_bets = (total_k - cash_k) + (total_p - cash_p)
    
    plt.suptitle(
        f'PolyKalshi Aligned Portfolio Summary\n'
        f'Cash: ${total_cash:.2f}  |  Bets: ${total_bets:.2f}  |  Total: ${grand_total:.2f}', 
        fontsize=20, fontweight='bold', y=0.98
    )
    
    # Increase widths/margins to prevent labels from being cut off
    fig.subplots_adjust(wspace=0.6, left=0.25, right=0.90)
    plt.tight_layout(rect=[0, 0.03, 1, 0.88])

    # Show the plot
    print("--- Opening Visualization Window ---")
    plt.show()

if __name__ == "__main__":
    main()
