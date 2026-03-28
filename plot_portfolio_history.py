import pandas as pd
import matplotlib.pyplot as plt
import os

# Configuration
HISTORY_CSV = os.path.join("Data", "portfolio_history.csv")

def plot_history():
    """
    Reads history data from CSV and creates a chart of portfolio value over time.
    """
    if not os.path.exists(HISTORY_CSV):
        print(f"Error: Could not find history file {HISTORY_CSV}. Run log_portfolio_history.py first.")
        return

    try:
        # 1. Load data
        df = pd.read_csv(HISTORY_CSV)
        
        if df.empty:
            print("History file is empty.")
            return

        # 2. Prepare data
        # Convert timestamp string to proper datetime objects
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        # Sort by time in case rows were added out of order
        df = df.sort_values('Timestamp')

        # 3. Create the chart
        plt.style.use('ggplot') # Use a nice style
        fig, ax = plt.subplots(figsize=(12, 7))

        # Plot the line
        ax.plot(df['Timestamp'], df['Total_Value_USD'], 
                marker='o', 
                linestyle='-', 
                color='#2980b9', 
                linewidth=2.5, 
                markersize=8,
                label='Total Portfolio Value')

        # Fill the area under the line for an "area chart" effect
        ax.fill_between(df['Timestamp'], df['Total_Value_USD'], color='#3498db', alpha=0.1)

        # 4. Formatting and design
        ax.set_title('Portfolio Value Over Time', fontsize=18, fontweight='bold', pad=20)
        ax.set_xlabel('Time', fontsize=13, labelpad=10)
        ax.set_ylabel('Value (USD)', fontsize=13, labelpad=10)
        
        # Add dollar signs to the Y-axis
        from matplotlib.ticker import StrMethodFormatter
        ax.yaxis.set_major_formatter(StrMethodFormatter('${x:,.2f}'))

        # Rotate date labels for better readability
        plt.xticks(rotation=45)
        
        ax.grid(True, linestyle='--', alpha=0.6)
        
        # Show the latest value as a text note in the chart
        last_val = df.iloc[-1]['Total_Value_USD']
        last_date = df.iloc[-1]['Timestamp']
        ax.annotate(f'Current: ${last_val:.2f}', 
                    xy=(last_date, last_val), 
                    xytext=(10, 10), 
                    textcoords='offset points',
                    fontsize=12,
                    fontweight='bold',
                    color='#c0392b',
                    arrowprops=dict(arrowstyle='->', color='#c0392b'))

        plt.tight_layout()
        
        print(f"Generating chart based on {len(df)} data points...")
        print("Opening a window to show the chart...")
        plt.show()

    except Exception as e:
        print(f"An error occurred while creating the chart: {e}")

if __name__ == "__main__":
    plot_history()
