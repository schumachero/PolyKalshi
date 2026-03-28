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
    Runs portfolio.py to fetch the latest data from Kalshi and Polymarket.
    This creates Data/portfolio.csv.
    """
    print(f"Fetching latest portfolio data by running {PORTFOLIO_SCRIPT}...")
    try:
        # Run the script silently to avoid cluttering the terminal (unless running in interactive mode)
        subprocess.run([sys.executable, PORTFOLIO_SCRIPT], check=True, stdout=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running portfolio script: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during fetching: {e}")
        return False

def calculate_current_total():
    """
    Reads portfolio.csv and calculates the total value.
    The sum includes both CASH and current position values.
    """
    if not os.path.exists(PORTFOLIO_CSV):
        print(f"Error: Could not find {PORTFOLIO_CSV}. Ensure portfolio.py is working.")
        return None

    try:
        df = pd.read_csv(PORTFOLIO_CSV)
        if df.empty:
            print("Portfolio is empty in the CSV file.")
            return 0.0
        
        # Sum the Value_USD column to get total capital.
        total = df["Value_USD"].sum()
        return total
    except Exception as e:
        print(f"Error reading CSV or calculating total value: {e}")
        return None

def log_to_history(total_value):
    """
    Saves the total value and total units with a timestamp in the history file.
    """
    if total_value is None:
        return

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = ["Timestamp", "Total_Value_USD", "Total_Units"]
    
    file_exists = os.path.isfile(HISTORY_CSV)
    total_units = total_value # Default to value if it's the first entry (Initial Price = 1.0)
    
    try:
        # Read the previous units if the file exists
        if file_exists:
            df_existing = pd.read_csv(HISTORY_CSV)
            if not df_existing.empty and "Total_Units" in df_existing.columns:
                total_units = df_existing.iloc[-1]["Total_Units"]
        
        # Ensure the Data folder exists
        os.makedirs(os.path.dirname(HISTORY_CSV), exist_ok=True)
        
        with open(HISTORY_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header if it's a new file
            if not file_exists:
                writer.writerow(header)
            writer.writerow([now, f"{total_value:.2f}", f"{total_units:.4f}"])
        
        print(f"History updated in {HISTORY_CSV}:")
        print(f"  Time: {now}")
        print(f"  Value: ${total_value:.2f}")
        print(f"  Units: {total_units:.4f}")
        if total_units > 0:
            print(f"  Share Price: ${total_value/total_units:.4f}")
    except Exception as e:
        print(f"Could not write to history file: {e}")

def main():
    # 1. Fetch latest data from APIs (via portfolio.py)
    if run_portfolio_fetcher():
        # 2. Calculate total value from the newly created CSV file
        total = calculate_current_total()
        # 3. Log the result to history
        log_to_history(total)

if __name__ == "__main__":
    main()
