import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
import textwrap
import traceback
import csv
import json
import base64
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# --- CLEAN PATH SETUP ---
import sys
import os

# Entry point is src/frontend_dashboard.py
SRC_DIR = os.path.dirname(os.path.abspath(__file__)) # .../src
PROJECT_ROOT = os.path.dirname(SRC_DIR)             # .../

# Add project root and src to sys.path
for path in [PROJECT_ROOT, SRC_DIR]:
    if path not in sys.path:
        sys.path.insert(0, path)

# 1) Try to load explicit .env file from project root (for local usage)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# 2) Sync Streamlit Secrets into environment variables (for cloud usage)
try:
    for k, v in st.secrets.items():
        if isinstance(v, str) and k not in os.environ:
            os.environ[k] = v
except Exception:
    pass

# Define defaults first to avoid NameError if imports fail non-traditionally
def get_kalshi_positions(): return []
def get_polymarket_positions(): return []
def get_kalshi_balance(): return {}
def get_polymarket_balance(addr): return 0
def generate_semantic_matches(k, p, threshold=0.3): return pd.DataFrame()

# Use absolute 'src.' imports (standard for Streamlit Cloud with src/ folder)
try:
    from src.apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance, get_kalshi_recent_trades, get_polymarket_recent_trades
    from src.matching.semantic_matching import generate_semantic_matches
except ImportError:
    try:
        # Fallback for environments where 'src' is the root or already in path
        from apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance, get_kalshi_recent_trades, get_polymarket_recent_trades
        from matching.semantic_matching import generate_semantic_matches
    except ImportError as e:
        st.error(f"Import Warning: {e}. Dashboard functionality may be limited.")

# --- CONFIGURATION ---
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
HISTORY_CSV = "Data/portfolio_history.csv"
CAPITAL_CHANGES_CSV = "Data/capital_changes.csv"

# --- CORE INVESTORS ---
CORE_INVESTORS = ["Arvid Hedin", "Arvid Axelsson", "David Hallkvist", "Elis Graipe", "Erik Schaine"]
PERFORMANCE_FEE_RATE = 0.20 # 20% of profit sharing

def push_to_github(file_path, content, message):
    """
    Commits a file to GitHub using the REST API.
    Requires GH_TOKEN and GH_REPO in st.secrets.
    """
    try:
        token = st.secrets.get("GH_TOKEN")
        repo = st.secrets.get("GH_REPO", "ErikS003/PolyKalshi")
        
        if not token:
            print("GH_TOKEN not found in secrets. Skipping GitHub push.")
            return False

        url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        # 1. Get the current file's SHA (required for update)
        res = requests.get(url, headers=headers)
        sha = None
        if res.status_code == 200:
            sha = res.json().get("sha")

        # 2. Push the update
        payload = {
            "message": message,
            "content": base64.b64encode(content.encode()).decode(),
        }
        if sha:
            payload["sha"] = sha

        put_res = requests.put(url, headers=headers, json=payload)
        if put_res.status_code in [200, 201]:
            st.toast(f"✅ Committed to GitHub: {os.path.basename(file_path)}")
            return True
        else:
            st.error(f"GitHub Push Error ({put_res.status_code}): {put_res.text}")
            return False
    except Exception as e:
        st.error(f"GitHub API Exception: {e}")
        return False

EXIT_TARGET = 0.99
INVEST_TARGET = 0.95
DISPLAY_TIMEZONE = ZoneInfo("Europe/Stockholm")
VOLUME_PERCENTILE_THRESHOLD = 0.20 # 20% of position (decimal)
VOLUME_FIXED_THRESHOLD = 10 # $10 worth

# Check for API keys
KALSHI_KEY_READY = os.getenv("KALSHI_ACCESS_KEY") is not None
POLY_KEY_READY = os.getenv("POLYMARKET_WALLET_ADDRESS") is not None
WALLET_ADDR = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")

# Page Config
st.set_page_config(
    page_title="PolyKalshi Mastery",
    page_icon="💎",
    layout="wide",
)

# --- THEME & STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Outfit', sans-serif; }
    .stMetric { background: #1e293b; border-radius: 15px; padding: 15px !important; border: 1px solid #334155; }
    h1, h2, h3, h4 { color: #f8fafc; font-weight: 700; }
    .status-box { padding: 8px; border-radius: 8px; margin-bottom: 8px; font-size: 0.8em; text-align: center; }
    .status-ok { background: #065f46; color: #34d399; border: 1px solid #059669; }
    .status-missing { background: #7f1d1d; color: #f87171; border: 1px solid #b91c1c; }
    .start-date-badge { color: #94a3b8; font-size: 0.9rem; background: #1e293b; padding: 8px 15px; border-radius: 12px; border: 1px solid #334155; display: inline-block; float: right; margin-top: 15px; }
</style>
""", unsafe_allow_html=True)

# --- UTILITIES ---

def wrap_label(text, width=40): # Widened wrap
    if not text: return ""
    return "<br>".join(textwrap.wrap(str(text), width=width))

def check_password():
    """Returns `True` if the user has correct password or if no password is required."""
    if not DASHBOARD_PASSWORD:
        return True

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == DASHBOARD_PASSWORD:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    # Show input for password
    st.markdown("### 🔒 Terminal Access Restricted")
    st.text_input(
        "Enter system access key:", type="password", on_change=password_entered, key="password"
    )
    if "password_correct" in st.session_state:
        st.error("😕 Access Key incorrect")
    return False

def transform_to_dataframe(k_pos, p_pos):
    """Consolidates raw API dictionary lists into a unified DataFrame."""
    rows = []
    # Process Kalshi
    for p in k_pos:
        rows.append({
            "Platform": "Kalshi",
            "Ticker": p["ticker"],
            "Title": p["title"],
            "Side": p["side"],
            "Quantity": p["quantity"],
            "Price": p.get("current_price", 0.0),
            "Value_USD": p.get("market_exposure_cents", 0.0) / 100,
            "Profit_USD": p.get("realized_pnl_cents", 0.0) / 100,
            "Matched_Ticker": p.get("matched_ticker", ""),
            "Match_Score": p.get("match_score", 0.0),
            "close_time": p.get("close_time", ""),
            "rules": p.get("rules", "")
        })
    # Process Polymarket
    for p in p_pos:
        rows.append({
            "Platform": "Polymarket",
            "Ticker": p["market_id"],
            "Title": p["title"],
            "Side": p["side"],
            "Quantity": p["size"],
            "Price": p.get("current_price", 0.0),
            "Value_USD": p.get("current_value", 0.0),
            "Profit_USD": p.get("pnl", 0.0),
            "Matched_Ticker": p.get("matched_ticker", ""),
            "Match_Score": p.get("match_score", 0.0),
            "close_time": p.get("close_time", ""),
            "rules": p.get("rules", "")
        })
    return pd.DataFrame(rows)

# --- EQUITY SYSTEM UTILITIES ---

def load_capital_changes():
    if not os.path.exists(CAPITAL_CHANGES_CSV):
        return pd.DataFrame(columns=["Timestamp", "Investor", "Type", "Amount_USD", "Units_Adjusted", "Price_At_Time"])
    return pd.read_csv(CAPITAL_CHANGES_CSV)

def get_investor_balances(current_nav):
    cap_df = load_capital_changes()
    if cap_df.empty:
        return pd.DataFrame(), 0.0, 1.0

    # Calculate total units
    total_units = cap_df["Units_Adjusted"].sum()
    current_price = current_nav / total_units if total_units > 0 else 1.0

    balances = []
    investors = cap_df["Investor"].unique()
    
    for inv in investors:
        inv_df = cap_df[cap_df["Investor"] == inv]
        shares = inv_df["Units_Adjusted"].sum()
        
        # Breakdown
        buys = inv_df[inv_df["Type"] == "BUY"]["Amount_USD"].sum()
        sells = inv_df[inv_df["Type"] == "SELL"]["Amount_USD"].sum()
        net_invested = buys - sells
        
        # Fee info (for Core users)
        fee_income_shares = inv_df[inv_df["Type"] == "FEE_TRANSFER"]["Units_Adjusted"].sum()
        fee_income_usd = fee_income_shares * current_price
        
        current_value = shares * current_price
        profit = current_value - net_invested
        growth_pct = (profit / net_invested * 100) if net_invested > 0 else 0.0
        
        hwm_price = inv_df["Price_At_Time"].max() if not inv_df.empty else 1.0
        
        balances.append({
            "Name": inv,
            "Shares": shares,
            "Inflow ($)": buys,
            "Outflow ($)": sells,
            "Net Invested": net_invested,
            "Current Value": current_value,
            "Profit": profit,
            "Fee Income ($)": fee_income_usd if inv in CORE_INVESTORS else 0.0,
            "Growth %": growth_pct,
            "Equity %": (shares / total_units * 100) if total_units > 0 else 0.0,
            "Is Core": inv in CORE_INVESTORS,
            "HWM": hwm_price
        })
        
    return pd.DataFrame(balances), total_units, current_price

def get_equity_history(h_df, cap_df):
    """Reconstructs historical value for each investor."""
    if h_df.empty or cap_df.empty:
        return pd.DataFrame()
    
    # Ensure timestamps are comparable
    h_df = h_df.copy()
    h_df['Timestamp'] = pd.to_datetime(h_df['Timestamp'])
    cap_df = cap_df.copy()
    cap_df['Timestamp'] = pd.to_datetime(cap_df['Timestamp'])
    
    investors = cap_df['Investor'].unique()
    history_records = []
    
    for _, h_row in h_df.iterrows():
        ts = h_row['Timestamp']
        total_val = h_row['Total_Value_USD']
        total_units = h_row['Total_Units']
        price = total_val / total_units if total_units > 0 else 1.0
        
        for inv in investors:
            # All transactions up to this point
            inv_trans = cap_df[(cap_df['Investor'] == inv) & (cap_df['Timestamp'] <= ts)]
            inv_shares = inv_trans['Units_Adjusted'].sum()
            
            if inv_shares > 1e-6: # Only track if they have holdings
                inv_val = inv_shares * price
                # Calculate basis at this point
                inv_buys = inv_trans[inv_trans['Type'] == 'BUY']['Amount_USD'].sum()
                inv_sells = inv_trans[inv_trans['Type'] == 'SELL']['Amount_USD'].sum()
                inv_cost = inv_buys - inv_sells
                inv_profit = inv_val - inv_cost
                
                history_records.append({
                    "Timestamp": ts,
                    "Investor": inv,
                    "Value": inv_val,
                    "Profit": inv_profit,
                    "Growth %": (inv_profit / inv_cost * 100) if inv_cost > 0 else 0.0
                })
                
    return pd.DataFrame(history_records)

@st.cache_data(ttl=600)
def get_dashboard_data():
    """Tries live API first, falls back to local CSV."""
    if KALSHI_KEY_READY and POLY_KEY_READY:
        try:
            # 1. Fetch Positions
            with st.spinner("🛰️ Fetching Live Market Positions..."):
                k_pos = get_kalshi_positions()
                p_pos = get_polymarket_positions()
                df = transform_to_dataframe(k_pos, p_pos)
            
            # 2. RUN SEMANTIC MATCHING ON LIVE DATA
            with st.spinner("🧠 Finding Hedge Pairs (Semantic Matching)..."):
                try:
                    # Filter for positions (ignoring any existing CASH rows)
                    k_df = df[df['Platform'] == 'Kalshi'].rename(columns={'Ticker':'market_ticker', 'Title':'market_title', 'rules':'rules_text'})
                    p_df = df[df['Platform'] == 'Polymarket'].rename(columns={'Ticker':'market_ticker', 'Title':'market_title', 'rules':'rules_text'})
                    
                    if not k_df.empty and not p_df.empty:
                        matches = generate_semantic_matches(k_df, p_df, threshold=0.45) # Raised threshold for accuracy
                        
                        # Use sets to ensure 1-to-1 matching and avoid overwriting better matches
                        matched_kalshi = set()
                        matched_poly = set()

                        # Map matches back
                        for _, m in matches.iterrows():
                            kt = m['kalshi_market_ticker']
                            pt = m['polymarket_market_ticker']
                            score = m['semantic_score']
                            
                            if kt not in matched_kalshi and pt not in matched_poly:
                                # Inject into main DF (only the first/best match)
                                df.loc[(df['Platform'] == 'Kalshi') & (df['Ticker'] == kt), 'Matched_Ticker'] = pt
                                df.loc[(df['Platform'] == 'Kalshi') & (df['Ticker'] == kt), 'Match_Score'] = score
                                df.loc[(df['Platform'] == 'Polymarket') & (df['Ticker'] == pt), 'Matched_Ticker'] = kt
                                df.loc[(df['Platform'] == 'Polymarket') & (df['Ticker'] == pt), 'Match_Score'] = score
                                
                                matched_kalshi.add(kt)
                                matched_poly.add(pt)
                except Exception as e_match:
                    st.warning(f"Semantic Matching on cloud failed: {e_match}")

            # 3. Fetch Cash
            with st.spinner("💰 Calculating Cash Balances..."):
                k_bal = get_kalshi_balance()
                k_cash = 0
                if k_bal and isinstance(k_bal, dict):
                    k_cash = k_bal.get('available_cents', 0) / 100
                
                p_cash = get_polymarket_balance(WALLET_ADDR)
                
                cash_rows = [
                    {"Platform": "Kalshi", "Ticker": "CASH", "Title": "Kalshi Available Cash", "Side": "N/A", "Value_USD": k_cash, "Profit_USD": 0, "Quantity": k_cash, "Price": 1.0, "close_time": "", "rules": ""},
                    {"Platform": "Polymarket", "Ticker": "CASH", "Title": "Polymarket USDC.e", "Side": "N/A", "Value_USD": p_cash, "Profit_USD": 0, "Quantity": p_cash, "Price": 1.0, "close_time": "", "rules": ""}
                ]
                df = pd.concat([df, pd.DataFrame(cash_rows)], ignore_index=True)
                
            if not df.empty:
                return df, "Live API"
        except Exception as e:
            err_msg = traceback.format_exc()
            st.error(f"Live fetch failed: {e}")
            with st.expander("🔍 Show Debug Traceback"):
                st.code(err_msg)
    
    # Fallback to local CSV
    if os.path.exists(PORTFOLIO_CSV):
        df = pd.read_csv(PORTFOLIO_CSV)
        # Standardize columns if reading from old CSV
        if 'P&L_USD' in df.columns:
            df = df.rename(columns={'P&L_USD': 'Profit_USD'})
        return df, "Local CSV"
    
    return pd.DataFrame(), "No Data"

# --- MAIN UI ---

def main():
    if not check_password():
        st.stop()
    
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        st.markdown("# PolyKalshi Terminal")
        if st.button("🔄 Refresh Data", type="primary", help="Fetch fresh market data and recent trades"):
            st.cache_data.clear()
            st.rerun()
    
    with col_t2:
        if os.path.exists(HISTORY_CSV):
            try:
                h_df_start = pd.read_csv(HISTORY_CSV)
                if not h_df_start.empty:
                    start_ts = pd.to_datetime(h_df_start.iloc[0]['Timestamp']).strftime("%Y-%m-%d")
                    st.markdown(f'<div class="start-date-badge">Tracking Since: {start_ts}</div>', unsafe_allow_html=True)
            except:
                pass

    # 1. Load Data (Move to top to avoid UnboundLocalError)
    df, source = get_dashboard_data()
    
    # 2. Sidebar
    with st.sidebar:
        st.header("Connection")
        if KALSHI_KEY_READY: 
            st.markdown('<div class="status-box status-ok">CONNECTED: KALSHI</div>', unsafe_allow_html=True)
        else: 
            st.markdown('<div class="status-box status-missing">MISSING: KALSHI</div>', unsafe_allow_html=True)
            
        if POLY_KEY_READY: 
            st.markdown('<div class="status-box status-ok">CONNECTED: POLYMARKET</div>', unsafe_allow_html=True)
        else: 
            st.markdown('<div class="status-box status-missing">MISSING: POLYMARKET</div>', unsafe_allow_html=True)
        
        st.divider()
        if st.button("🔄 Force Global Re-Sync", help="Clears cache and refetches everything"):
            st.cache_data.clear()
            st.rerun()
        
        st.caption(f"Data Source: {source}")
        with st.expander("Capital Management", expanded=False):
            st.markdown("Register injection/extraction of funds")
            
            # 1. Fetch current pool of investors
            cap_df_sidebar = load_capital_changes()
            existing_investors = sorted(cap_df_sidebar["Investor"].unique().tolist()) if not cap_df_sidebar.empty else CORE_INVESTORS
            
            # 2. Investor Selection
            investor_options = existing_investors + ["+ Add New Investor"]
            selected_investor = st.selectbox("Select Investor", options=investor_options)
            
            target_investor = selected_investor
            if selected_investor == "+ Add New Investor":
                target_investor = st.text_input("New Investor Name")
            
            # 3. Transaction Type
            trans_type = st.radio("Type", ["BUY (Injection)", "SELL (Withdrawal)"], horizontal=True)
            type_val = "BUY" if "BUY" in trans_type else "SELL"
            
            # 4. Amount input
            cap_change_raw = st.text_input("Amount (USD)", value="0.0")
            cap_change = 0.0
            try:
                cap_change = float(cap_change_raw)
            except ValueError:
                st.error("Please enter a valid numeric amount.")
            
            if st.button("Confirm Transaction"):
                if not target_investor:
                    st.error("Please provide an investor name.")
                else:
                    try:
                        # Fetch latest state
                        h_df_tmp = pd.read_csv(HISTORY_CSV) if os.path.exists(HISTORY_CSV) else pd.DataFrame()
                        
                        # Current NAV is either the last logged history value or the live value
                        current_nav = df['Value_USD'].sum() 
                        
                        # Get total units from capital changes (Source of Truth)
                        total_units_before = cap_df_sidebar["Units_Adjusted"].sum() if not cap_df_sidebar.empty else 0.0
                        
                        # Calculate price per share
                        current_price = current_nav / total_units_before if total_units_before > 0 else 1.0
                        
                        units_to_adjust = cap_change / current_price
                        if type_val == "SELL":
                            units_to_adjust = -units_to_adjust
                        
                        # Realize the change
                        new_row = {
                            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Investor": target_investor,
                            "Type": type_val,
                            "Amount_USD": cap_change,
                            "Units_Adjusted": units_to_adjust,
                            "Price_At_Time": current_price
                        }
                        
                        # 1. Update capital changes CSV
                        new_cap_record = pd.DataFrame([new_row])
                        if os.path.exists(CAPITAL_CHANGES_CSV):
                            new_cap_record.to_csv(CAPITAL_CHANGES_CSV, mode='a', header=False, index=False)
                        else:
                            new_cap_record.to_csv(CAPITAL_CHANGES_CSV, index=False)
                            
                        # 2. Update history units (if file exists)
                        if not h_df_tmp.empty:
                            h_df_tmp.iloc[-1, h_df_tmp.columns.get_loc("Total_Units")] += units_to_adjust
                            h_df_tmp.to_csv(HISTORY_CSV, index=False)
                            push_to_github(HISTORY_CSV, h_df_tmp.to_csv(index=False), f"Update units: {type_val} for {target_investor}")

                        # 3. PUSH TO GITHUB
                        with open(CAPITAL_CHANGES_CSV, 'r') as f_cap:
                            push_to_github(CAPITAL_CHANGES_CSV, f_cap.read(), f"Log {type_val}: {cap_change} USD for {target_investor}")

                        st.success(f"Success! {target_investor} adjusted by {units_to_adjust:,.4f} units")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e_cap:
                        st.error(f"Failed to update capital: {e_cap}")
            
            # Show Recent Injections
            if os.path.exists(CAPITAL_CHANGES_CSV):
                try:
                    cap_history = pd.read_csv(CAPITAL_CHANGES_CSV)
                    if not cap_history.empty:
                        st.divider()
                        st.markdown("**Recent Injections/Withdrawals**")
                        st.dataframe(cap_history.sort_values("Timestamp", ascending=False).head(5), use_container_width=True, hide_index=True)
                    else:
                        st.caption("No capital changes to display")
                except:
                    pass

    if df.empty:
        st.error("No data found. Ensure your keys are in Streamlit Secrets.")
        return

    # 2. Key Metrics
    total_val = df['Value_USD'].sum()      # Cash + Positions
    cash_val = df[df['Ticker'] == 'CASH']['Value_USD'].sum()
    invested_val = total_val - cash_val
    # Use stockholm timezone directly
    adj_time = datetime.now(tz=DISPLAY_TIMEZONE).strftime("%H:%M:%S")

    # --- RESET-AWARE PROFIT ---
    total_profit = 0.0
    profit_pct = 0.0
    if os.path.exists(HISTORY_CSV):
        try:
            h_df_metrics = pd.read_csv(HISTORY_CSV)
            if not h_df_metrics.empty and "Total_Units" in h_df_metrics.columns:
                first_h = h_df_metrics.iloc[0]
                last_h = h_df_metrics.iloc[-1]
                
                initial_price = first_h['Total_Value_USD'] / first_h['Total_Units'] if first_h['Total_Units'] > 0 else 1.0
                current_units = last_h['Total_Units']
                
                if current_units > 0:
                    current_price = total_val / current_units
                    total_profit = (current_price - initial_price) * current_units
                    profit_pct = (current_price / initial_price - 1) * 100
        except:
            pass # Fallback to 0 if history is malformed

    # --- TERMINAL VALUE ---
    # Cash + (total contracts / 2) * $1.00
    total_contracts = df[df['Ticker'] != 'CASH']['Quantity'].sum()
    terminal_val = cash_val + (total_contracts / 2) * 1.0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Net Asset Value", f"${total_val:,.2f}", f"{total_profit:+.2f} ({profit_pct:+.2f}%)")
    m2.metric("Terminal Value", f"${terminal_val:,.2f}", help="Cash + (Total Contracts ÷ 2) × $1")
    m3.metric("Portfolio Weight", f"${invested_val:,.2f}")
    m4.metric("Available Cash", f"${cash_val:,.2f}")
    m5.metric("Last Update", adj_time)

    # --- SHAREHOLDER EQUITY SECTION ---
    st.divider()
    st.subheader("💎 Shareholder Equity")
    
    bal_df, total_units, current_pps = get_investor_balances(total_val)
    
    if not bal_df.empty:
        # 1. Visualization
        s_col1, s_col2 = st.columns([2, 1])
        with s_col1:
            display_df = bal_df.copy()
            # Formatting for display
            display_df["Shares"] = display_df["Shares"].map("{:,.2f}".format)
            display_df["Inflow ($)"] = display_df["Inflow ($)"].map("${:,.2f}".format)
            display_df["Outflow ($)"] = display_df["Outflow ($)"].map("${:,.2f}".format)
            display_df["Current Value"] = display_df["Current Value"].map("${:,.2f}".format)
            display_df["Profit"] = display_df["Profit"].map("${:,.2f}".format)
            display_df["Fee Income ($)"] = display_df["Fee Income ($)"].map("${:,.2f}".format)
            display_df["Growth %"] = display_df["Growth %"].map("{:,.1f}%".format)
            display_df["Equity %"] = display_df["Equity %"].map("{:,.1f}%".format)
            
            # Show Fee Income column only if there has been any
            cols_to_show = ["Name", "Shares", "Inflow ($)", "Outflow ($)", "Current Value", "Profit", "Growth %", "Equity %"]
            if bal_df["Fee Income ($)"].sum() > 0:
                cols_to_show.insert(5, "Fee Income ($)")

            st.dataframe(
                display_df[cols_to_show],
                use_container_width=True,
                hide_index=True
            )
            
        with s_col2:
            fig_equity = px.pie(
                bal_df, values='Shares', names='Name', 
                title='Equity Distribution',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_equity.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=300, showlegend=False)
            st.plotly_chart(fig_equity, use_container_width=True)

        # 3. Individual Equity History
        if os.path.exists(HISTORY_CSV):
            try:
                h_df_equity = pd.read_csv(HISTORY_CSV)
                cap_df_equity = load_capital_changes()
                equity_hist = get_equity_history(h_df_equity, cap_df_equity)
                
                if not equity_hist.empty:
                    st.markdown("##### 📈 Individual Value History ($)")
                    fig_eq_hist = px.line(
                        equity_hist, x="Timestamp", y="Value", color="Investor",
                        template="plotly_dark", height=400,
                        line_shape="spline",
                        labels={"Value": "Value (USD)"}
                    )
                    fig_eq_hist.update_layout(margin=dict(l=40, r=40, t=20, b=40), hovermode="x unified")
                    st.plotly_chart(fig_eq_hist, use_container_width=True)
            except Exception as e_hist:
                st.caption(f"Could not load individual history: {e_hist}")

        # 4. Performance Fee Logic (Carry)
        new_users = bal_df[~bal_df["Is Core"]]
        if not new_users.empty:
            st.markdown("##### 📈 Unrealized Performance Fees (20% Carry)")
            fee_rows = []
            total_unrealized_fee = 0.0
            
            for _, u in new_users.iterrows():
                # Profit above HWM
                gain_per_share = max(0, current_pps - u["HWM"])
                if gain_per_share > 0:
                    unrealized_fee_dollars = gain_per_share * u["Shares"] * PERFORMANCE_FEE_RATE
                    total_unrealized_fee += unrealized_fee_dollars
                    fee_rows.append({
                        "Investor": u["Name"],
                        "Profit Above HWM": f"${(gain_per_share * u['Shares']):,.2f}",
                        "Expected Fee": f"${unrealized_fee_dollars:,.2f}",
                        "Fee in Shares": f"{(unrealized_fee_dollars / current_pps):,.4f}"
                    })
            
            if fee_rows:
                st.table(pd.DataFrame(fee_rows))
                if st.button("🚀 Realize Performance Fees", help="Transfers earned shares from new users to original 5 core users"):
                    # realization logic
                    try:
                        new_cap_entries = []
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        core_investors_df = bal_df[bal_df["Is Core"]]
                        total_core_shares = core_investors_df["Shares"].sum()
                        
                        for _, u in new_users.iterrows():
                            gain_per_share = max(0, current_pps - u["HWM"])
                            if gain_per_share > 0:
                                fee_dollars = gain_per_share * u["Shares"] * PERFORMANCE_FEE_RATE
                                shares_to_transfer = fee_dollars / current_pps
                                
                                # 1. Deduct from New User
                                new_cap_entries.append({
                                    "Timestamp": timestamp,
                                    "Investor": u["Name"],
                                    "Type": "FEE_TRANSFER",
                                    "Amount_USD": 0.0,
                                    "Units_Adjusted": -shares_to_transfer,
                                    "Price_At_Time": current_pps
                                })
                                
                                # 2. Disperse to Core Users (Proportional)
                                for _, core in core_investors_df.iterrows():
                                    weight = core["Shares"] / total_core_shares
                                    new_cap_entries.append({
                                        "Timestamp": timestamp,
                                        "Investor": core["Name"],
                                        "Type": "FEE_TRANSFER",
                                        "Amount_USD": 0.0,
                                        "Units_Adjusted": shares_to_transfer * weight,
                                        "Price_At_Time": current_pps
                                    })
                        
                        if new_cap_entries:
                            fee_df = pd.DataFrame(new_cap_entries)
                            fee_df.to_csv(CAPITAL_CHANGES_CSV, mode='a', header=False, index=False)
                            push_to_github(CAPITAL_CHANGES_CSV, fee_df.to_csv(header=False, index=False), "Realized Performance Fees")
                            st.success(f"Successfully realized ${total_unrealized_fee:,.2f} in performance fees!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e_fee:
                        st.error(f"Fee realization failed: {e_fee}")
            else:
                st.info("No realized profits eligible for performance fees at current price.")
    else:
        st.warning("Could not calculate shareholder balances. Check Data/capital_changes.csv")


    st.divider()

    # 3. Hedge Strategy & Convergence (Unified View)
    st.subheader(f"Hedge Strategy & Convergence")
    k_match = df[df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side = df[df['Platform'] == 'Polymarket']
    
    if not k_match.empty:
        with st.spinner("📈 Fetching Real-time Market Depth..."):
            try:
                from apis.orderbook import get_matched_orderbooks
                
                strategy_rows = []
                invest_rows = []
                
                for _, k in k_match.iterrows():
                    p = p_side[p_side['Ticker'] == k['Matched_Ticker']]
                    if p.empty: continue
                    p = p.iloc[0]
                    
                    # Fetch fresh orderbooks
                    kt, pt = k['Ticker'], p['Ticker']
                    obs = get_matched_orderbooks(kt, pt, levels=1)
                    k_side_raw, p_side_raw = k['Side'], p['Side']
                    
                    # --- EXIT LOGIC (BIDS) ---
                    k_b_list = obs.get('kalshi', {}).get(k_side_raw.lower(), {}).get('bids', [])
                    p_b_list = obs.get('polymarket', {}).get(p_side_raw.lower(), {}).get('bids', [])
                    
                    k_bid_raw, k_bid_vol = (k_b_list[0]['price'], k_b_list[0]['volume']) if k_b_list else (0, 0)
                    p_bid, p_bid_vol = (p_b_list[0]['price'], p_b_list[0]['volume']) if p_b_list else (0, 0)
                    
                    # Apply Kalshi Fee to Bids (We receive less when selling)
                    k_bid_fee = 0.07 * k_bid_raw * (1.0 - k_bid_raw) if k_bid_raw > 0 else 0
                    k_bid_net = k_bid_raw - k_bid_fee
                    
                    combined_bid = k_bid_net + p_bid
                    k_bid_liq_ok = (k_bid_vol >= VOLUME_PERCENTILE_THRESHOLD * k['Quantity']) or (k_bid_vol * k_bid_raw >= VOLUME_FIXED_THRESHOLD)
                    p_bid_liq_ok = (p_bid_vol >= VOLUME_PERCENTILE_THRESHOLD * p['Quantity']) or (p_bid_vol * p_bid >= VOLUME_FIXED_THRESHOLD)
                    
                    if combined_bid >= EXIT_TARGET and k_bid_liq_ok and p_bid_liq_ok: sell_status = "✅ Ready to Exit"
                    elif combined_bid >= EXIT_TARGET: sell_status = "⚠️ Low Bid Volume"
                    else: sell_status = "⏳ Pending Price"
                    
                    is_hedge = "Standard Hedge" if k_side_raw != p_side_raw else "⚠️ Directional (Same Side)"

                    strategy_rows.append({
                        "Strategy": k['Title'],
                        "Combo Bid (Net)": f"${combined_bid:.3f}",
                        "Sellable Status": sell_status,
                        "Hedge Type": is_hedge,
                        "Kalshi Side (Net Bid)": f"{k_side_raw} (${k_bid_net:.3f})",
                        "Polymarket Side (Bid)": f"{p_side_raw} (${p_bid:.3f})",
                        "Gap (Net)": f"${max(0.99-combined_bid, 0):.3f}",
                        "Total Value": f"${(k['Value_USD'] + p['Value_USD']):,.2f}",
                        "Total P&L": f"${(k['Profit_USD'] + p['Profit_USD']):,.2f}"
                    })
                    
                    # --- INVESTMENT LOGIC (ASKS) ---
                    k_a_list = obs.get('kalshi', {}).get(k_side_raw.lower(), {}).get('asks', [])
                    p_a_list = obs.get('polymarket', {}).get(p_side_raw.lower(), {}).get('asks', [])
                    
                    k_ask_raw, k_ask_vol = (k_a_list[0]['price'], k_a_list[0]['volume']) if k_a_list else (1.0, 0)
                    p_ask, p_ask_vol = (p_a_list[0]['price'], p_a_list[0]['volume']) if p_a_list else (1.0, 0)
                    
                    # Apply Kalshi Fee to Asks (We pay more when buying)
                    k_ask_fee = 0.07 * k_ask_raw * (1.0 - k_ask_raw) if k_ask_raw < 1.0 else 0
                    k_ask_net = k_ask_raw + k_ask_fee
                    
                    combined_ask = k_ask_net + p_ask
                    # Investment liquidity
                    k_ask_liq_ok = (k_ask_vol * k_ask_raw >= VOLUME_FIXED_THRESHOLD)
                    p_ask_liq_ok = (p_ask_vol * p_ask >= VOLUME_FIXED_THRESHOLD)
                    
                    if combined_ask <= INVEST_TARGET and k_ask_liq_ok and p_ask_liq_ok: invest_status = "✅ Available Investment"
                    elif combined_ask <= INVEST_TARGET: invest_status = "⚠️ Low Ask Volume"
                    else: invest_status = "⏳ Awaiting Spread"
                    
                    invest_rows.append({
                        "Strategy": k['Title'],
                        "Combo Ask (Net)": f"${combined_ask:.3f}",
                        "Investment Status": invest_status,
                        "Kalshi Side (Net Ask)": f"{k_side_raw} (${k_ask_net:.3f})",
                        "Polymarket Side (Ask)": f"{p_side_raw} (${p_ask:.3f})",
                        "Arbitrage Opportunity (Net)": f"${max(1.0-combined_ask, 0):.3f}",
                        "Current Holding Value": f"${(k['Value_USD'] + p['Value_USD']):,.2f}"
                    })
                
                if strategy_rows:
                    strat_df = pd.DataFrame(strategy_rows)
                    strat_df.index = range(1, len(strat_df) + 1)
                    st.dataframe(strat_df, use_container_width=True)
                else:
                    st.info("No active strategy pairs detected.")
                    
                st.subheader("Holding Expansion Opportunities")
                if invest_rows:
                    inv_df = pd.DataFrame(invest_rows)
                    inv_df.index = range(1, len(inv_df) + 1)
                    st.dataframe(inv_df, use_container_width=True)
                else:
                    st.info("No re-investment opportunities detected.")
                    
            except Exception as e_strat:
                st.warning(f"Strategy view failed: {e_strat}")
    else:
        st.info("No strategy pairs detected. Ensure you have positions on both platforms.")

    st.divider()

    # 4. Aligned Exposure Visualization (Plotly Subplots - Synchronized Axes)
    st.subheader("Exposure Distribution")
    
    pos_only = df[df['Ticker'] != 'CASH'].copy()
    if not pos_only.empty:
        from plotly.subplots import make_subplots
        import plotly.graph_objects as go
        
        def get_pair_key(row):
            t, m = str(row['Ticker']), str(row.get('Matched_Ticker', ''))
            if not m or m.lower() in ['nan', '', 'none']: return tuple(sorted([t]))
            return tuple(sorted([t, m]))

        pos_only['PairID'] = pos_only.apply(get_pair_key, axis=1)
        
        k_df, p_df = pos_only[pos_only['Platform'] == 'Kalshi'], pos_only[pos_only['Platform'] == 'Polymarket']
        all_pids = sorted(list(set(k_df['PairID'].tolist() + p_df['PairID'].tolist())))
        pair_list = []
        
        for pid in all_pids:
            kr, pr = k_df[k_df['PairID'] == pid], p_df[p_df['PairID'] == pid]
            title = kr['Title'].iloc[0] if not kr.empty else pr['Title'].iloc[0]
            kv, pv = kr['Value_USD'].sum() if not kr.empty else 0, pr['Value_USD'].sum() if not pr.empty else 0
            kq, pq = kr['Quantity'].sum() if not kr.empty else 0, pr['Quantity'].sum() if not pr.empty else 0
            pair_list.append({
                'Title': title, 'K_Val': kv, 'P_Val': pv,
                'K_Qty': kq, 'P_Qty': pq,
                'K_Side': kr['Side'].iloc[0] if not kr.empty else '', 
                'P_Side': pr['Side'].iloc[0] if not pr.empty else '',
                'MaxVal': max(kv, pv)
            })
        
        aligned_df = pd.DataFrame(pair_list).sort_values('MaxVal', ascending=True)
        aligned_df['WrappedTitle'] = aligned_df['Title'].apply(wrap_label)
        
        # Calculate synchronized axis range
        max_exp = max(aligned_df['K_Val'].max(), aligned_df['P_Val'].max(), 0) * 1.2
        if max_exp < 10: max_exp = 100 # Floor for better display

        fig_aligned = make_subplots(rows=1, cols=2, shared_yaxes=True, 
                                   subplot_titles=("Kalshi Exposure", "Polymarket Exposure"),
                                   horizontal_spacing=0.1)
        
        color_map = {'YES': '#2ecc71', 'NO': '#e74c3c'}
        
        fig_aligned.add_trace(go.Bar(
            y=aligned_df['WrappedTitle'], x=aligned_df['K_Val'], name='Kalshi', orientation='h',
            marker_color=[color_map.get(s, '#bdc3c7') for s in aligned_df['K_Side']],
            text=aligned_df.apply(lambda r: f"{r['K_Qty']:,.0f} ctx (${r['K_Val']:,.2f})" if r['K_Val']>0 else "", axis=1), 
            textposition='auto',
            hovertemplate="<b>%{y}</b><br>Kalshi Qty: %{text}<extra></extra>"
        ), row=1, col=1)
        
        fig_aligned.add_trace(go.Bar(
            y=aligned_df['WrappedTitle'], x=aligned_df['P_Val'], name='Polymarket', orientation='h',
            marker_color=[color_map.get(s, '#bdc3c7') for s in aligned_df['P_Side']],
            text=aligned_df.apply(lambda r: f"{r['P_Qty']:,.0f} ctx (${r['P_Val']:,.2f})" if r['P_Val']>0 else "", axis=1), 
            textposition='auto',
            hovertemplate="<b>%{y}</b><br>Polymarket Qty: %{text}<extra></extra>"
        ), row=1, col=2)
        
        fig_aligned.update_layout(
            template="plotly_dark", 
            height=max(600, len(aligned_df)*75), # More height per row for readability
            showlegend=False, 
            margin=dict(l=400, r=40, t=80, b=60), # Even larger margin for long titles
            hovermode="y unified",
            bargap=0.4 # Wider gap
        )
        # Sync X-axes
        fig_aligned.update_xaxes(range=[0, max_exp], row=1, col=1, title="Exposure ($)")
        fig_aligned.update_xaxes(range=[0, max_exp], row=1, col=2, title="Exposure ($)")
        
        st.plotly_chart(fig_aligned, use_container_width=True)
    else:
        st.info("No positions to visualize.")

    # --- PORTFOLIO HISTORY ---
    st.divider()
    
    if os.path.exists(HISTORY_CSV):
        try:
            h_df = pd.read_csv(HISTORY_CSV)
            if not h_df.empty:
                h_df['Timestamp'] = pd.to_datetime(h_df['Timestamp'])
                h_df = h_df.sort_values('Timestamp')
                
                # --- CALCULATE GROWTH ---
                if "Total_Units" in h_df.columns:
                    h_df['Price'] = h_df['Total_Value_USD'] / h_df['Total_Units']
                    initial_price = h_df.iloc[0]['Price']
                    if initial_price > 0:
                        h_df['Growth_Pct'] = (h_df['Price'] / initial_price - 1) * 100
                    else:
                        h_df['Growth_Pct'] = 0.0
                else:
                    h_df['Growth_Pct'] = 0.0

                # --- SMART APR ---
                if len(h_df) >= 2 and "Total_Units" in h_df.columns:
                    first_row = h_df.iloc[0]
                    last_row = h_df.iloc[-1]
                    time_diff = (last_row['Timestamp'] - first_row['Timestamp']).total_seconds()
                    days_diff = time_diff / (24 * 3600)
                    
                    price_start = first_row['Price']
                    price_last = last_row['Price']
                    
                    if days_diff > 0.01 and price_start > 0:
                        total_return = (price_last / price_start) - 1
                        apr = (total_return * 365 / days_diff) * 100
                        hist_col1, hist_col2 = st.columns([3, 1])
                        with hist_col1:
                            st.subheader("Performance History")
                        with hist_col2:
                            st.metric("Projected APR", f"{apr:+.1f}%", help="Annualized return based on history trajectory")
                    else:
                        st.subheader("Performance History")
                else:
                    st.subheader("Performance History")

                # --- DUAL AXIS GRAPH ---
                fig_hist = make_subplots(specs=[[{"secondary_y": True}]])

                # Trace 1: Value USD (Left)
                fig_hist.add_trace(
                    go.Scatter(
                        x=h_df['Timestamp'], 
                        y=h_df['Total_Value_USD'],
                        name="Value ($)",
                        mode='lines',
                        fill='tozeroy',
                        line=dict(width=3, color='#00e676'),
                        fillcolor='rgba(0, 230, 118, 0.15)',
                        hovertemplate="<b>Value:</b> $%{y:,.2f}<br>"
                    ),
                    secondary_y=False,
                )

                # Trace 2: Growth % (Right)
                fig_hist.add_trace(
                    go.Scatter(
                        x=h_df['Timestamp'], 
                        y=h_df['Growth_Pct'],
                        name="Growth (%)",
                        mode='lines',
                        line=dict(width=3, color='#facc15'), # Vibrant Yellow
                        hovertemplate="<b>Growth:</b> %{y:+.2f}%<br>"
                    ),
                    secondary_y=True,
                )

                fig_hist.update_layout(
                    template="plotly_dark",
                    height=450,
                    margin=dict(l=40, r=40, t=20, b=40),
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=dict(
                        showgrid=False,
                        rangeselector=dict(
                            buttons=list([
                                dict(count=1, label="1d", step="day", stepmode="backward"),
                                dict(count=7, label="1w", step="day", stepmode="backward"),
                                dict(count=1, label="1m", step="month", stepmode="backward"),
                                dict(step="all")
                            ]),
                            bgcolor="rgba(30, 41, 59, 0.8)",
                            font=dict(color="#f8fafc")
                        )
                    )
                )

                # Left Y-Axis
                fig_hist.update_yaxes(
                    title_text="Portfolio Value ($)", 
                    secondary_y=False, 
                    showgrid=True, 
                    gridcolor="rgba(255,255,255,0.05)",
                    tickprefix="$",
                    tickformat=",."
                )
                
                # Right Y-Axis
                fig_hist.update_yaxes(
                    title_text="Growth (%)", 
                    secondary_y=True, 
                    showgrid=False,
                    ticksuffix="%",
                    tickformat=".1f"
                )

                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.subheader("📊 Performance History")
                st.info("Portfolio history is currently empty. Logging will begin automatically.")
        except Exception as e_hist:
            st.error(f"Error loading history: {e_hist}")
    else:
        st.info("Portfolio history not yet available. First run of scheduled task pending.")

    st.divider()

    # 5. Single-Sided Audit
    with st.expander("🔍 Single-Sided Positions & Audit Log"):
        unmatched_k = pos_only[(pos_only['Platform'] == 'Kalshi') & (pos_only['Matched_Ticker'].isna() | (pos_only['Matched_Ticker'] == ""))]
        unmatched_p = pos_only[(pos_only['Platform'] == 'Polymarket') & (pos_only['Matched_Ticker'].isna() | (pos_only['Matched_Ticker'] == ""))]
        
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            st.markdown("**Kalshi Only**")
            st.dataframe(unmatched_k[['Title', 'Side', 'Quantity', 'Value_USD', 'Profit_USD']], use_container_width=True, hide_index=True)
        with col_u2:
            st.markdown("**Polymarket Only**")
            st.dataframe(unmatched_p[['Title', 'Side', 'Quantity', 'Value_USD', 'Profit_USD']], use_container_width=True, hide_index=True)
        
        st.divider()
        st.markdown("**Raw Data API Feed**")
        st.dataframe(df, use_container_width=True)

    st.divider()

    # 6. Recent Trade History (Last 14 Days)
    st.subheader("⏱️ Recent Trade History (Last 14 Days)")
    st.markdown("Recent filled orders across both platforms.")
    
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        st.markdown("##### Kalshi Trades")
        with st.spinner("Fetching Kalshi trades..."):
            try:
                k_trades = get_kalshi_recent_trades(days=14)
                if k_trades:
                    k_trade_df = pd.DataFrame(k_trades)
                    # Convert to timezone aware datetime, then to local timezone, then format
                    k_trade_df['date'] = pd.to_datetime(k_trade_df['date']).dt.tz_convert(DISPLAY_TIMEZONE).dt.strftime('%Y-%m-%d %H:%M')
                    st.dataframe(k_trade_df[['date', 'title', 'side', 'quantity', 'price']], use_container_width=True, hide_index=True)
                else:
                    st.info("No Kalshi trades in the last 14 days.")
            except Exception as e:
                st.error(f"Error fetching Kalshi trades: {e}")
                
    with col_r2:
        st.markdown("##### Polymarket Trades")
        with st.spinner("Fetching Polymarket trades..."):
            try:
                p_trades = get_polymarket_recent_trades(WALLET_ADDR, days=14)
                if p_trades:
                    p_trade_df = pd.DataFrame(p_trades)
                    # Convert to timezone aware datetime, then to local timezone, then format
                    p_trade_df['date'] = pd.to_datetime(p_trade_df['date']).dt.tz_convert(DISPLAY_TIMEZONE).dt.strftime('%Y-%m-%d %H:%M')
                    st.dataframe(p_trade_df[['date', 'title', 'side', 'quantity', 'price']], use_container_width=True, hide_index=True)
                else:
                    st.info("No Polymarket trades in the last 14 days.")
            except Exception as e:
                 st.warning(f"Error fetching Polymarket trades: {e}")


if __name__ == "__main__":
    main()
