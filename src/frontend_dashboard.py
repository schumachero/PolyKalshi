import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import textwrap
import traceback
import csv
import json
import base64
import requests
from datetime import datetime, timedelta

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

# Define defaults first to avoid NameError if imports fail non-traditionally
def get_kalshi_positions(): return []
def get_polymarket_positions(): return []
def get_kalshi_balance(): return {}
def get_polymarket_balance(addr): return 0
def generate_semantic_matches(k, p, threshold=0.3): return pd.DataFrame()

# Use absolute 'src.' imports (standard for Streamlit Cloud with src/ folder)
try:
    from src.apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance
    from src.matching.semantic_matching import generate_semantic_matches
except ImportError:
    try:
        # Fallback for environments where 'src' is the root or already in path
        from apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance
        from matching.semantic_matching import generate_semantic_matches
    except ImportError as e:
        st.error(f"Import Warning: {e}. Dashboard functionality may be limited.")

# --- CONFIGURATION ---
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
HISTORY_CSV = "Data/portfolio_history.csv"
CAPITAL_CHANGES_CSV = "Data/capital_changes.csv"

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
TIME_OFFSET_HOURS = 1 
VOLUME_PERCENTILE_THRESHOLD = 0.20 # 20% of position
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
    .start-date-badge { position: absolute; top: 10px; right: 20px; color: #94a3b8; font-size: 0.85rem; background: rgba(30, 41, 59, 0.5); padding: 5px 12px; border-radius: 20px; border: 1px solid #334155; }
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
            "Price": p.get("current_price", 0),
            "Value_USD": p.get("market_exposure_cents", 0) / 100,
            "Profit_USD": p.get("realized_pnl_cents", 0) / 100,
            "Matched_Ticker": p.get("matched_ticker", ""),
            "Match_Score": p.get("match_score", 0),
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
            "Price": p.get("current_price", 0),
            "Value_USD": p.get("current_value", 0),
            "Profit_USD": p.get("pnl", 0),
            "Matched_Ticker": p.get("matched_ticker", ""),
            "Match_Score": p.get("match_score", 0),
            "close_time": p.get("close_time", ""),
            "rules": p.get("rules", "")
        })
    return pd.DataFrame(rows)

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
    
    st.markdown("# PolyKalshi Terminal")

    # Start Date Badge logic
    if os.path.exists(HISTORY_CSV):
        try:
            h_df_start = pd.read_csv(HISTORY_CSV)
            if not h_df_start.empty:
                start_ts = pd.to_datetime(h_df_start.iloc[0]['Timestamp']).strftime("%Y-%m-%d")
                st.markdown(f'<div class="start-date-badge">Start Date: {start_ts}</div>', unsafe_allow_html=True)
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
        
        st.divider()
        with st.expander("Capital Management", expanded=False):
            st.markdown("Register injection/extraction of funds")
            # Simplified input to avoid potential JS module loading issues in some environments
            cap_change_raw = st.text_input("Amount (USD)", value="0.0")
            cap_change = 0.0
            try:
                cap_change = float(cap_change_raw)
            except ValueError:
                st.error("Please enter a valid numeric amount.")
            
            if st.button("Confirm Capital Change"):
                if os.path.exists(HISTORY_CSV):
                    try:
                        h_df_tmp = pd.read_csv(HISTORY_CSV)
                        if not h_df_tmp.empty and "Total_Units" in h_df_tmp.columns:
                            # Current Price = Last Total Value / Last Total Units
                            last_val = h_df_tmp.iloc[-1]["Total_Value_USD"]
                            last_units = h_df_tmp.iloc[-1]["Total_Units"]
                            
                            if last_units > 0:
                                current_price = last_val / last_units
                                units_to_add = cap_change / current_price
                                
                                # 1. Update history units (locally)
                                h_df_tmp.iloc[-1, h_df_tmp.columns.get_loc("Total_Units")] += units_to_add
                                h_df_tmp.to_csv(HISTORY_CSV, index=False)
                                
                                # 2. Log to capital changes history (locally)
                                cap_record = pd.DataFrame([{
                                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "Amount_USD": cap_change,
                                    "Units_Adjusted": units_to_add,
                                    "Price_At_Time": current_price
                                }])
                                if os.path.exists(CAPITAL_CHANGES_CSV):
                                    cap_record.to_csv(CAPITAL_CHANGES_CSV, mode='a', header=False, index=False)
                                else:
                                    cap_record.to_csv(CAPITAL_CHANGES_CSV, index=False)

                                # 3. PUSH TO GITHUB (If token available)
                                push_to_github(HISTORY_CSV, h_df_tmp.to_csv(index=False), f"Update units: {cap_change:+} USD")
                                
                                # Re-read for logging history to GH
                                with open(CAPITAL_CHANGES_CSV, 'r') as f_cap:
                                    push_to_github(CAPITAL_CHANGES_CSV, f_cap.read(), f"Log injection: {cap_change:+} USD")

                                st.success(f"Adjusted portfolio by {units_to_add:,.4f} units")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("Cannot adjust capital: Initial units are zero.")
                    except Exception as e_cap:
                        st.error(f"Failed to update units: {e_cap}")
                else:
                    st.warning("No history file found to update.")
            
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
    adj_time = (datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime("%H:%M:%S")

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

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Net Asset Value", f"${total_val:,.2f}", f"{total_profit:+.2f} ({profit_pct:+.2f}%)")
    m2.metric("Portfolio Weight", f"${invested_val:,.2f}")
    m3.metric("Available Cash", f"${cash_val:,.2f}")
    m4.metric("Last Update", adj_time)


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
            pair_list.append({
                'Title': title, 'K_Val': kv, 'P_Val': pv,
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
            text=aligned_df['K_Val'].apply(lambda v: f"${v:,.2f}" if v>0 else ""), textposition='auto'
        ), row=1, col=1)
        
        fig_aligned.add_trace(go.Bar(
            y=aligned_df['WrappedTitle'], x=aligned_df['P_Val'], name='Polymarket', orientation='h',
            marker_color=[color_map.get(s, '#bdc3c7') for s in aligned_df['P_Side']],
            text=aligned_df['P_Val'].apply(lambda v: f"${v:,.2f}" if v>0 else ""), textposition='auto'
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
                
                # --- SMART APR (Share Price Method) ---
                if len(h_df) >= 2 and "Total_Units" in h_df.columns:
                    h_df['Price'] = h_df['Total_Value_USD'] / h_df['Total_Units']
                    
                    first_row = h_df.iloc[0]
                    last_row = h_df.iloc[-1]
                    
                    time_diff = (last_row['Timestamp'] - first_row['Timestamp']).total_seconds()
                    days_diff = time_diff / (24 * 3600)
                    
                    price_start = first_row['Price']
                    price_last = last_row['Price']
                    
                    if days_diff > 0.01 and price_start > 0:
                        # Return based on Share Price growth (Time-Weighted Return)
                        total_return = (price_last / price_start) - 1
                        apr = (total_return * 365 / days_diff) * 100
                        
                        hist_col1, hist_col2 = st.columns([3, 1])
                        with hist_col1:
                            st.subheader("Portfolio Value History")
                        with hist_col2:
                            st.metric("Projected APR", f"{apr:+.1f}%", help="Annualized return based on history trajectory")
                    else:
                        st.subheader("Portfolio Value History")
                else:
                    st.subheader("Portfolio Value History")

                # Create a premium area chart
                fig_hist = px.area(
                    h_df, 
                    x='Timestamp', 
                    y='Total_Value_USD',
                    template="plotly_dark",
                    labels={'Total_Value_USD': 'Value ($)', 'Timestamp': 'Time'}
                )
                
                # Enhance aesthetics
                fig_hist.update_traces(
                    line_color='#00e676', # Vibrant emerald
                    fillcolor='rgba(0, 230, 118, 0.15)',
                    line_width=3,
                    hovertemplate="<b>Value:</b> $%{y:,.2f}<br><b>Time:</b> %{x|%Y-%m-%d %H:%M}"
                )
                
                fig_hist.update_layout(
                    height=450,
                    margin=dict(l=40, r=40, t=20, b=40),
                    hovermode="x unified",
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
                    ),
                    yaxis=dict(
                        showgrid=True, 
                        gridcolor="rgba(255,255,255,0.05)",
                        tickprefix="$",
                        tickformat=",."
                    )
                )
                
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.subheader("📊 Portfolio Value History")
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


if __name__ == "__main__":
    main()
