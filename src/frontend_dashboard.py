import streamlit as st
import pandas as pd
import plotly.express as px
import os
import textwrap
from datetime import datetime, timedelta

# Import your actual API logic directly
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
try:
    from apis.portfolio import get_kalshi_positions, get_polymarket_positions
except ImportError:
    # Handle direct execution vs module execution context
    from apis.portfolio import get_kalshi_positions, get_polymarket_positions

# --- CONFIGURATION ---
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
EXIT_TARGET = 0.99
TIME_OFFSET_HOURS = 1 # Shift forward to Europe/Stockholm (UTC+1)

# Check for API keys
KALSHI_KEY_READY = os.getenv("KALSHI_ACCESS_KEY") is not None
POLY_KEY_READY = os.getenv("POLYMARKET_WALLET_ADDRESS") is not None

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
</style>
""", unsafe_allow_html=True)

# --- UTILITIES ---

def wrap_label(text, width=35):
    if not text: return ""
    return "<br>".join(textwrap.wrap(str(text), width=width))

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
            "Match_Score": p.get("match_score", 0)
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
            "Match_Score": p.get("match_score", 0)
        })
    return pd.DataFrame(rows)

@st.cache_data(ttl=600)
def get_dashboard_data():
    """Tries live API first, falls back to local CSV."""
    if KALSHI_KEY_READY and POLY_KEY_READY:
        with st.spinner("🛰️ Synchronizing with Market APIs..."):
            try:
                k_pos = get_kalshi_positions()
                p_pos = get_polymarket_positions()
                df = transform_to_dataframe(k_pos, p_pos)
                if not df.empty:
                    return df, "Live API"
            except Exception as e:
                st.warning(f"Live fetch failed: {e}")
    
    # Fallback
    if os.path.exists(PORTFOLIO_CSV):
        df = pd.read_csv(PORTFOLIO_CSV)
        # Standardize columns if reading from old CSV
        if 'P&L_USD' in df.columns:
            df = df.rename(columns={'P&L_USD': 'Profit_USD'})
        return df, "Local CSV"
    
    return pd.DataFrame(), "No Data"

# --- MAIN UI ---

def main():
    st.markdown("# 🏛 PolyKalshi Terminal")
    
    # Sidebar Status
    with st.sidebar:
        st.header("Terminal Control")
        if KALSHI_KEY_READY: st.markdown('<div class="status-box status-ok">CONNECTED: KALSHI</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-box status-missing">MISSING: KALSHI KEYS</div>', unsafe_allow_html=True)
        
        if POLY_KEY_READY: st.markdown('<div class="status-box status-ok">CONNECTED: POLYGON</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-box status-missing">MISSING: POLY WALLET</div>', unsafe_allow_html=True)
        
        st.divider()
        if st.button("🚀 Force Global Sync"):
            st.cache_data.clear()
            st.rerun()
            
        st.divider()
        st.markdown("**Filters**")
        platforms = st.multiselect("Visible Platforms", ["Kalshi", "Polymarket"], default=["Kalshi", "Polymarket"])

    # 1. Load Data
    df, source = get_dashboard_data()
    if df.empty:
        st.error("No data found. Ensure your keys are in Streamlit Secrets.")
        return

    f_df = df[df['Platform'].isin(platforms)]
    
    # 2. Key Metrics
    m1, m2, m3 = st.columns(3)
    total_val = f_df['Value_USD'].sum()
    total_profit = f_df['Profit_USD'].sum()
    # Corrected time with offset
    adj_time = (datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime("%H:%M:%S")

    m1.metric("Total Net Equity", f"${total_val:,.2f}", f"{total_profit:+.2f} Total Profit")
    m2.metric("Data Context", source)
    m3.metric("Terminal Time", adj_time, help="Adjusted to Stockholm/Europe Time")

    st.divider()

    # 3. Hedge Convergence Tracker
    st.subheader("🎯 Active Hedge Strategies")
    k_match = f_df[f_df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side = f_df[f_df['Platform'] == 'Polymarket']
    
    conv_rows = []
    for _, k in k_match.iterrows():
        p = p_side[p_side['Ticker'] == k['Matched_Ticker']]
        if not p.empty:
            p = p.iloc[0]
            try:
                k_p, p_p = float(k['Price']), float(p['Price'])
                combined = k_p + p_p
                conv_rows.append({
                    "Strategy Pair": f"{k['Ticker']} / {p['Ticker']}",
                    "Current Value": f"${combined:.3f}",
                    "Profit Gap": f"${max(0.99-combined, 0):.3f}",
                    "Confidence": f"{k.get('Match_Score', 0):.2f}"
                })
            except: pass
            
    if conv_rows:
        st.table(pd.DataFrame(conv_rows))
    else:
        st.info("No active hedge pairs detected. Run matching locally and push the CSV to see pairs here.")

    st.divider()

    # 4. Two-Column Position Split (Visualized like visualize_portfolios.py)
    st.subheader("📊 Portfolio Allocations")
    col_chart1, col_chart2 = st.columns(2)
    
    # Pre-process for wrapped labels
    pos_only = f_df[~f_df['Ticker'].str.contains("CASH", na=False)].copy()
    pos_only['WrappedTitle'] = pos_only['Title'].apply(wrap_label)
    
    with col_chart1:
        st.markdown("#### 🏛 Kalshi Holdings")
        k_data = pos_only[pos_only['Platform'] == 'Kalshi'].sort_values('Value_USD', ascending=True)
        if not k_data.empty:
            fig_k = px.bar(k_data, y='WrappedTitle', x='Value_USD', orientation='h', 
                           color='Side', color_discrete_map={'YES':'#2ecc71','NO':'#e74c3c'},
                           text_auto='.2s', template="plotly_dark")
            fig_k.update_layout(height=max(400, len(k_data)*60), margin=dict(l=0, r=20, t=20, b=20),
                                yaxis=dict(title=None), xaxis=dict(title="Value in USD"))
            st.plotly_chart(fig_k, use_container_width=True)
        else: st.info("No active Kalshi positions.")

    with col_chart2:
        st.markdown("#### 💎 Polymarket Holdings")
        p_data = pos_only[pos_only['Platform'] == 'Polymarket'].sort_values('Value_USD', ascending=True)
        if not p_data.empty:
            fig_p = px.bar(p_data, y='WrappedTitle', x='Value_USD', orientation='h', 
                           color='Side', color_discrete_map={'YES':'#3498db','NO':'#9b59b6'},
                           text_auto='.2s', template="plotly_dark")
            fig_p.update_layout(height=max(400, len(p_data)*60), margin=dict(l=0, r=20, t=20, b=20),
                                yaxis=dict(title=None), xaxis=dict(title="Value in USD"))
            st.plotly_chart(fig_p, use_container_width=True)
        else: st.info("No active Polymarket positions.")

    # 5. History Section
    st.divider()
    st.subheader("📈 Portfolio History")
    history_file = "Data/history/run_log.csv"
    if os.path.exists(history_file):
        try:
            h_df = pd.read_csv(history_file)
            fig_h = px.line(h_df, x='snapshot_time', y='total_value_usd', 
                            title="Total Value Over Time", template="plotly_dark")
            st.plotly_chart(fig_h, use_container_width=True)
        except: st.info("History log found but unreadable.")
    else:
        st.info("💡 History is logged locally. Push `Data/history/run_log.csv` to GitHub to see your growth chart here.")

    # 6. Audit Log
    with st.expander("🔍 Detailed Audit Log"):
        st.dataframe(f_df, use_container_width=True)

if __name__ == "__main__":
    main()
