import streamlit as st
import pandas as pd
import plotly.express as px
import os
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

# Check for API keys in environment (Streamlit Secrets injects these automatically)
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
    h1, h2, h3 { color: #f8fafc; }
    .status-box { padding: 10px; border-radius: 10px; margin-bottom: 10px; font-size: 0.8em; }
    .status-ok { background: #065f46; color: #34d399; border: 1px solid #059669; }
    .status-missing { background: #7f1d1d; color: #f87171; border: 1px solid #b91c1c; }
</style>
""", unsafe_allow_html=True)

# --- DATA ORCHESTRATION ---

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
            "P&L_USD": p.get("realized_pnl_cents", 0) / 100,
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
            "P&L_USD": p.get("pnl", 0),
            "Matched_Ticker": p.get("matched_ticker", ""),
            "Match_Score": p.get("match_score", 0)
        })
    
    return pd.DataFrame(rows)

@st.cache_data(ttl=600)  # Auto-refresh every 10 minutes
def get_dashboard_data():
    """The master fetcher. Tries live API first, falls back to CSV."""
    if KALSHI_KEY_READY and POLY_KEY_READY:
        with st.spinner("🔄 Fetching Live Market Data..."):
            try:
                k_pos = get_kalshi_positions()
                p_pos = get_polymarket_positions()
                df = transform_to_dataframe(k_pos, p_pos)
                if not df.empty:
                    return df, "Live API"
            except Exception as e:
                st.warning(f"Live fetch failed, trying local cache... ({e})")
    
    # Fallback to local CSV
    if os.path.exists(PORTFOLIO_CSV):
        df = pd.read_csv(PORTFOLIO_CSV)
        return df, "Local CSV"
    
    return pd.DataFrame(), "No Data"

# --- MAIN UI ---

def main():
    st.markdown("# 🏛 PolyKalshi Terminal")
    
    # 1. Sidebar Status & Controls
    with st.sidebar:
        st.header("Connection Status")
        
        # Security Status Indicator
        if KALSHI_KEY_READY:
            st.markdown('<div class="status-box status-ok">✅ Kalshi API Linked</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-box status-missing">❌ Kalshi API Missing</div>', unsafe_allow_html=True)
            
        if POLY_KEY_READY:
            st.markdown('<div class="status-box status-ok">✅ Poly Wallet Linked</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-box status-missing">❌ Poly Wallet Missing</div>', unsafe_allow_html=True)
            
        st.divider()
        if st.button("🚀 Force Global Re-Sync"):
            st.cache_data.clear()
            st.rerun()
            
        st.divider()
        st.markdown("**Filters**")
        platforms = st.multiselect("Active Platforms", ["Kalshi", "Polymarket"], default=["Kalshi", "Polymarket"])

    # 2. Load Data
    df, source = get_dashboard_data()
    
    if df.empty:
        st.error("No portfolio data available. Please check your API keys or run a local sync.")
        st.info("💡 Make sure your .env variables are pasted into the Streamlit Cloud 'Secrets' vault.")
        return

    # Filtered View
    f_df = df[df['Platform'].isin(platforms)]
    
    # 3. Top Metrics Row
    m1, m2, m3 = st.columns(3)
    total_val = f_df['Value_USD'].sum()
    total_pnl = f_df['P&L_USD'].sum()
    
    m1.metric("Net Asset Value", f"${total_val:,.2f}", f"{total_pnl:+.2f} P&L", delta_color="normal")
    m2.metric("Data Source", source)
    m3.metric("Last Update", datetime.now().strftime("%H:%M:%S"))

    st.divider()

    # 4. Convergence Monitor
    st.subheader("🎯 Hedge Convergence Tracker")
    
    k_match = f_df[f_df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side = f_df[f_df['Platform'] == 'Polymarket']
    
    conv_data = []
    for _, k in k_match.iterrows():
        p = p_side[p_side['Ticker'] == k['Matched_Ticker']]
        if not p.empty:
            p = p.iloc[0]
            try:
                k_p, p_p = float(k['Price']), float(p['Price'])
            except:
                k_p, p_p = 0, 0
            
            combined = k_p + p_p
            conv_data.append({
                "Strategy": f"{k['Ticker']} / {p['Ticker']}",
                "Combined Bid": combined,
                "Match Conf.": k.get('Match_Score', 0),
                "Distance to $0.99": max(0.99 - combined, 0)
            })
            
    if conv_data:
        st.table(pd.DataFrame(conv_data))
    else:
        st.info("No active hedge pairs detected. Ensure you have run semantic matching.")

    st.divider()

    # 5. Visualizations
    col1, col2 = st.columns(2)
    
    pos_only = f_df[~f_df['Ticker'].str.contains("CASH", na=False)]
    
    with col1:
        st.markdown("### 📊 Platform Exposure")
        fig_pie = px.pie(f_df, values='Value_USD', names='Platform', hole=.4, template="plotly_dark")
        st.plotly_chart(fig_pie, use_container_width=True)
        
    with col2:
        st.markdown("### 📈 Top Position Sizes")
        fig_bar = px.bar(pos_only.sort_values('Value_USD', ascending=False).head(10), 
                         x='Value_USD', y='Ticker', color='Side', orientation='h',
                         template="plotly_dark")
        st.plotly_chart(fig_bar, use_container_width=True)

    # 6. Raw Audit Data
    with st.expander("🔍 Open Full Portfolio Audit Log"):
        st.dataframe(f_df, use_container_width=True)

if __name__ == "__main__":
    main()
