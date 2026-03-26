import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys
import textwrap
from datetime import datetime, timedelta

# Robust Path Handling
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if current_dir not in sys.path: sys.path.append(current_dir)
if parent_dir not in sys.path: sys.path.append(parent_dir)

try:
    from apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance
    from matching.semantic_matching import generate_semantic_matches
except ImportError:
    # If the above fails, try importing with 'src.' prefix
    from src.apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance
    from src.matching.semantic_matching import generate_semantic_matches

# --- CONFIGURATION ---
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
EXIT_TARGET = 0.99
TIME_OFFSET_HOURS = 1 # Shift forward to Europe/Stockholm (UTC+1)

# Check for API keys
KALSHI_KEY_READY = os.getenv("KALSHI_ACCESS_KEY") is not None
POLY_KEY_READY = os.getenv("POLYMARKET_WALLET_ADDRESS") is not None
WALLET_ADDR = os.getenv("POLYMARKET_WALLET_ADDRESS", "")

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
                    k_df = df[df['Platform'] == 'Kalshi'].rename(columns={'Ticker':'market_ticker', 'Title':'market_title'})
                    p_df = df[df['Platform'] == 'Polymarket'].rename(columns={'Ticker':'market_ticker', 'Title':'market_title'})
                    
                    if not k_df.empty and not p_df.empty:
                        matches = generate_semantic_matches(k_df, p_df, threshold=0.3)
                        # Map matches back
                        for _, m in matches.iterrows():
                            kt = m['kalshi_market_ticker']
                            pt = m['polymarket_market_ticker']
                            score = m['semantic_score']
                            
                            # Inject into main DF
                            df.loc[(df['Platform'] == 'Kalshi') & (df['Ticker'] == kt), 'Matched_Ticker'] = pt
                            df.loc[(df['Platform'] == 'Kalshi') & (df['Ticker'] == kt), 'Match_Score'] = score
                            df.loc[(df['Platform'] == 'Polymarket') & (df['Ticker'] == pt), 'Matched_Ticker'] = kt
                            df.loc[(df['Platform'] == 'Polymarket') & (df['Ticker'] == pt), 'Match_Score'] = score
                except Exception as e:
                    st.warning(f"Semantic Matching on cloud failed: {e}")

            # 3. Fetch Cash
            with st.spinner("💰 Calculating Cash Balances..."):
                k_bal = get_kalshi_balance()
                k_cash = k_bal.get('available_cents', 0) / 100
                p_cash = get_polymarket_balance(WALLET_ADDR)
                
                cash_rows = [
                    {"Platform": "Kalshi", "Ticker": "CASH", "Title": "Kalshi Available Cash", "Side": "N/A", "Value_USD": k_cash, "Profit_USD": 0, "Quantity": k_cash, "Price": 1.0},
                    {"Platform": "Polymarket", "Ticker": "CASH", "Title": "Polymarket USDC.e", "Side": "N/A", "Value_USD": p_cash, "Profit_USD": 0, "Quantity": p_cash, "Price": 1.0}
                ]
                df = pd.concat([df, pd.DataFrame(cash_rows)], ignore_index=True)
                
            if not df.empty:
                return df, "Live API"
        except Exception as e:
            st.warning(f"Live fetch failed: {e}")
    
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
    st.markdown("# PolyKalshi Terminal")
    
    # sidebar
    with st.sidebar:
        st.header("Connection")
        if KALSHI_KEY_READY: st.markdown('<div class="status-box status-ok">CONNECTED: KALSHI</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-box status-missing">MISSING: KALSHI</div>', unsafe_allow_html=True)
        if POLY_KEY_READY: st.markdown('<div class="status-box status-ok">CONNECTED: POLYGON</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-box status-missing">MISSING: POLYGON</div>', unsafe_allow_html=True)
        
        st.divider()
        if st.button("Force Global Re-Sync"):
            st.cache_data.clear()
            st.rerun()

    # 1. Load Data
    df, source = get_dashboard_data()
    if df.empty:
        st.error("No data found. Ensure your keys are in Streamlit Secrets.")
        return

    # 2. Key Metrics
    total_val = df['Value_USD'].sum()      # Cash + Positions
    total_profit = df['Profit_USD'].sum()
    cash_val = df[df['Ticker'] == 'CASH']['Value_USD'].sum()
    invested_val = total_val - cash_val
    adj_time = (datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime("%H:%M:%S")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Net Asset Value", f"${total_val:,.2f}", f"{total_profit:+.2f} Profit")
    m2.metric("Portfolio Weight", f"${invested_val:,.2f}")
    m3.metric("Available Cash", f"${cash_val:,.2f}")
    m4.metric("Last Update", adj_time)

    st.divider()

    # 3. Strategy / Matched Pairs Section (Improved Readability)
    st.subheader("Active Strategy Pairs")
    k_match = df[df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side = df[df['Platform'] == 'Polymarket']
    
    strategy_rows = []
    for _, k in k_match.iterrows():
        p = p_side[p_side['Ticker'] == k['Matched_Ticker']]
        if not p.empty:
            p = p.iloc[0]
            try:
                k_p, p_p = float(k['Price']), float(p['Price'])
                combined = k_p + p_p
                strategy_rows.append({
                    "Market Description": k['Title'],
                    "Kalshi Side": k['Side'],
                    "Kalshi Price": f"${k_p:.3f}",
                    "Poly Side": p['Side'],
                    "Poly Price": f"${p_p:.3f}",
                    "Combined Bid": f"${combined:.3f}",
                    "Target Gap": f"${max(0.99-combined, 0):.3f}"
                })
            except: pass
            
    if strategy_rows:
        st.dataframe(pd.DataFrame(strategy_rows), use_container_width=True)
    else:
        st.info("No strategy pairs detected. Run matching locally.")

    st.divider()

    # 4. Side-by-Side Charts (Better Alignment)
    st.subheader("Portfolio Allocations")
    col_chart1, col_chart2 = st.columns(2)
    
    # Pre-process for wrapped labels
    pos_only = df[df['Ticker'] != 'CASH'].copy()
    pos_only['WrappedTitle'] = pos_only['Title'].apply(wrap_label)
    
    with col_chart1:
        st.markdown("#### Kalshi Holdings")
        k_data = pos_only[pos_only['Platform'] == 'Kalshi'].sort_values('Value_USD', ascending=True)
        if not k_data.empty:
            fig_k = px.bar(k_data, y='WrappedTitle', x='Value_USD', orientation='h', 
                           color='Side', color_discrete_map={'YES':'#2ecc71','NO':'#e74c3c'},
                           text_auto='.2s', template="plotly_dark")
            fig_k.update_layout(height=max(400, len(k_data)*80), margin=dict(l=0, r=20, t=20, b=20),
                                yaxis=dict(title=None), xaxis=dict(title="Value in USD"))
            st.plotly_chart(fig_k, use_container_width=True)
        else: st.info("No Kalshi positions.")

    with col_chart2:
        st.markdown("#### Polymarket Holdings")
        p_data = pos_only[pos_only['Platform'] == 'Polymarket'].sort_values('Value_USD', ascending=True)
        if not p_data.empty:
            fig_p = px.bar(p_data, y='WrappedTitle', x='Value_USD', orientation='h', 
                           color='Side', color_discrete_map={'YES':'#3498db','NO':'#9b59b6'},
                           text_auto='.2s', template="plotly_dark")
            fig_p.update_layout(height=max(400, len(p_data)*80), margin=dict(l=0, r=20, t=20, b=20),
                                yaxis=dict(title=None), xaxis=dict(title="Value in USD"))
            st.plotly_chart(fig_p, use_container_width=True)
        else: st.info("No Polymarket positions.")

    # 5. History Section
    st.divider()
    st.subheader("Total Equity Growth")
    history_file = "Data/history/run_log.csv"
    if os.path.exists(history_file):
        try:
            h_df = pd.read_csv(history_file)
            fig_h = px.line(h_df, x='snapshot_time', y='total_value_usd', title="NAV Over Time", template="plotly_dark")
            st.plotly_chart(fig_h, use_container_width=True)
        except: st.info("History log found but unreadable.")
    else:
        st.info("💡 Run a local scan to generate performance logs.")

    # 6. Audit
    with st.expander("🔍 Portfolio Audit Log"):
        st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
