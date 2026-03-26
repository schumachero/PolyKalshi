import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys
import textwrap
import traceback
from datetime import datetime, timedelta

# --- CLEAN PATH SETUP ---
# Entry point is src/frontend_dashboard.py
# We want to import from sibling folders 'apis' and 'matching'
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# --- CONFIGURATION ---
PORTFOLIO_CSV = os.path.join("Data", "portfolio.csv")
EXIT_TARGET = 0.99
TIME_OFFSET_HOURS = 1 
VOLUME_PERCENTILE_THRESHOLD = 0.20 # 20% of position
VOLUME_FIXED_THRESHOLD = 10 # $10 worth

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
            # Absolute, clean imports from the 'src' directory added to sys.path above
            from apis.portfolio import get_kalshi_positions, get_polymarket_positions, get_kalshi_balance, get_polymarket_balance
            from matching.semantic_matching import generate_semantic_matches

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
                    {"Platform": "Kalshi", "Ticker": "CASH", "Title": "Kalshi Available Cash", "Side": "N/A", "Value_USD": k_cash, "Profit_USD": 0, "Quantity": k_cash, "Price": 1.0},
                    {"Platform": "Polymarket", "Ticker": "CASH", "Title": "Polymarket USDC.e", "Side": "N/A", "Value_USD": p_cash, "Profit_USD": 0, "Quantity": p_cash, "Price": 1.0}
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

    # 3. Strategy / Matched Pairs Section (Improved Readability & Fresh Pricing)
    st.subheader("Active Strategy Pairs")
    k_match = df[df['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    p_side = df[df['Platform'] == 'Polymarket']
    
    if not k_match.empty:
        with st.spinner("📈 Fetching Real-time Bids & Liquidity..."):
            try:
                from apis.orderbook import get_matched_orderbooks
                
                strategy_rows = []
                for _, k in k_match.iterrows():
                    p = p_side[p_side['Ticker'] == k['Matched_Ticker']]
                    if p.empty: continue
                    p = p.iloc[0]
                    
                    # Fetch fresh orderbooks for exact bid/volume
                    kt, pt = k['Ticker'], p['Ticker']
                    obs = get_matched_orderbooks(kt, pt, levels=1)
                    k_side_raw = k['Side']
                    p_side_raw = p['Side']
                    
                    # Logic: We want the BID price for the side we HOLD (to see what we can sell for)
                    k_b_list = obs.get('kalshi', {}).get(k_side_raw.lower(), {}).get('bids', [])
                    p_b_list = obs.get('polymarket', {}).get(p_side_raw.lower(), {}).get('bids', [])
                    
                    k_bid, k_vol = (k_b_list[0]['price'], k_b_list[0]['volume']) if k_b_list else (0, 0)
                    p_bid, p_vol = (p_b_list[0]['price'], p_b_list[0]['volume']) if p_b_list else (0, 0)
                    
                    # Liquidity Checks (Align with exit_monitor.py)
                    k_liq_ok = (k_vol >= VOLUME_PERCENTILE_THRESHOLD * k['Quantity']) or (k_vol * k_bid >= VOLUME_FIXED_THRESHOLD)
                    p_liq_ok = (p_vol >= VOLUME_PERCENTILE_THRESHOLD * p['Quantity']) or (p_vol * p_bid >= VOLUME_FIXED_THRESHOLD)
                    combined = k_bid + p_bid
                    
                    # Descriptive Sell Status
                    if combined >= EXIT_TARGET and k_liq_ok and p_liq_ok:
                        sell_status = "✅ Ready to Exit"
                    elif combined >= EXIT_TARGET:
                        sell_status = "⚠️ Low Volume"
                    else:
                        sell_status = "⏳ Pending Price"
                    
                    # Hedging Detection
                    is_hedge = "Standard" if k_side_raw != p_side_raw else "⚠️ Directional Same-Side"

                    strategy_rows.append({
                        "Description": k['Title'],
                        "Combo Bid": f"${combined:.3f}",
                        "Sellable Status": sell_status,
                        "Hedge Type": is_hedge,
                        "Kalshi": f"{k_side_raw} (${k_bid:.3f})",
                        "Poly": f"{p_side_raw} (${p_bid:.3f})",
                        "Gap": f"${max(0.99-combined, 0):.3f}"
                    })
                
                if strategy_rows:
                    strat_df = pd.DataFrame(strategy_rows)
                    # Shift Index to start at 1
                    strat_df.index = range(1, len(strat_df) + 1)
                    st.dataframe(strat_df, use_container_width=True)
                else:
                    st.info("No active strategy pairs detected.")
            except Exception as e_strat:
                st.warning(f"Strategy view failed: {e_strat}")
    else:
        st.info("No strategy pairs detected. Ensure you have positions on both platforms.")

    st.divider()

    # 4. Unified Portfolio Allocations (Matched pairs on same row)
    st.subheader("Portfolio Allocations")
    
    # Process for unified display
    pos_only = df[df['Ticker'] != 'CASH'].copy()
    
    # Separate Matched from Unmatched
    matched_k = pos_only[pos_only['Platform'] == 'Kalshi'].dropna(subset=['Matched_Ticker'])
    matched_p = pos_only[pos_only['Platform'] == 'Polymarket'].dropna(subset=['Matched_Ticker'])
    
    unmatched_k = pos_only[(pos_only['Platform'] == 'Kalshi') & (pos_only['Matched_Ticker'].isna() | (pos_only['Matched_Ticker'] == ""))]
    unmatched_p = pos_only[(pos_only['Platform'] == 'Polymarket') & (pos_only['Matched_Ticker'].isna() | (pos_only['Matched_Ticker'] == ""))]
    
    if not matched_k.empty:
        st.markdown("#### 🤝 Matched Hedge Pairs")
        unified_rows = []
        # We iterate over matched_k and find their partners in matched_p
        # To avoid double counting, we track which Poly tickers we've paired
        paired_poly_tickers = set()
        
        for _, k in matched_k.iterrows():
            p = matched_p[matched_p['Ticker'] == k['Matched_Ticker']]
            if p.empty: continue
            p = p.iloc[0]
            paired_poly_tickers.add(p['Ticker'])
            
            unified_rows.append({
                "Strategy": k['Title'],
                "Kalshi Side": k['Side'],
                "Kalshi Qty": int(k['Quantity']),
                "Kalshi Val": f"${k['Value_USD']:,.2f}",
                "Polymarket Side": p['Side'],
                "Polymarket Qty": f"{p['Quantity']:,.0f}",
                "Polymarket Val": f"${p['Value_USD']:,.2f}",
                "Combined Val": k['Value_USD'] + p['Value_USD'],
                "Combined P&L": k['Profit_USD'] + p['Profit_USD']
            })
        
        if unified_rows:
            u_df = pd.DataFrame(unified_rows).sort_values("Combined Val", ascending=False)
            st.dataframe(u_df.style.format({"Combined Val": "${:,.2f}", "Combined P&L": "${:,.2f}"}), use_container_width=True, hide_index=True)

    if not unmatched_k.empty or not unmatched_p.empty:
        st.markdown("#### 🚩 Single-Sided Positions")
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            if not unmatched_k.empty:
                st.markdown("**Kalshi Only**")
                st.dataframe(unmatched_k[['Title', 'Side', 'Quantity', 'Value_USD', 'Profit_USD']].rename(columns={'Profit_USD':'P&L'}), use_container_width=True, hide_index=True)
            else: st.info("No single-sided Kalshi positions.")
        with col_u2:
            if not unmatched_p.empty:
                st.markdown("**Polymarket Only**")
                st.dataframe(unmatched_p[['Title', 'Side', 'Quantity', 'Value_USD', 'Profit_USD']].rename(columns={'Profit_USD':'P&L'}), use_container_width=True, hide_index=True)
            else: st.info("No single-sided Polymarket positions.")

    # 5. Visual Summary Charts
    st.divider()
    st.subheader("Exposure Distribution")
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # Sunburst or Pie for Platform split
        fig_platform = px.pie(df, values='Value_USD', names='Platform', title="Asset Distribution by Platform",
                             color_discrete_map={'Kalshi':'#2ecc71', 'Polymarket':'#3498db'},
                             template="plotly_dark", hole=0.4)
        st.plotly_chart(fig_platform, use_container_width=True)
        
    with col_chart2:
        # Bar chart for Top 10 Positions by USD value
        top_pos = pos_only.sort_values('Value_USD', ascending=False).head(10)
        top_pos['WrappedTitle'] = top_pos['Title'].apply(wrap_label)
        fig_top = px.bar(top_pos, x='Value_USD', y='WrappedTitle', orientation='h', 
                         title="Top 10 Largest Positions",
                         color='Platform', color_discrete_map={'Kalshi':'#2ecc71', 'Polymarket':'#3498db'},
                         template="plotly_dark")
        fig_top.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_top, use_container_width=True)

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
