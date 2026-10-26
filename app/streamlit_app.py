import streamlit as st
import pandas as pd
import numpy as np
import os
import requests
import sqlite3
import tempfile

st.set_page_config(page_title="Growth & Profitability Screener", layout="wide")

@st.cache_data(ttl=86400)
def download_and_load_db():
    db_url = "https://raw.githubusercontent.com/dones97/valuation/main/screener_data.db"
    
    headers = {}
    if "GITHUB_TOKEN" in st.secrets:
        headers["Authorization"] = f"token {st.secrets['GITHUB_TOKEN']}"
        
    try:
        response = requests.get(db_url, headers=headers)
        if response.status_code != 200:
            st.error(f"Failed to download database from GitHub. Status code: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching database: {e}")
        return pd.DataFrame()
        
    temp_dir = tempfile.gettempdir()
    db_path = os.path.join(temp_dir, "screener_data.db")
    with open(db_path, "wb") as f:
        f.write(response.content)
        
    conn = sqlite3.connect(db_path)
    
    query_info = """
    SELECT c.ticker, c.company_name, c.sector, c.industry, k.market_cap 
    FROM companies c
    LEFT JOIN key_metrics k ON c.ticker = k.ticker
    """
    df_info = pd.read_sql(query_info, conn)
    
    query_financials = """
    SELECT ticker, year, sales, net_profit,
           expenses, other_income, interest, depreciation, profit_before_tax
    FROM annual_profit_loss
    WHERE net_profit IS NOT NULL
    """
    df_fin = pd.read_sql(query_financials, conn)
    conn.close()

    # Banks/NBFCs have no "Sales" row on Screener (their top line is "Revenue"), so sales is NULL for them.
    # Rebuild it from the P&L identity: Revenue = PBT - Other Income + Depreciation + Interest + Expenses
    # (matches reported sales within 1% for ~97% of non-financial company-years).
    derived_sales = (
        df_fin['profit_before_tax'] - df_fin['other_income'] + df_fin['depreciation']
        + df_fin['interest'] + df_fin['expenses']
    )
    df_fin['sales'] = df_fin['sales'].fillna(derived_sales)
    df_fin = df_fin.dropna(subset=['sales'])[['ticker', 'year', 'sales', 'net_profit']]

    if len(df_fin) > 0:
        df_fin['year_dt'] = pd.to_datetime(df_fin['year'], format='%b %Y', errors='coerce')
        df_fin = df_fin.dropna(subset=['year_dt']).sort_values(['ticker', 'year_dt'])
        
        def calculate_metrics(group):
            group = group.tail(5)
            if len(group) == 0:
                return pd.Series({'cagr_3yr': np.nan, 'cagr_5yr': np.nan, 'npm_avg_3yr': np.nan, 'npm_avg_5yr': np.nan})
                
            latest_sales = group.iloc[-1]['sales']

            # CAGR needs positive start and end sales. NPM is aggregate profit / aggregate sales over the
            # window, and only defined when every year has positive sales (zero/negative sales give inf/garbage).
            def cagr(first_sales, years):
                return (latest_sales / first_sales)**(1/years) - 1 if first_sales > 0 and latest_sales > 0 else np.nan

            def npm(window):
                return window['net_profit'].sum() / window['sales'].sum() if (window['sales'] > 0).all() else np.nan

            # 5 year
            if len(group) == 5:
                cagr_5yr = cagr(group.iloc[0]['sales'], 4)
                npm_avg_5yr = npm(group)
            else:
                cagr_5yr = np.nan
                npm_avg_5yr = np.nan

            # 3 year
            group_3yr = group.tail(3)
            if len(group_3yr) == 3:
                cagr_3yr = cagr(group_3yr.iloc[0]['sales'], 2)
                npm_avg_3yr = npm(group_3yr)
            else:
                cagr_3yr = np.nan
                npm_avg_3yr = np.nan
                
            return pd.Series({
                'cagr_3yr': cagr_3yr, 
                'cagr_5yr': cagr_5yr, 
                'npm_avg_3yr': npm_avg_3yr, 
                'npm_avg_5yr': npm_avg_5yr
            })
            
        metrics_df = df_fin.groupby('ticker').apply(calculate_metrics, include_groups=False).reset_index()
        final_df = pd.merge(df_info, metrics_df, on='ticker', how='left')
    else:
        final_df = df_info.copy()
        final_df['cagr_3yr'] = np.nan
        final_df['cagr_5yr'] = np.nan
        final_df['npm_avg_3yr'] = np.nan
        final_df['npm_avg_5yr'] = np.nan
        
    # Convert metric percentages to 0-1 range if they are large or keep as is?
    # Our calculation naturally outputs 0.15 for 15% CAGR, which matches typical representation.
    
    return final_df

df = download_and_load_db()

if df.empty:
    st.stop()

# Prepare sidebar controls
st.sidebar.header("Screening Criteria")

# 1. Sector Filter
all_sectors = sorted(df['sector'].dropna().unique())
selected_sectors = st.sidebar.multiselect(
    "Select Sector(s)",
    options=["All"] + all_sectors,
    default=["All"],
    help="Filter by broad sector categories."
)

if "All" in selected_sectors or len(selected_sectors) == 0:
    filtered = df.copy()
else:
    filtered = df[df['sector'].isin(selected_sectors)].copy()

# 2. Industry Filter (cascading)
all_industries = sorted(filtered['industry'].dropna().unique())
selected_inds = st.sidebar.multiselect(
    "Select Industry/Industries",
    options=["All"] + all_industries,
    default=["All"],
    help="Filter by specific industries within the chosen sectors."
)

if "All" not in selected_inds and len(selected_inds) > 0:
    filtered = filtered[filtered['industry'].isin(selected_inds)].copy()

st.sidebar.markdown("---")
st.sidebar.markdown("**Select metrics to screen on:**")

timeframe = st.sidebar.radio("Metrics Timeframe", ["3 Year", "5 Year"])
cagr_col = "cagr_3yr" if timeframe == "3 Year" else "cagr_5yr"
npm_col = "npm_avg_3yr" if timeframe == "3 Year" else "npm_avg_5yr"

use_rev_cagr = st.sidebar.checkbox(f"Filter by Revenue CAGR ({timeframe})", value=False)
use_npm = st.sidebar.checkbox(f"Filter by Net Profit Margin Avg ({timeframe})", value=False)

percentile_options = [1, 5, 10, 20, 25, 50]
peer_help = (
    "Compared against peers: companies in the SAME industry that also fall within the selected "
    "sector(s) and market cap range. 'Peers' in the results shows the size of this group."
)
if use_rev_cagr:
    rev_cagr_p = st.sidebar.selectbox("Revenue CAGR percentile (top %)", percentile_options, index=1, help=peer_help)
else:
    rev_cagr_p = None

if use_npm:
    npm_p = st.sidebar.selectbox("Net Profit Margin percentile (top %)", percentile_options, index=1, help=peer_help)
else:
    npm_p = None

st.sidebar.markdown("---")
# Market Cap slider
# The screener_data.db stores market cap directly in Crores
mc_min = 100.0  # 100 Crores
max_mc = df['market_cap'].max()
mc_max = float(max_mc) if pd.notna(max_mc) and max_mc > mc_min else 2000000.0  # Default to 20 Lakh Crores if missing

steps = []
cur = mc_min
while cur < mc_max:
    steps.append(cur)
    cur *= 10
if not steps or steps[-1] < mc_max:
    steps.append(mc_max)
steps = sorted(list(set([int(x) for x in steps])))

def display_cr(val):
    return f"{val:,.0f} Cr"

mc_range = st.sidebar.select_slider(
    "Market Cap Range (Cr)",
    options=steps,
    value=(steps[0], steps[-1]) if len(steps) > 1 else steps[0],
    format_func=display_cr,
    help="Move slider. Each step is one order of magnitude."
)

# Apply market cap filter
# We also include companies with missing market cap to not filter them out arbitrarily if data is missing
filtered = filtered[
    (filtered['market_cap'].isna()) | 
    ((filtered['market_cap'] >= mc_range[0]) & (filtered['market_cap'] <= mc_range[1]))
].copy()

# Apply percentile filters per industry
def get_industry_cutoff(data, metric_col, pct):
    return data.groupby('industry')[metric_col].apply(
        lambda x: np.percentile(x.dropna(), 100 - pct) if not x.dropna().empty else -np.inf
    ).to_dict()

# Peer group = companies left after the sector/industry/market cap filters. Both percentile cutoffs
# are computed on this same group and then combined, so one filter does not shrink the other's peers.
peer_counts = filtered['industry'].value_counts().to_dict()
passes = pd.Series(True, index=filtered.index)

if use_rev_cagr:
    cutoffs_cagr = get_industry_cutoff(filtered, cagr_col, rev_cagr_p)
    passes &= filtered[cagr_col] >= filtered['industry'].map(cutoffs_cagr)

if use_npm:
    cutoffs_npm = get_industry_cutoff(filtered, npm_col, npm_p)
    passes &= filtered[npm_col] >= filtered['industry'].map(cutoffs_npm)

filtered = filtered[passes].copy()
filtered['industry_count'] = filtered['industry'].map(peer_counts).astype('Int64')

# Sort numerically before the columns are formatted as strings
filtered = filtered.sort_values(
    by=[cagr_col, npm_col, 'industry_count'], ascending=[False, False, False], na_position='last'
)

# Rename columns for display
display_df = filtered.copy()
display_df = display_df.rename(
    columns={
        'ticker': 'Ticker',
        'company_name': 'Name',
        'sector': 'Sector',
        'industry': 'Industry',
        'industry_count': 'Peers',
        cagr_col: 'Rev CAGR',
        npm_col: 'NPM Avg',
        'market_cap': 'MCap'
    }
)

# Format numerical columns
display_df['MCap'] = display_df['MCap'].apply(lambda x: f"{x:,.0f} Cr" if pd.notna(x) else "N/A")
display_df['Rev CAGR'] = (display_df['Rev CAGR'] * 100).apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
display_df['NPM Avg'] = (display_df['NPM Avg'] * 100).apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")

show_cols = ["Ticker", "Name", "Sector", "Industry", "Peers", "Rev CAGR", "NPM Avg", "MCap"]
for col in show_cols:
    if col not in display_df.columns:
        display_df[col] = np.nan

# Main area
st.title("Stock Screener: Growth & Profitability Leaders")
metric_desc = []
if use_rev_cagr:
    metric_desc.append(f"top {rev_cagr_p}% Revenue CAGR")
if use_npm:
    metric_desc.append(f"top {npm_p}% Net Profit Margin")
metric_desc_str = ", ".join(metric_desc) if metric_desc else "no metrics filtered"

st.write(
    f"**Showing stocks matching {metric_desc_str}** | "
    f"Market Cap between {display_cr(mc_range[0])} and {display_cr(mc_range[1])}"
)

st.write(f"**{len(display_df)} stocks found**")

st.dataframe(
    display_df[show_cols].reset_index(drop=True),
    hide_index=True,
    use_container_width=True,
    column_order=show_cols,
)

# CSV export option
csv_export_df = filtered.rename(
    columns={
        'ticker': 'Ticker',
        'company_name': 'Name',
        'sector': 'Sector',
        'industry': 'Industry',
        'industry_count': 'Peers',
        cagr_col: 'Rev CAGR',
        npm_col: 'NPM Avg',
        'market_cap': 'MCap'
    }
)[show_cols]
st.download_button(
    "Download filtered results as CSV",
    data=csv_export_df.to_csv(index=False),
    file_name="filtered_stocks.csv",
    mime="text/csv"
)
