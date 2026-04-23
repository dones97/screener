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
    SELECT ticker, year, sales, net_profit 
    FROM annual_profit_loss
    WHERE sales IS NOT NULL AND net_profit IS NOT NULL
    """
    df_fin = pd.read_sql(query_financials, conn)
    conn.close()
    
    if len(df_fin) > 0:
        df_fin['year_dt'] = pd.to_datetime(df_fin['year'], format='%b %Y', errors='coerce')
        df_fin = df_fin.dropna(subset=['year_dt']).sort_values(['ticker', 'year_dt'])
        
        def calculate_metrics(group):
            group = group.tail(5)
            if len(group) == 0:
                return pd.Series({'cagr_3yr': np.nan, 'cagr_5yr': np.nan, 'npm_avg_3yr': np.nan, 'npm_avg_5yr': np.nan})
                
            latest_sales = group.iloc[-1]['sales']
            
            # 5 year
            if len(group) == 5:
                first_sales_5yr = group.iloc[0]['sales']
                cagr_5yr = (latest_sales / first_sales_5yr)**(1/4) - 1 if first_sales_5yr > 0 else np.nan
                npm_avg_5yr = (group['net_profit'] / group['sales']).mean()
            else:
                cagr_5yr = np.nan
                npm_avg_5yr = np.nan
                
            # 3 year
            group_3yr = group.tail(3)
            if len(group_3yr) == 3:
                first_sales_3yr = group_3yr.iloc[0]['sales']
                cagr_3yr = (latest_sales / first_sales_3yr)**(1/2) - 1 if first_sales_3yr > 0 else np.nan
                npm_avg_3yr = (group_3yr['net_profit'] / group_3yr['sales']).mean()
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
if use_rev_cagr:
    rev_cagr_p = st.sidebar.selectbox("Revenue CAGR percentile (top %)", percentile_options, index=1, help="Compared against other companies in the SAME industry.")
else:
    rev_cagr_p = None

if use_npm:
    npm_p = st.sidebar.selectbox("Net Profit Margin percentile (top %)", percentile_options, index=1, help="Compared against other companies in the SAME industry.")
else:
    npm_p = None

st.sidebar.markdown("---")
# Market Cap slider
CRORE = 1e7
mc_min = 100 * CRORE
max_mc = df['market_cap'].max()
mc_max = float(max_mc) if pd.notna(max_mc) else mc_min * 10
steps = []
cur = mc_min
while cur < mc_max:
    steps.append(cur)
    cur *= 10
if not steps or steps[-1] < mc_max:
    steps.append(mc_max)
steps = sorted(list(set([int(x) for x in steps])))

def display_cr(val):
    return f"{val/1e7:,.0f} Cr"

mc_range = st.sidebar.select_slider(
    "Market Cap Range (Cr)",
    options=steps,
    value=(steps[0], steps[-1]) if len(steps) > 1 else steps[0],
    format_func=display_cr,
    help="Move slider. Each step is one order of magnitude."
)

# Apply market cap filter
filtered = filtered[
    (filtered['market_cap'] >= mc_range[0]) &
    (filtered['market_cap'] <= mc_range[1])
].copy()

# Apply percentile filters per industry
def get_industry_cutoff(data, metric_col, pct):
    return data.groupby('industry')[metric_col].apply(
        lambda x: np.percentile(x.dropna(), 100 - pct) if not x.dropna().empty else -np.inf
    ).to_dict()

cutoffs_cagr = {}
cutoffs_npm = {}

if use_rev_cagr:
    cutoffs_cagr = get_industry_cutoff(filtered, cagr_col, rev_cagr_p)
    filtered['cagr_cutoff'] = filtered['industry'].map(cutoffs_cagr)
    filtered = filtered[filtered[cagr_col] >= filtered['cagr_cutoff']]

if use_npm:
    cutoffs_npm = get_industry_cutoff(filtered, npm_col, npm_p)
    filtered['npm_cutoff'] = filtered['industry'].map(cutoffs_npm)
    filtered = filtered[filtered[npm_col] >= filtered['npm_cutoff']]

# Compute industry counts
industry_counts = df['industry'].value_counts().to_dict()
filtered['industry_count'] = filtered['industry'].map(industry_counts)

# Rename columns for display
display_df = filtered.copy()
display_df = display_df.rename(
    columns={
        'ticker': 'Ticker',
        'company_name': 'Name',
        'sector': 'Sector',
        'industry': 'Industry',
        'industry_count': 'Ind Count',
        cagr_col: 'Rev CAGR',
        npm_col: 'NPM Avg',
        'market_cap': 'MCap'
    }
)

# Format numerical columns
display_df['MCap'] = (display_df['MCap'] / 1e7).map('{:,.0f}'.format) + " Cr"
display_df['Rev CAGR'] = (display_df['Rev CAGR'] * 100).map('{:.1f}%'.format)
display_df['NPM Avg'] = (display_df['NPM Avg'] * 100).map('{:.1f}%'.format)

show_cols = ["Ticker", "Name", "Sector", "Industry", "Ind Count", "Rev CAGR", "NPM Avg", "MCap"]
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
    display_df[show_cols]
    .sort_values(by=["Rev CAGR", "NPM Avg", "Ind Count"], ascending=[False, False, False])
    .reset_index(drop=True),
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
        'industry_count': 'Ind Count',
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
