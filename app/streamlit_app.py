import streamlit as st
import pandas as pd
import numpy as np
import os

# File paths (adjust if your structure is different)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
METRICS_PATH = os.path.join(DATA_DIR, "metrics_table.csv")
CUTOFFS_PATH = os.path.join(DATA_DIR, "cutoffs_table.csv")

# Load data
@st.cache_data
def load_data():
    metrics = pd.read_csv(METRICS_PATH)
    cutoffs = pd.read_csv(CUTOFFS_PATH)
    return metrics, cutoffs

metrics, cutoffs = load_data()

# Prepare sidebar controls
st.sidebar.header("Screening Criteria")

# Industry selection
industries = ["All"] + sorted(metrics['industry'].dropna().unique())
industry = st.sidebar.selectbox("Select Industry", industries)

# Percentile choices
percentile_options = [1, 5, 10]
rev_cagr_p = st.sidebar.selectbox("Revenue CAGR percentile (top)", percentile_options, index=2)
npm_p = st.sidebar.selectbox("Net Profit Margin percentile (top)", percentile_options, index=2)

# Market Cap slider
min_mc = float(np.nanmin(metrics['market_cap']))
max_mc = float(np.nanmax(metrics['market_cap']))
market_cap_range = st.sidebar.slider(
    "Market Cap Range",
    min_value=min_mc,
    max_value=max_mc,
    value=(min_mc, max_mc),
    step=1e6,
    format="%.0f"
)

# Main filtering logic
def get_cutoff(ind, metric, pct):
    """Return the cutoff value for an industry, metric, and percentile."""
    row = cutoffs[
        (cutoffs['industry'] == ind) &
        (cutoffs['percentile'] == pct)
    ]
    if not row.empty:
        return float(row.iloc[0][metric])
    else:
        return None

def filter_stocks(df, cutoffs, industry, rev_cagr_p, npm_p, mc_range):
    # Subset by industry if needed
    df = df.copy()
    if industry != "All":
        df = df[df['industry'] == industry]

    # Get percentile cutoffs for each industry (or for selected industry)
    if industry == "All":
        # Apply per-industry cutoffs for each row
        df['rev_cagr_cutoff'] = df.apply(
            lambda row: get_cutoff(row['industry'], 'revenue_cagr', rev_cagr_p), axis=1)
        df['npm_cutoff'] = df.apply(
            lambda row: get_cutoff(row['industry'], 'net_profit_margin_avg', npm_p), axis=1)
    else:
        # Get cutoffs once
        rev_cagr_cutoff = get_cutoff(industry, 'revenue_cagr', rev_cagr_p)
        npm_cutoff = get_cutoff(industry, 'net_profit_margin_avg', npm_p)
        df['rev_cagr_cutoff'] = rev_cagr_cutoff
        df['npm_cutoff'] = npm_cutoff

    # Apply metric cutoffs
    df = df[
        (df['revenue_cagr'] >= df['rev_cagr_cutoff']) &
        (df['net_profit_margin_avg'] >= df['npm_cutoff'])
    ]
    # Market cap filter
    df = df[
        (df['market_cap'] >= mc_range[0]) &
        (df['market_cap'] <= mc_range[1])
    ]
    return df

filtered = filter_stocks(metrics, cutoffs, industry, rev_cagr_p, npm_p, market_cap_range)

# Main area
st.title("Stock Screener: Growth & Profitability Leaders")
st.write(
    f"**Showing stocks in `{industry}` industry" +
    ("" if industry == "All" else f" (top {rev_cagr_p}th pctile Revenue CAGR, top {npm_p}th pctile Net Profit Margin)") +
    f", Market Cap between {market_cap_range[0]:,.0f} and {market_cap_range[1]:,.0f}**"
)

st.write(f"**{len(filtered)} stocks found**")
show_cols = ["ticker", "industry", "revenue_cagr", "net_profit_margin_avg", "market_cap"]
st.dataframe(
    filtered[show_cols]
    .sort_values(by=["revenue_cagr", "net_profit_margin_avg", "market_cap"], ascending=False)
    .reset_index(drop=True),
    use_container_width=True
)

# CSV export option
st.download_button(
    "Download filtered results as CSV",
    data=filtered[show_cols].to_csv(index=False),
    file_name="filtered_stocks.csv",
    mime="text/csv"
)

# Optionally show percentile ranks for context
with st.expander("Show percentile cutoffs by industry and metric"):
    st.dataframe(
        cutoffs.sort_values(["industry", "percentile"]),
        use_container_width=True
    )
