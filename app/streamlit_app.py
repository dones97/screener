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

# Industry selection (multiple, with "All" default)
all_industries = sorted(metrics['industry'].dropna().unique())
industries_options = ["All"] + all_industries
default_inds = ["All"]
selected_inds = st.sidebar.multiselect(
    "Select Industry/Industries",
    options=industries_options,
    default=default_inds,
    help="Select one or more industries. 'All' includes all industries."
)
if "All" in selected_inds:
    selected_inds = all_industries  # select all if 'All' is in selection

# Metric selection (checkboxes for Revenue CAGR and NPM)
st.sidebar.markdown("**Select metrics to screen on:**")
use_rev_cagr = st.sidebar.checkbox("Revenue CAGR", value=True)
use_npm = st.sidebar.checkbox("Net Profit Margin Avg", value=True)

if not use_rev_cagr and not use_npm:
    st.sidebar.warning("At least one metric must be selected. Defaulting to both.")
    use_rev_cagr = True
    use_npm = True

# Percentile choices (only show if metric is selected)
percentile_options = [1, 5, 10]
if use_rev_cagr:
    rev_cagr_p = st.sidebar.selectbox("Revenue CAGR percentile (top)", percentile_options, index=0)
else:
    rev_cagr_p = None
if use_npm:
    npm_p = st.sidebar.selectbox("Net Profit Margin percentile (top)", percentile_options, index=0)
else:
    npm_p = None

# Market Cap slider (order-of-magnitude steps, from 100 crore)
CRORE = 1e7  # 1 crore = 10 million
mc_min = 100 * CRORE
mc_max = float(np.nanmax(metrics['market_cap']))
steps = []
cur = mc_min
while cur < mc_max:
    steps.append(cur)
    cur *= 10
steps.append(mc_max)
steps = sorted(list(set([int(x) for x in steps])))

def display_cr(val):
    return f"{val/1e7:.0f} Cr"

mc_range = st.sidebar.select_slider(
    "Market Cap Range (Cr)",
    options=steps,
    value=(steps[0], steps[-1]),
    format_func=display_cr,
    help="Move slider. Each step is one order of magnitude (e.g., 100 Cr, 1000 Cr, 10,000 Cr, etc.)"
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

def filter_stocks(df, cutoffs, industries, use_rev_cagr, rev_cagr_p, use_npm, npm_p, mc_range):
    df = df.copy()
    df = df[df['industry'].isin(industries)]

    # Get percentile cutoffs for each industry as needed
    if use_rev_cagr:
        df['rev_cagr_cutoff'] = df.apply(
            lambda row: get_cutoff(row['industry'], 'revenue_cagr', rev_cagr_p), axis=1)
    if use_npm:
        df['npm_cutoff'] = df.apply(
            lambda row: get_cutoff(row['industry'], 'net_profit_margin_avg', npm_p), axis=1)

    # Apply selected metric cutoffs
    filter_cond = pd.Series(True, index=df.index)
    if use_rev_cagr:
        filter_cond = filter_cond & (df['revenue_cagr'] >= df['rev_cagr_cutoff'])
    if use_npm:
        filter_cond = filter_cond & (df['net_profit_margin_avg'] >= df['npm_cutoff'])
    df = df[filter_cond]

    # Market cap filter
    df = df[
        (df['market_cap'] >= mc_range[0]) &
        (df['market_cap'] <= mc_range[1])
    ]
    return df

filtered = filter_stocks(
    metrics, cutoffs, selected_inds, use_rev_cagr, rev_cagr_p, use_npm, npm_p, mc_range
)

# Compute industry counts (from the full metrics table)
industry_counts = metrics['industry'].value_counts().to_dict()
filtered['industry_count'] = filtered['industry'].map(industry_counts)

# Rename columns for display (so DataFrame columns match your intended display names)
display_df = filtered.copy()
display_df = display_df.rename(
    columns={
        'ticker': 'Ticker',
        'industry': 'Industry',
        'industry_count': 'Ind Count',
        'revenue_cagr': 'Rev CAGR',
        'net_profit_margin_avg': 'NPM Avg',
        'market_cap': 'MCap'
    }
)

# Reorder columns for display
show_cols = ["Ticker", "Industry", "Ind Count", "Rev CAGR", "NPM Avg", "MCap"]

# Main area
st.title("Stock Screener: Growth & Profitability Leaders")
metric_desc = []
if use_rev_cagr:
    metric_desc.append(f"top {rev_cagr_p}th pctile Revenue CAGR")
if use_npm:
    metric_desc.append(f"top {npm_p}th pctile Net Profit Margin")
metric_desc_str = ", ".join(metric_desc) if metric_desc else "no metric selected"
industry_display = ", ".join(selected_inds) if len(selected_inds) <= 5 else f"{len(selected_inds)} industries selected"
st.write(
    f"**Showing stocks in `{industry_display}`"
    f" ({metric_desc_str})"
    f", Market Cap between {display_cr(mc_range[0])} and {display_cr(mc_range[1])}**"
)

st.write(f"**{len(display_df)} stocks found**")

# Display all columns without horizontal scroll (Streamlit 1.22+: use 'hide_index' and 'width')
st.dataframe(
    display_df[show_cols]
    .sort_values(by=["Rev CAGR", "NPM Avg", "MCap"], ascending=False)
    .reset_index(drop=True),
    hide_index=True,
    use_container_width=True,
    column_order=show_cols,
)

# CSV export option
st.download_button(
    "Download filtered results as CSV",
    data=display_df[show_cols].to_csv(index=False),
    file_name="filtered_stocks.csv",
    mime="text/csv"
)

# Optionally show percentile ranks for context
with st.expander("Show percentile cutoffs by industry and metric"):
    st.dataframe(
        cutoffs.sort_values(["industry", "percentile"]),
        use_container_width=True
    )
