import streamlit as st
import pandas as pd
import yfinance as yf
import requests_cache
from functools import lru_cache
import time
import io
from datetime import datetime
import sqlite3

st.set_page_config(page_title="Metrics Ingestion & Percentile Precompute", layout="wide")
st.title("Metrics Ingestion & Percentile Precompute")

# Persist results in session state
if 'metrics_df' not in st.session_state:
    st.session_state.metrics_df = None
    st.session_state.cutoffs_df = None

st.markdown("""
Upload the **qualified tickers CSV** from the validation step, select desired percentiles,
and run to compute **Sales CAGR** and **Net Profit Margin** metrics per ticker, plus industry cutoffs.
""")

# File uploader
qualified_file = st.file_uploader("Upload qualified tickers CSV", type=["csv"])
# Percentiles selection
percentiles = st.multiselect(
    "Select percentiles to compute",
    options=[1, 5, 10, 20, 25, 50],
    default=[1, 5, 10]
)
# Option to store in SQLite
use_db = st.checkbox("Also store results in SQLite DB", value=False)

if st.button("Run Ingestion"):
    if qualified_file is None:
        st.error("Please upload the qualified tickers CSV.")
    else:
        df_q = pd.read_csv(qualified_file)
        tickers = df_q['YF_Ticker'].dropna().unique().tolist()
        st.write(f"Processing {len(tickers)} tickers...")

        # Setup HTTP caching
        try:
            requests_cache.install_cache('yf_cache', expire_after=86400)
        except:
            st.warning("requests_cache not installed; proceeding without HTTP caching.")

        @lru_cache(maxsize=None)
        def get_financials_and_industry(ticker):
            t = yf.Ticker(ticker)
            return t.financials, t.info.get("industry")

        def compute_metrics(fin_df):
            # Identify revenue row
            rev_field = "Total Revenue" if "Total Revenue" in fin_df.index else "Revenue"
            ni_field = "Net Income"
            if rev_field not in fin_df.index or ni_field not in fin_df.index:
                return None, None
            rev = fin_df.loc[rev_field].dropna()
            ni  = fin_df.loc[ni_field].dropna()
            if len(rev) < 2:
                return None, None
            rev = rev.sort_index()
            # CAGR
            n_periods = len(rev) - 1
            cagr = (rev.iloc[-1] / rev.iloc[0]) ** (1/n_periods) - 1
            # NPM average
            npm = (ni.reindex(rev.index) / rev).dropna().mean()
            return cagr, npm

        records = []
        progress = st.progress(0)
        for idx, ticker in enumerate(tickers):
            fin, industry = get_financials_and_industry(ticker)
            if fin is None or fin.empty or not industry:
                continue
            cagr, npm = compute_metrics(fin)
            if cagr is None or npm is None:
                continue
            records.append({
                "ticker": ticker,
                "industry": industry,
                "sales_cagr": cagr,
                "net_profit_margin": npm,
                "as_of": datetime.now().date()
            })
            if idx % 20 == 0:
                time.sleep(0.1)
            progress.progress((idx + 1) / len(tickers))

        metrics_df = pd.DataFrame(records)
        st.session_state.metrics_df = metrics_df

        # Compute cutoffs
        pct_records = []
        for industry, group in metrics_df.groupby("industry"):
            for pct in percentiles:
                pct_records.append({
                    "industry": industry,
                    "percentile": pct,
                    "sales_cagr_cutoff": group["sales_cagr"].quantile(pct/100),
                    "npm_cutoff": group["net_profit_margin"].quantile(pct/100)
                })
        cutoffs_df = pd.DataFrame(pct_records)
        st.session_state.cutoffs_df = cutoffs_df

        # Optionally store in SQLite
        if use_db:
            conn = sqlite3.connect("screener_data.db")
            metrics_df.to_sql("metrics", conn, if_exists="replace", index=False)
            cutoffs_df.to_sql("cutoffs", conn, if_exists="replace", index=False)
            conn.close()
            with open("screener_data.db", "rb") as f:
                db_bytes = f.read()
            st.download_button(
                "Download SQLite DB",
                data=db_bytes,
                file_name="screener_data.db",
                mime="application/x-sqlite3"
            )

        st.success("Metrics ingestion complete!")

# Display and download results
if st.session_state.metrics_df is not None:
    st.write("### Metrics Sample")
    st.dataframe(st.session_state.metrics_df.head())

    st.write("### Cutoffs Sample")
    st.dataframe(st.session_state.cutoffs_df.head())

    buf_m = io.StringIO()
    buf_c = io.StringIO()
    st.session_state.metrics_df.to_csv(buf_m, index=False)
    st.session_state.cutoffs_df.to_csv(buf_c, index=False)

    st.download_button(
        "Download metrics_table.csv",
        data=buf_m.getvalue(),
        file_name="metrics_table.csv",
        mime="text/csv"
    )
    st.download_button(
        "Download cutoffs_table.csv",
        data=buf_c.getvalue(),
        file_name="cutoffs_table.csv",
        mime="text/csv"
    )

