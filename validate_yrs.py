import streamlit as st
import pandas as pd
import yfinance as yf
import requests_cache
from functools import lru_cache
import time
import io
from datetime import datetime

# Streamlit config
st.set_page_config(page_title="5-Year Data Validator", layout="wide")
st.title("Financial Data Validator (Last-N-Years)")

# Persist DataFrames across interactions
if 'qualified_df' not in st.session_state:
    st.session_state.qualified_df = None
    st.session_state.excluded_df = None

# Description (fixed string literal)
st.markdown(
    "Upload your NSE and BSE CSVs, set **N** (e.g. 5), and the app will require data for:\n\n"
    "- `CurrentYear-1`, `CurrentYear-2`, …, `CurrentYear-N`\n\n"
    "(even if `CurrentYear` itself isn’t available yet)."
)

# Upload inputs
nse_file = st.file_uploader("Upload NSE CSV", type="csv")
bse_file = st.file_uploader("Upload BSE CSV", type="csv")
min_years = st.number_input(
    "Minimum years (N) of data required", 
    min_value=1, max_value=20, value=4, step=1
)

if st.button("Validate Data"):
    if not nse_file or not bse_file:
        st.error("Please upload both NSE and BSE files.")
    else:
        # Read and map tickers
        nse_df = pd.read_csv(nse_file)
        bse_df = pd.read_csv(bse_file)
        nse_map = nse_df[['Ticker','ISIN']].drop_duplicates()
        nse_map['YF_Ticker'] = nse_map['Ticker'].astype(str) + '.NS'
        bse_map = (
            bse_df[['TckrSymb','ISIN']]
            .drop_duplicates()
            .rename(columns={'TckrSymb':'Ticker'})
        )
        bse_map['YF_Ticker'] = bse_map['Ticker'].astype(str) + '.BO'
        combined = pd.concat([
            nse_map[['ISIN','YF_Ticker']],
            bse_map[['ISIN','YF_Ticker']]
        ]).drop_duplicates().reset_index(drop=True)
        tickers = combined['YF_Ticker'].tolist()
        st.write(f"🔎 Validating {len(tickers)} tickers…")

        # Cache HTTP for 1 day
        requests_cache.install_cache('yf_cache', expire_after=86400)

        current_year = datetime.now().year
        # Required years = {current_year-1, ..., current_year-min_years}
        required_years = set(range(current_year - min_years, current_year))

        @lru_cache(maxsize=None)
        def get_financial_years(ticker):
            try:
                fin = yf.Ticker(ticker).financials
                if fin is None or fin.empty:
                    return set()
                return {dt.year for dt in fin.columns}
            except:
                return set()

        qualified, excluded = [], []
        progress = st.progress(0)
        batch_size = 50

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            for t in batch:
                years = get_financial_years(t)
                if required_years.issubset(years):
                    qualified.append({'YF_Ticker': t, 'Years': sorted(years)})
                else:
                    excluded.append({'YF_Ticker': t, 'Years': sorted(years)})
            progress.progress(min(i + batch_size, len(tickers)) / len(tickers))
            time.sleep(0.2)

        # Save into session state
        st.session_state.qualified_df = pd.DataFrame(qualified)
        st.session_state.excluded_df  = pd.DataFrame(excluded)
        st.success("✅ Validation complete!")

# If we have results, display & allow downloads
if st.session_state.qualified_df is not None:
    qdf = st.session_state.qualified_df
    edf = st.session_state.excluded_df

    st.write(f"✅ **Qualified**: {len(qdf)} tickers")
    st.write(f"⚠ **Excluded**: {len(edf)} tickers")

    buf_q, buf_e = io.StringIO(), io.StringIO()
    qdf.to_csv(buf_q, index=False)
    edf.to_csv(buf_e, index=False)

    st.download_button(
        "Download qualified CSV",
        data=buf_q.getvalue(),
        file_name="qualified_tickers.csv",
        mime="text/csv"
    )
    st.download_button(
        "Download excluded CSV",
        data=buf_e.getvalue(),
        file_name="excluded_tickers.csv",
        mime="text/csv"
    )
