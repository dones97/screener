import streamlit as st
import pandas as pd
import yfinance as yf
import requests_cache
from functools import lru_cache
import time
from datetime import datetime
import os

# Path to data folder (adjust if running from a different working directory)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
nse_path = os.path.join(DATA_DIR, "nse_equity_list.csv")
bse_path = os.path.join(DATA_DIR, "bse_equity_list.csv")
qualified_path = os.path.join(DATA_DIR, "qualified_tickers.csv")
excluded_path = os.path.join(DATA_DIR, "excluded_tickers.csv")

st.set_page_config(page_title="5-Year Data Validator", layout="wide")
st.title("Financial Data Validator (Last-N-Years)")

st.markdown(
    """
    This tool reads the latest NSE and BSE equity data from the repo's `data/` directory.
    It validates which tickers have at least N years of available financials on Yahoo Finance.
    Results are saved back to the `data/` directory for downstream processing.
    """
)

min_years = st.number_input(
    "Minimum years (N) of data required",
    min_value=1, max_value=20, value=4, step=1
)

run_validation = st.button("Run Validation using repo-stored equity lists")

if run_validation:
    if not (os.path.exists(nse_path) and os.path.exists(bse_path)):
        st.error(
            "NSE or BSE input files not found in data/. Please update them."
        )
    else:
        nse_df = pd.read_csv(nse_path)
        bse_df = pd.read_csv(bse_path)
        nse_map = nse_df[['Ticker', 'ISIN']].drop_duplicates()
        nse_map['YF_Ticker'] = nse_map['Ticker'].astype(str) + '.NS'
        bse_map = (
            bse_df[['TckrSymb', 'ISIN']]
            .drop_duplicates()
            .rename(columns={'TckrSymb': 'Ticker'})
        )
        bse_map['YF_Ticker'] = bse_map['Ticker'].astype(str) + '.BO'
        combined = pd.concat([
            nse_map[['ISIN', 'YF_Ticker']],
            bse_map[['ISIN', 'YF_Ticker']]
        ]).drop_duplicates().reset_index(drop=True)
        tickers = combined['YF_Ticker'].tolist()
        st.write(f"🔎 Validating {len(tickers)} tickers…")

        requests_cache.install_cache('yf_cache', expire_after=86400)
        current_year = datetime.now().year
        required_years = set(range(current_year - min_years, current_year))

        @lru_cache(maxsize=None)
        def get_financial_years(ticker):
            try:
                fin = yf.Ticker(ticker).financials
                if fin is None or fin.empty:
                    return set()
                return {dt.year for dt in fin.columns}
            except Exception:
                return set()

        qualified, excluded = [], []
        progress = st.progress(0)
        for idx, t in enumerate(tickers):
            years = get_financial_years(t)
            if required_years.issubset(years):
                qualified.append({'YF_Ticker': t, 'Years': sorted(years)})
            else:
                excluded.append({'YF_Ticker': t, 'Years': sorted(years)})
            if idx % 20 == 0 or idx == len(tickers) - 1:
                progress.progress((idx + 1) / len(tickers))

        qualified_df = pd.DataFrame(qualified)
        excluded_df = pd.DataFrame(excluded)

        qualified_df.to_csv(qualified_path, index=False)
        excluded_df.to_csv(excluded_path, index=False)
        st.success("✅ Validation complete! Results saved to data/.")

        st.write(f"Qualified: {len(qualified_df)}")
        st.write(f"Excluded: {len(excluded_df)}")

        # Show sample results and allow download
        st.write("### Qualified Sample")
        st.dataframe(qualified_df.head())
        st.write("### Excluded Sample")
        st.dataframe(excluded_df.head())

        st.download_button(
            "Download qualified_tickers.csv",
            data=qualified_df.to_csv(index=False),
            file_name="qualified_tickers.csv"
        )
        st.download_button(
            "Download excluded_tickers.csv",
            data=excluded_df.to_csv(index=False),
            file_name="excluded_tickers.csv"
        )
else:
    st.info(
        "To update the input files, replace `data/nse_equity_list.csv` and `data/bse_equity_list.csv` in the repo."
    )
    if os.path.exists(qualified_path):
        st.write("Latest Qualified Tickers Sample:")
        st.dataframe(pd.read_csv(qualified_path).head())
    if os.path.exists(excluded_path):
        st.write("Latest Excluded Tickers Sample:")
        st.dataframe(pd.read_csv(excluded_path).head())
