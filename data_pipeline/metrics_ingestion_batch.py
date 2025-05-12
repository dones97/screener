import pandas as pd
import yfinance as yf
import numpy as np
import sqlite3
import os
import logging
import time
import random
from datetime import datetime
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
QUALIFIED_PATH = os.path.join(DATA_DIR, "qualified_tickers.csv")
METRICS_PATH = os.path.join(DATA_DIR, "metrics_table.csv")
CUTOFFS_PATH = os.path.join(DATA_DIR, "cutoffs_table.csv")
DB_PATH = os.path.join(DATA_DIR, "screener_data.db")

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# Screening/percentile metrics
SCREEN_METRICS = [
    "revenue_cagr",
    "net_income_cagr",
    "eps_cagr",
    "net_profit_margin_avg",
    "operating_cashflow_cagr"
]

# Display metrics to pull from yfinance.info, if available
DISPLAY_METRICS_MAP = {
    "market_cap": "marketCap",
    "pe_ratio": "trailingPE",
    "roce": "returnOnCapitalEmployed",  # Not always available
    "roe": "returnOnEquity",
    "debt_to_equity": "debtToEquity",
    "pledged_percentage": "pledgedShares",  # Not available in yfinance (skip if not present)
    "piotroski_score": "piotroskiScore",   # Not available in yfinance (skip if not present)
    "peg_ratio": "pegRatio",
    "interest_coverage": "interestCoverage"  # Not always available
}

MAX_RETRIES = 4
RETRY_WAIT = 60  # seconds
PER_TICKER_SLEEP = 1.0  # seconds

# Helper: calculate CAGR
def calculate_cagr(series: pd.Series) -> Optional[float]:
    if len(series) < 2 or series.iloc[0] == 0 or any(series <= 0):
        return None
    n = len(series) - 1
    try:
        return (series.iloc[-1] / series.iloc[0]) ** (1 / n) - 1
    except Exception:
        return None

# Helper: average NPM
def calculate_avg_margin(net_income: pd.Series, revenue: pd.Series) -> Optional[float]:
    try:
        npm = (net_income / revenue).replace([np.inf, -np.inf], np.nan)
        npm = npm.infer_objects(copy=False).dropna()  # Fix FutureWarning
        if len(npm) == 0:
            return None
        return npm.mean()
    except Exception:
        return None

def safe_get(info, field):
    return info.get(field, None) if info else None

def extract_financials(ticker: str, min_years: int = 4):
    """
    Returns a dict of all metrics, or None if not enough data.
    """
    t = yf.Ticker(ticker)
    fin = t.financials
    cf = t.cashflow
    bal = t.balance_sheet
    info = t.info
    out = {"ticker": ticker, "industry": safe_get(info, "industry")}
    
    # Screening metrics
    # 1. Revenue CAGR
    rev_row = "Total Revenue" if "Total Revenue" in fin.index else "Revenue"
    ni_row = "Net Income"
    eps_row = "Diluted EPS" if "Diluted EPS" in fin.index else "Basic EPS"
    ocf_row = "Total Cash From Operating Activities"
    try:
        revenue = fin.loc[rev_row].dropna().sort_index()
        net_income = fin.loc[ni_row].dropna().sort_index()
        eps = fin.loc[eps_row].dropna().sort_index() if eps_row in fin.index else None
        ocf = cf.loc[ocf_row].dropna().sort_index() if ocf_row in cf.index else None

        # Only consider if at least min_years+1 data points
        if len(revenue) >= min_years + 1:
            out["revenue_cagr"] = calculate_cagr(revenue.tail(min_years+1))
        else:
            out["revenue_cagr"] = None

        if len(net_income) >= min_years + 1:
            out["net_income_cagr"] = calculate_cagr(net_income.tail(min_years+1))
        else:
            out["net_income_cagr"] = None

        if eps is not None and len(eps) >= min_years + 1:
            out["eps_cagr"] = calculate_cagr(eps.tail(min_years+1))
        else:
            out["eps_cagr"] = None

        if (
            len(net_income) >= min_years and
            len(revenue) >= min_years
        ):
            out["net_profit_margin_avg"] = calculate_avg_margin(
                net_income.tail(min_years), revenue.tail(min_years)
            )
        else:
            out["net_profit_margin_avg"] = None

        if ocf is not None and len(ocf) >= min_years + 1:
            out["operating_cashflow_cagr"] = calculate_cagr(ocf.tail(min_years+1))
        else:
            out["operating_cashflow_cagr"] = None
    except Exception as e:
        logging.warning(f"[{ticker}] Error calculating screening metrics: {e}")
        for m in SCREEN_METRICS:
            out[m] = None

    # Display info metrics
    for metric, yf_field in DISPLAY_METRICS_MAP.items():
        out[metric] = safe_get(info, yf_field)
    # Piotroski, pledged pct: yfinance does not provide these; leave as None

    return out

def process_ticker_with_retries(ticker, min_years=4):
    for attempt in range(MAX_RETRIES):
        try:
            metrics = extract_financials(ticker, min_years=min_years)
            return metrics
        except Exception as e:
            err = str(e).lower()
            if "too many requests" in err or "rate limit" in err or "timed out" in err:
                wait_time = RETRY_WAIT * (attempt + 1) + random.uniform(0, 10)
                logging.warning(f"[{ticker}] Rate limited or network error (attempt {attempt+1}/{MAX_RETRIES}). Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                logging.warning(f"[{ticker}] Error: {e}")
                return None
    logging.error(f"[{ticker}] Failed after {MAX_RETRIES} attempts due to rate limiting.")
    return None

def main():
    # Load qualified tickers
    if not os.path.exists(QUALIFIED_PATH):
        logging.error(f"Missing {QUALIFIED_PATH}. Please generate it first.")
        return

    qualified_df = pd.read_csv(QUALIFIED_PATH)
    tickers = qualified_df['YF_Ticker'].dropna().unique().tolist()
    logging.info(f"Processing {len(tickers)} tickers from {QUALIFIED_PATH}")

    records = []
    for idx, ticker in enumerate(tickers):
        metrics = process_ticker_with_retries(ticker, min_years=4)
        if metrics:
            records.append(metrics)
        time.sleep(PER_TICKER_SLEEP + random.uniform(0, 0.5))  # Add jitter to avoid pattern

        if (idx + 1) % 50 == 0 or idx == len(tickers) - 1:
            logging.info(f"Processed {idx+1}/{len(tickers)} tickers")

    metrics_df = pd.DataFrame(records)
    metrics_df.to_csv(METRICS_PATH, index=False)
    logging.info(f"Saved metrics for {len(metrics_df)} tickers to {METRICS_PATH}")

    # Compute industry percentiles for screening metrics
    pct_list = [1, 5, 10, 20, 25, 50]
    pct_records = []
    for industry, group in metrics_df.groupby("industry"):
        for pct in pct_list:
            values = {}
            for metric in SCREEN_METRICS:
                try:
                    values[metric] = group[metric].dropna().quantile(pct / 100.0)
                except Exception:
                    values[metric] = None
            pct_records.append(
                dict(
                    industry=industry,
                    percentile=pct,
                    **values
                )
            )
    cutoffs_df = pd.DataFrame(pct_records)
    cutoffs_df.to_csv(CUTOFFS_PATH, index=False)
    logging.info(f"Saved percentiles to {CUTOFFS_PATH}")

    # Also save to SQLite
    try:
        conn = sqlite3.connect(DB_PATH)
        metrics_df.to_sql("metrics", conn, if_exists="replace", index=False)
        cutoffs_df.to_sql("cutoffs", conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"Saved metrics and cutoffs to SQLite at {DB_PATH}")
    except Exception as e:
        logging.warning(f"Could not save to SQLite: {e}")

    logging.info("Metrics ingestion batch complete.")

if __name__ == "__main__":
    main()
