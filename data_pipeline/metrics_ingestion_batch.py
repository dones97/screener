import os
import pandas as pd
import yfinance as yf
import numpy as np
import logging
import time
import random
from data_pipeline.metrics_utils import calculate_avg_roce

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
QUALIFIED_PATH = os.path.join(DATA_DIR, "qualified_tickers.csv")
METRICS_PATH = os.path.join(DATA_DIR, "metrics_table.csv")

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

PER_TICKER_SLEEP = 0.2
MAX_RETRIES = 2

def get_fin_row(fin, names):
    for name in names:
        if name in fin.index:
            return fin.loc[name].dropna().sort_index()
    return pd.Series(dtype=float)

def calculate_cagr(series: pd.Series) -> float:
    if len(series) < 2 or series.iloc[0] == 0 or any(series <= 0):
        return None
    n = len(series) - 1
    try:
        return (series.iloc[-1] / series.iloc[0]) ** (1 / n) - 1
    except Exception:
        return None

def calculate_avg_margin(net_income: pd.Series, revenue: pd.Series) -> float:
    try:
        npm = (net_income / revenue).replace([np.inf, -np.inf], np.nan)
        npm = npm.infer_objects(copy=False).dropna()
        if len(npm) == 0:
            return None
        return npm.mean()
    except Exception:
        return None

def extract_financials(ticker: str, min_years: int = 4):
    t = yf.Ticker(ticker)
    fin = t.financials
    cf = t.cashflow
    info = t.info
    out = {
        "ticker": ticker,
        "industry": info.get("industry", None),
        "market_cap": info.get("marketCap", None)
    }
    # Robust row matching
    revenue = get_fin_row(fin, ["Total Revenue", "Revenue"])
    net_income = get_fin_row(fin, ["Net Income", "NetIncome"])

    # Compute metrics for screening (both must have min_years)
    out["revenue_cagr"] = calculate_cagr(revenue.tail(min_years)) if len(revenue) >= min_years else None
    out["net_profit_margin_avg"] = (calculate_avg_margin(net_income.tail(min_years), revenue.tail(min_years))
                                    if len(net_income) >= min_years and len(revenue) >= min_years else None)
    out["roce"] = calculate_avg_roce(fin, bal, min_years)

    return out

def process_ticker_with_retries(ticker, min_years=4):
    for attempt in range(MAX_RETRIES):
        try:
            return extract_financials(ticker, min_years)
        except Exception as e:
            logging.warning(f"[{ticker}] Error: {e} (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(1)
    logging.error(f"[{ticker}] Failed after {MAX_RETRIES} attempts")
    return None

def main():
    if not os.path.exists(QUALIFIED_PATH):
        logging.error(f"{QUALIFIED_PATH} not found! Place a qualified_tickers.csv in the data directory.")
        return

    qualified_df = pd.read_csv(QUALIFIED_PATH)
    all_tickers = qualified_df['YF_Ticker'].dropna().unique().tolist()

    if len(all_tickers) == 0:
        logging.error("No tickers found in qualified_tickers.csv.")
        return

    logging.info(f"Processing {len(all_tickers)} tickers from {QUALIFIED_PATH}")

    records = []
    for idx, ticker in enumerate(all_tickers):
        metrics = process_ticker_with_retries(ticker, min_years=4)
        if metrics:
            records.append(metrics)
        time.sleep(PER_TICKER_SLEEP + random.uniform(0, 0.2))
        if (idx + 1) % 50 == 0 or idx == len(all_tickers) - 1:
            logging.info(f"Processed {idx+1}/{len(all_tickers)} tickers")

    metrics_df = pd.DataFrame(records)
    # Ensure robust types for downstream filtering
    metrics_df = metrics_df.astype({
        "ticker": str,
        "industry": str,
        "market_cap": "float64",
        "revenue_cagr": "float64",
        "net_profit_margin_avg": "float64"
        "roce": "float64"
    }, errors='ignore')
    metrics_df.to_csv(METRICS_PATH, index=False)
    logging.info(f"Saved metrics for {len(metrics_df)} tickers to {METRICS_PATH}")

    print(metrics_df.head(10))

if __name__ == "__main__":
    main()
