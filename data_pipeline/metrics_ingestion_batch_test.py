import pandas as pd
import yfinance as yf
import numpy as np
import logging
import time
import random

# Settings for batch test
DATA_DIR = '../data'
QUALIFIED_PATH = f"{DATA_DIR}/qualified_tickers.csv"
METRICS_PATH = f"{DATA_DIR}/metrics_table_test.csv"
BATCH_SIZE = 15  # Test with 15 tickers

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

SCREEN_METRICS = [
    "revenue_cagr",
    "net_income_cagr",
    "eps_cagr",
    "net_profit_margin_avg",
    "operating_cashflow_cagr"
]
DISPLAY_METRICS_MAP = {
    "market_cap": "marketCap",
    "pe_ratio": "trailingPE",
    "roce": "returnOnCapitalEmployed",
    "roe": "returnOnEquity",
    "debt_to_equity": "debtToEquity",
    "peg_ratio": "pegRatio",
    "interest_coverage": "interestCoverage"
}

PER_TICKER_SLEEP = 0.2  # Looser constraint for speed
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
    out = {"ticker": ticker, "industry": info.get("industry", None)}

    # Robust row matching
    revenue = get_fin_row(fin, ["Total Revenue", "Revenue"])
    net_income = get_fin_row(fin, ["Net Income", "NetIncome"])
    eps = get_fin_row(fin, ["Diluted EPS", "EPS", "Basic EPS"])
    ocf = get_fin_row(cf, ["Total Cash From Operating Activities", "Operating Cash Flow"])

    # Screening metrics using min_years (not min_years+1)
    out["revenue_cagr"] = calculate_cagr(revenue.tail(min_years)) if len(revenue) >= min_years else None
    out["net_income_cagr"] = calculate_cagr(net_income.tail(min_years)) if len(net_income) >= min_years else None
    out["eps_cagr"] = calculate_cagr(eps.tail(min_years)) if len(eps) >= min_years else None
    out["net_profit_margin_avg"] = (calculate_avg_margin(net_income.tail(min_years), revenue.tail(min_years))
                                    if len(net_income) >= min_years and len(revenue) >= min_years else None)
    out["operating_cashflow_cagr"] = calculate_cagr(ocf.tail(min_years)) if len(ocf) >= min_years else None

    for metric, yf_field in DISPLAY_METRICS_MAP.items():
        out[metric] = info.get(yf_field, None)
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
    qualified_df = pd.read_csv(QUALIFIED_PATH)
    tickers = qualified_df['YF_Ticker'].dropna().unique().tolist()[:BATCH_SIZE]
    logging.info(f"Batch-testing {len(tickers)} tickers: {tickers}")
    records = []
    for idx, ticker in enumerate(tickers):
        metrics = process_ticker_with_retries(ticker, min_years=4)
        if metrics:
            records.append(metrics)
        time.sleep(PER_TICKER_SLEEP + random.uniform(0, 0.2))
    metrics_df = pd.DataFrame(records)
    metrics_df.to_csv(METRICS_PATH, index=False)
    logging.info(f"Saved batch test results to {METRICS_PATH}")
    print(metrics_df.head(10))

if __name__ == "__main__":
    main()
