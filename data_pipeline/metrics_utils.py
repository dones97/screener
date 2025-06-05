import numpy as np

def calculate_avg_roce(fin, bal, min_years=4):
    """
    Calculate the average ROCE (Return on Capital Employed) over the most recent N years.

    ROCE = EBIT / Capital Employed
    Capital Employed = Total Assets - Total Current Liabilities

    Args:
        fin: pd.DataFrame, t.financials from yfinance
        bal: pd.DataFrame, t.balance_sheet from yfinance
        min_years: int, minimum years of data to average over

    Returns:
        float or None: Average ROCE (as a decimal, e.g., 0.15 for 15%) or None if not computable
    """
    try:
        # EBIT (Operating Income)
        ebit = None
        for name in ["EBIT", "Operating Income"]:
            if name in fin.index:
                ebit = fin.loc[name].sort_index()
                break

        # Capital Employed = Total Assets - Total Current Liabilities
        if "Total Assets" in bal.index and "Total Current Liabilities" in bal.index:
            total_assets = bal.loc["Total Assets"].sort_index()
            current_liabilities = bal.loc["Total Current Liabilities"].sort_index()
            capital_employed = total_assets - current_liabilities
        else:
            return None

        # Ensure we have enough data points and align indices
        if ebit is None or len(ebit.dropna()) < min_years or len(capital_employed.dropna()) < min_years:
            return None

        # Take the most recent min_years (last N columns)
        ebit = ebit.dropna().tail(min_years)
        capital_employed = capital_employed.dropna().reindex(ebit.index)
        roce_series = (ebit / capital_employed).replace([np.inf, -np.inf], np.nan).dropna()
        if len(roce_series) == 0:
            return None
        return roce_series.mean()
    except Exception:
        return None
