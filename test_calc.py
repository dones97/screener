import sqlite3
import pandas as pd
import numpy as np

db_path = r'c:\Users\dones\OneDrive\Documents\Investments\Valuation\Valuation guthub clone\valuation\screener_data.db'
conn = sqlite3.connect(db_path)

# 1. Load companies and key_metrics
query_info = """
SELECT c.ticker, c.company_name, c.sector, c.industry, k.market_cap 
FROM companies c
LEFT JOIN key_metrics k ON c.ticker = k.ticker
"""
df_info = pd.read_sql(query_info, conn)

# 2. Load annual_profit_loss
query_financials = """
SELECT ticker, year, sales, net_profit 
FROM annual_profit_loss
WHERE sales IS NOT NULL AND net_profit IS NOT NULL
"""
df_fin = pd.read_sql(query_financials, conn)
conn.close()

if len(df_fin) > 0:
    # Convert year strings to datetime to sort them
    # Typically year is 'Mar 2023', 'Dec 2022'
    df_fin['year_dt'] = pd.to_datetime(df_fin['year'], format='%b %Y', errors='coerce')
    df_fin = df_fin.dropna(subset=['year_dt']).sort_values(['ticker', 'year_dt'])
    
    # Calculate 3yr and 5yr CAGR and NPM
    def calculate_metrics(group):
        group = group.tail(5) # get up to last 5 years
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
        
    metrics_df = df_fin.groupby('ticker').apply(calculate_metrics).reset_index()
    
    # Merge with info
    final_df = pd.merge(df_info, metrics_df, on='ticker', how='left')
    print("Merged shape:", final_df.shape)
    print(final_df.dropna(subset=['cagr_3yr']).head())
else:
    print("No financial data found.")
