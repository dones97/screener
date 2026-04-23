import sqlite3
import pandas as pd
import numpy as np

db_path = r'c:\Users\dones\OneDrive\Documents\Investments\Valuation\Valuation guthub clone\valuation\screener_data.db'
conn = sqlite3.connect(db_path)

# Query to get sector, industry, market cap
query_info = """
SELECT c.ticker, c.company_name, c.sector, c.industry, k.market_cap 
FROM companies c
JOIN key_metrics k ON c.ticker = k.ticker
WHERE c.sector IS NOT NULL AND k.market_cap IS NOT NULL
LIMIT 10
"""
df_info = pd.read_sql(query_info, conn)
print("Info:")
print(df_info.head())

# Query for CAGR and NPM
query_financials = """
SELECT ticker, year, sales, net_profit 
FROM annual_profit_loss
WHERE sales IS NOT NULL AND net_profit IS NOT NULL
LIMIT 10
"""
df_fin = pd.read_sql(query_financials, conn)
print("\nFinancials:")
print(df_fin.head(10))

conn.close()
