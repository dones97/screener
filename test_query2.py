import sqlite3
import pandas as pd

db_path = r'c:\Users\dones\OneDrive\Documents\Investments\Valuation\Valuation guthub clone\valuation\screener_data.db'
conn = sqlite3.connect(db_path)

print("Total Companies:", pd.read_sql("SELECT COUNT(*) FROM companies", conn).iloc[0,0])
print("Companies with sector:", pd.read_sql("SELECT COUNT(*) FROM companies WHERE sector IS NOT NULL", conn).iloc[0,0])
print("Sample companies with sector:\n", pd.read_sql("SELECT * FROM companies WHERE sector IS NOT NULL LIMIT 2", conn))

print("Total Key Metrics:", pd.read_sql("SELECT COUNT(*) FROM key_metrics", conn).iloc[0,0])
print("Key metrics with market_cap:", pd.read_sql("SELECT COUNT(*) FROM key_metrics WHERE market_cap IS NOT NULL", conn).iloc[0,0])
print("Sample key metrics with market_cap:\n", pd.read_sql("SELECT * FROM key_metrics WHERE market_cap IS NOT NULL LIMIT 2", conn))

print("Total Annual Profit Loss:", pd.read_sql("SELECT COUNT(*) FROM annual_profit_loss", conn).iloc[0,0])
print("Annual PL with sales:", pd.read_sql("SELECT COUNT(*) FROM annual_profit_loss WHERE sales IS NOT NULL", conn).iloc[0,0])
print("Sample Annual PL with sales:\n", pd.read_sql("SELECT ticker, year, sales, net_profit FROM annual_profit_loss WHERE sales IS NOT NULL LIMIT 2", conn))

conn.close()
