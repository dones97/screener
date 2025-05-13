import os
import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
METRICS_PATH = os.path.join(DATA_DIR, "metrics_table.csv")
CUTOFFS_PATH = os.path.join(DATA_DIR, "cutoffs_table.csv")

PERCENTILES = [1, 5, 10]
METRICS = ["revenue_cagr", "net_profit_margin_avg"]

def main():
    df = pd.read_csv(METRICS_PATH)
    # Remove rows with missing industry or metrics
    df = df.dropna(subset=["industry"] + METRICS)

    cutoff_rows = []
    for industry, grp in df.groupby("industry"):
        for p in PERCENTILES:
            cut = {"industry": industry, "percentile": p}
            for metric in METRICS:
                val = grp[metric].quantile(1 - p / 100.0)
                cut[metric] = val
            cutoff_rows.append(cut)
    cutoffs = pd.DataFrame(cutoff_rows)
    cutoffs.to_csv(CUTOFFS_PATH, index=False)
    print(cutoffs.head(10))

if __name__ == "__main__":
    main()
