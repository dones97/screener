# Stock Screener

A robust, industry-aware stock screener for Indian equities, focused on growth and profitability metrics. Data is sourced from Yahoo Finance and processed into easily filterable tables for interactive analysis.

---

## Features

- **Industry-aware percentile cutoffs** for Revenue CAGR and Net Profit Margin (NPM)
- **Market Cap filtering** with intuitive slider
- **Downloadable CSV output** of filtered stocks
- **Pipeline for robust data validation and ingestion**

---

## Folder Structure

```
app/
  streamlit_app.py        # Main screener Streamlit app
data_pipeline/
  validate_yrs.py         # Streamlit app for validating years of financials
  metrics_ingestion_batch.py  # Batch script for pulling financial metrics
  gen_cutoffs_table.py    # Script for computing percentile cutoffs per industry
data/
  qualified_tickers.csv   # Tickers with enough years of data
  metrics_table.csv       # Main metrics table for screener
  cutoffs_table.csv       # Percentile cutoffs by industry
  nse_equity_list.csv     # Raw NSE equity list (input)
  bse_equity_list.csv     # Raw BSE equity list (input)
.github/
  workflows/
    main.yml              # GitHub Actions workflow for CI or automated data refresh
```

---

## Usage Guide

### 1. **Update Raw Equity Lists**

Place updated `nse_equity_list.csv` and `bse_equity_list.csv` in the `data/` folder.  
*(You can source these files from official NSE/BSE websites.)*

### 2. **Validate Tickers with Enough Data**

Run the validator Streamlit app:
```bash
streamlit run data_pipeline/validate_yrs.py
```
- Set the minimum years (`N`) of financials required.
- Click "Run Validation" to check all tickers.
- Results saved as `qualified_tickers.csv` and `excluded_tickers.csv` in `data/`.

### 3. **Ingest Metrics for Screenable Tickers**

Run the batch ingestion script:
```bash
python -m data_pipeline.metrics_ingestion_batch
```
- Pulls financial metrics for all qualified tickers.
- Saves results as `metrics_table.csv` in `data/`.

### 4. **Generate Industry Percentile Cutoffs**

Run the cutoffs computation script:
```bash
python data_pipeline/gen_cutoffs_table.py
```
- Calculates top 1%, 5%, 10% cutoffs by industry for each metric.
- Saves as `cutoffs_table.csv` in `data/`.

### 5. **Run the Screener App**

Launch the interactive screener:
```bash
streamlit run app/streamlit_app.py
```
- Choose industries, metrics, percentiles, and market cap range in the sidebar.
- View and download filtered results.

---

## GitHub Workflow

The GitHub Actions workflow (`.github/workflows/main.yml`) automates parts of the pipeline for CI/CD or scheduled data updates.

**Typical workflow tasks:**
- Install dependencies
- Run batch ingestion (`metrics_ingestion_batch`)
- Run cutoffs computation (`gen_cutoffs_table`)
- Optionally commit and push updated data files

**When is this used?**
- On each push/PR to `main` (or as scheduled)
- Ensures pipeline scripts run and update data tables automatically

**Manual steps (Streamlit apps):**
- Validation with `validate_yrs.py` must be run manually, as it requires interactive user input.

*You can customize the workflow file to automate as much of the pipeline as you want, except for manual validation (Streamlit UI).*

---

## FAQ

**Q: Do I need to run `validate_yrs.py` and other scripts every time?**  
A: Run `validate_yrs.py` if you update raw equity lists or want new tickers. Then run the batch and cutoffs scripts to refresh the metrics and cutoffs.

**Q: Does the main Streamlit app (`streamlit_app.py`) run the data pipeline scripts?**  
A: No. It only reads and displays the processed CSV files. You must run pipeline scripts yourself or automate via GitHub Actions.

**Q: Where do I place this README?**  
A: Place it at the root of your repository as `README.md`.

---

## License

MIT
