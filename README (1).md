# Automated ETL & Statistical Anomaly Detection Pipeline

A modular Python pipeline that fetches daily market data from Yahoo Finance,
validates and enriches it, stores it in SQLite or PostgreSQL, scores unusual
price/volume behavior, and displays results in a Streamlit dashboard.

> **Data-use note:** `yfinance` is an unofficial interface to Yahoo Finance.
> Availability, coverage, and terms may change. Check the applicable data
> provider terms before using this project in production or for commercial use.
> This example is for analytics and monitoring, not investment advice.

## Architecture

```mermaid
flowchart LR
    YF["Yahoo Finance / yfinance"] --> ETL["etl_pipeline.py"]
    ETL --> Q["Quality checks<br/>schema, nulls, duplicates"]
    Q --> KPI["KPI engineering<br/>7-day and 30-day"]
    KPI --> DB[("SQLAlchemy database<br/>SQLite or PostgreSQL")]
    DB --> DET["anomaly_detector.py"]
    DET --> Z["Rolling Z-score"]
    DET --> IF["Isolation Forest"]
    Z --> DB
    IF --> DB
    DB --> UI["dashboard.py<br/>Streamlit + Plotly"]
    UI --> HEALTH["Pipeline health"]
    UI --> CHARTS["Interactive KPI trends<br/>with anomaly markers"]
    UI --> AUDIT["Filterable SQL audit logs"]
```

## Features

- Retrieves daily OHLCV data for configurable tickers using `yfinance`.
- Checks required source columns, removes invalid null rows, and deduplicates
  trading dates.
- Calculates daily returns, rolling 7/30-observation close averages, volume
  averages, and return volatility.
- Upserts observations by `(ticker, date)` so repeat runs do not duplicate data.
- Scores price-return and log-volume behavior with a prior-observation rolling
  Z-score and a per-ticker Isolation Forest.
- Stores anomaly flags on the daily records and writes flagged events to an
  audit table.
- Tracks pipeline run status and displays it alongside KPI charts and audit
  tables in Streamlit.

## Project files

| File | Purpose |
| --- | --- |
| `etl_pipeline.py` | Extraction, validation, KPI engineering, database schema, and loading |
| `anomaly_detector.py` | Z-score and Isolation Forest scoring and anomaly audit writes |
| `dashboard.py` | Streamlit executive dashboard and SQL audit views |
| `requirements.txt` | Python runtime dependencies |
| `Dockerfile` | Container image for the Streamlit dashboard |

## Requirements

- Python 3.10+ (Docker image uses Python 3.12)
- Internet access for the Yahoo Finance data request
- SQLite by default, or PostgreSQL plus a reachable database server

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Configure optional settings through environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite:///./data/market_data.db` | SQLAlchemy database URL |
| `TICKERS` | `SPY,QQQ,^GSPC` | Comma-separated Yahoo Finance symbols |
| `HISTORY_PERIOD` | `2y` | yfinance lookback period for each ETL run |
| `LOG_LEVEL` | `INFO` | Python logging level |

For PostgreSQL, install dependencies from `requirements.txt` and set a URL such
as:

```bash
export DATABASE_URL='postgresql+psycopg://user:password@localhost:5432/market'
```

Avoid committing credentials. Use your deployment platform's secret manager or
environment configuration.

## Run the pipeline

Run extraction first, then anomaly detection:

```bash
python etl_pipeline.py
python anomaly_detector.py
```

The ETL refreshes the configured history window and upserts observations.
Refreshing a lookback window also recalculates rolling metrics with sufficient
context at the beginning of that window.

A nonzero ETL exit indicates a complete failure. If some tickers fail but at
least one ticker loads successfully, the run is recorded as
`partial_failure`; inspect the `pipeline_runs.message` field for details.

## Start the dashboard

```bash
streamlit run dashboard.py
```

Open the URL printed by Streamlit, typically `http://localhost:8501`. The
dashboard refreshes cached database reads every 30 seconds. Run the ETL and
detector again to update the displayed data.

## Docker

Build and start the dashboard container with a persistent SQLite data volume:

```bash
docker build -t market-pipeline .
docker run --rm -p 8501:8501 \
  -v "$PWD/data:/app/data" \
  market-pipeline
```

Run the ETL and detector in the same image, mounting the same database volume:

```bash
docker run --rm \
  -v "$PWD/data:/app/data" \
  -e TICKERS="SPY,QQQ,^GSPC" \
  market-pipeline python etl_pipeline.py

docker run --rm \
  -v "$PWD/data:/app/data" \
  market-pipeline python anomaly_detector.py
```

For PostgreSQL, provide the same `DATABASE_URL` to the dashboard, ETL, and
detector containers. Ensure that the database host is reachable from the
container (for a local host database, `localhost` inside the container is not
the host machine).

## Database tables

- **`daily_prices`** — one row per ticker and date, with OHLCV, rolling KPIs,
  model scores, and anomaly flags.
- **`pipeline_runs`** — one record per ETL attempt, including status, timestamps,
  row count, tickers, and error messages.
- **`anomaly_audit`** — append-only records for observations flagged by either
  detector method.

The detector's Isolation Forest is fit independently per ticker and needs at
least 20 usable observations by default. Z-score thresholds use the preceding
30 observations where available and require at least 7 prior values for a
baseline. These are configurable using the `run_detector` function arguments.

## Operational considerations

- For scheduled operation, run ETL and anomaly detection as separate scheduled
  jobs; run the dashboard as a long-lived service.
- Monitor `pipeline_runs.status` and `pipeline_runs.message` for failed or
  partial runs. Yahoo Finance requests can fail or be rate-limited.
- The anomaly audit records each flagged observation each time the detector
  runs. Add a uniqueness policy or deduplication if the audit table must contain
  only first-time detections.
- Tune thresholds and model contamination using representative data and review
  false positives before treating flags as actionable.
- For higher-volume or business-critical workloads, consider adding retries,
  provider-specific rate-limit handling, automated tests, schema migrations,
  alerting, and a managed database.