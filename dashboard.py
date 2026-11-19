"""Streamlit executive dashboard for ETL status, KPIs, and anomaly review."""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import select

from etl_pipeline import (
    anomaly_audit,
    daily_prices,
    get_engine,
    initialize_database,
    pipeline_runs,
)

st.set_page_config(page_title="Market Data Pipeline", layout="wide")
st.title("Market Data ETL & Anomaly Monitoring")

engine = get_engine(os.getenv("DATABASE_URL", "sqlite:///./data/market_data.db"))
initialize_database(engine)


@st.cache_data(ttl=30)
def load_table(table, order_by, limit: int = 10000) -> pd.DataFrame:
    with engine.connect() as connection:
        return pd.read_sql(select(table).order_by(order_by).limit(limit), connection)


try:
    prices = load_table(
        daily_prices,
        (daily_prices.c.ticker, daily_prices.c.date),
    )
    runs = load_table(pipeline_runs, pipeline_runs.c.started_at.desc(), 200)
    audit = load_table(anomaly_audit, anomaly_audit.c.created_at.desc(), 2000)
except Exception as exc:
    st.error(f"Could not read the database: {exc}")
    st.stop()

latest = runs.iloc[0] if not runs.empty else None
status = latest["status"] if latest is not None else "not run"
status_color = {
    "success": "🟢",
    "partial_failure": "🟠",
    "failure": "🔴",
    "not run": "⚪",
}.get(status, "⚪")

health_col, run_col, loaded_col, anomaly_col = st.columns(4)
health_col.metric("Pipeline health", f"{status_color} {status}")
run_col.metric(
    "Last run (UTC)",
    str(latest["started_at"]) if latest is not None else "No runs recorded",
)
loaded_col.metric(
    "Rows loaded",
    int(latest["rows_loaded"]) if latest is not None else 0,
)
anomaly_col.metric(
    "Flagged observations",
    int(prices["is_anomaly"].fillna(False).sum()) if not prices.empty else 0,
)

if latest is not None and pd.notna(latest.get("message")):
    st.warning(str(latest["message"]))

if prices.empty:
    st.info("No market data is available yet. Run `python etl_pipeline.py`.")
    st.stop()

prices["date"] = pd.to_datetime(prices["date"])
tickers = sorted(prices["ticker"].dropna().unique().tolist())
selected = st.multiselect("Tickers", tickers, default=tickers)
date_min = prices["date"].min().date()
date_max = prices["date"].max().date()
date_range = st.date_input(
    "Date range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

filtered = prices[
    prices["ticker"].isin(selected)
    & prices["date"].dt.date.between(start_date, end_date)
].copy()

if filtered.empty:
    st.info("No observations match the selected filters.")
else:
    metric = st.selectbox("Trend", ["close", "volume", "sma_7", "sma_30"])
    chart = px.line(
        filtered,
        x="date",
        y=metric,
        color="ticker",
        title=f"{metric.replace('_', ' ').title()} over time",
        hover_data=["close", "volume", "return_1d"],
    )
    flagged = filtered[filtered["is_anomaly"].fillna(False)]
    if not flagged.empty:
        chart.add_scatter(
            x=flagged["date"],
            y=flagged[metric],
            mode="markers",
            name="Anomaly",
            marker={"color": "red", "size": 11, "symbol": "x"},
            customdata=flagged[["ticker", "z_score", "isolation_score"]],
            hovertemplate=(
                "Ticker: %{customdata[0]}<br>Date: %{x}<br>Value: %{y}"
                "<br>Z-score: %{customdata[1]}<br>Isolation score: %{customdata[2]}"
                "<extra></extra>"
            ),
        )
    chart.update_layout(legend_title_text="Ticker", hovermode="x unified")
    st.plotly_chart(chart, use_container_width=True)

st.subheader("SQL audit log — pipeline runs")
if runs.empty:
    st.info("No pipeline runs have been recorded.")
else:
    statuses = sorted(runs["status"].dropna().unique().tolist())
    status_filter = st.multiselect("Filter audit log by status", statuses, default=statuses)
    run_view = runs[runs["status"].isin(status_filter)]
    st.dataframe(
        run_view[
            ["id", "started_at", "finished_at", "status", "tickers", "rows_loaded", "message"]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Anomaly audit log")
if audit.empty:
    st.info("No anomaly events have been recorded.")
else:
    anomaly_tickers = sorted(audit["ticker"].dropna().unique().tolist())
    audit_tickers = st.multiselect(
        "Filter anomaly log by ticker", anomaly_tickers, default=anomaly_tickers
    )
    audit_view = audit[audit["ticker"].isin(audit_tickers)]
    st.dataframe(
        audit_view[
            [
                "created_at",
                "ticker",
                "date",
                "close",
                "volume",
                "z_score",
                "isolation_score",
                "methods",
                "details",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )