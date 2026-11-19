"""Fetch, validate, engineer, and persist daily market data."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

LOG = logging.getLogger("etl_pipeline")

metadata = MetaData()

daily_prices = Table(
    "daily_prices",
    metadata,
    Column("date", Date, nullable=False),
    Column("ticker", String(32), nullable=False),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    Column("adj_close", Float),
    Column("volume", Float),
    Column("return_1d", Float),
    Column("sma_7", Float),
    Column("sma_30", Float),
    Column("volume_avg_7", Float),
    Column("volume_avg_30", Float),
    Column("return_volatility_7", Float),
    Column("return_volatility_30", Float),
    Column("z_score", Float),
    Column("isolation_score", Float),
    Column("anomaly_z", Boolean, nullable=False, default=False),
    Column("anomaly_isolation", Boolean, nullable=False, default=False),
    Column("is_anomaly", Boolean, nullable=False, default=False),
    UniqueConstraint("ticker", "date", name="uq_daily_prices_ticker_date"),
)

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(20), nullable=False),
    Column("tickers", String(1000), nullable=False),
    Column("rows_loaded", Integer, nullable=False, default=0),
    Column("message", String(4000)),
)

anomaly_audit = Table(
    "anomaly_audit",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("ticker", String(32), nullable=False),
    Column("date", Date, nullable=False),
    Column("close", Float),
    Column("volume", Float),
    Column("z_score", Float),
    Column("isolation_score", Float),
    Column("methods", String(100)),
    Column("details", String(1000)),
)


@dataclass(frozen=True)
class Settings:
    database_url: str
    tickers: tuple[str, ...]
    history_period: str

    @classmethod
    def from_env(cls) -> "Settings":
        tickers = tuple(
            ticker.strip().upper()
            for ticker in os.getenv("TICKERS", "SPY,QQQ,^GSPC").split(",")
            if ticker.strip()
        )
        return cls(
            database_url=os.getenv("DATABASE_URL", "sqlite:///./data/market_data.db"),
            tickers=tickers,
            history_period=os.getenv("HISTORY_PERIOD", "2y"),
        )


def get_engine(database_url: str) -> Engine:
    """Create an SQLAlchemy engine, ensuring the default SQLite directory exists."""
    if database_url.startswith("sqlite:///"):
        db_path = database_url.removeprefix("sqlite:///")
        if db_path != ":memory:" and not db_path.startswith("/"):
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    return create_engine(database_url, pool_pre_ping=True, future=True)


def initialize_database(engine: Engine) -> None:
    metadata.create_all(engine)


def _upsert_rows(engine: Engine, table: Table, rows: list[dict[str, Any]]) -> None:
    """Insert rows or update existing rows using the ticker/date unique key."""
    if not rows:
        return

    dialect = engine.dialect.name
    if dialect == "sqlite":
        statement = sqlite_insert(table).values(rows)
    elif dialect == "postgresql":
        statement = pg_insert(table).values(rows)
    else:
        raise ValueError(f"Unsupported database dialect: {dialect}")

    update_values = {
        column.name: getattr(statement.excluded, column.name)
        for column in table.columns
        if column.name not in {"ticker", "date"}
    }
    statement = statement.on_conflict_do_update(
        index_elements=["ticker", "date"],
        set_=update_values,
    )
    with engine.begin() as connection:
        connection.execute(statement)


def _to_python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def validate_and_engineer(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Verify source schema, clean observations, and calculate rolling KPIs."""
    required = {"Open", "High", "Low", "Close", "Volume"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"{ticker}: missing required source columns: {sorted(missing)}")

    if raw.empty:
        raise ValueError(f"{ticker}: source returned no observations")

    frame = raw.copy()
    frame.index = pd.to_datetime(frame.index, utc=True).date
    frame.index.name = "date"

    # Adjusted close is optional in some yfinance responses.
    frame["Adj Close"] = frame["Adj Close"] if "Adj Close" in frame else frame["Close"]
    frame = frame.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    expected = ["open", "high", "low", "close", "adj_close", "volume"]
    frame = frame[expected]

    for column in expected:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    # OHLCV fields are required for valid daily observations.
    frame = frame.dropna(subset=["open", "high", "low", "close", "volume"])
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    if frame.empty:
        raise ValueError(f"{ticker}: no valid observations after null and duplicate checks")

    frame["return_1d"] = frame["close"].pct_change()
    for window in (7, 30):
        frame[f"sma_{window}"] = frame["close"].rolling(window, min_periods=1).mean()
        frame[f"volume_avg_{window}"] = (
            frame["volume"].rolling(window, min_periods=1).mean()
        )
        frame[f"return_volatility_{window}"] = (
            frame["return_1d"].rolling(window, min_periods=2).std()
        )

    frame["ticker"] = ticker
    frame["z_score"] = None
    frame["isolation_score"] = None
    frame["anomaly_z"] = False
    frame["anomaly_isolation"] = False
    frame["is_anomaly"] = False
    frame = frame.reset_index()

    return frame


def fetch_ticker(ticker: str, history_period: str) -> pd.DataFrame:
    """Fetch daily history from Yahoo Finance."""
    return yf.Ticker(ticker).history(
        period=history_period,
        interval="1d",
        auto_adjust=False,
        actions=False,
    )


def run_pipeline(
    settings: Settings | None = None,
    engine: Engine | None = None,
) -> int:
    """Run extraction, validation, KPI engineering, and database loading."""
    settings = settings or Settings.from_env()
    engine = engine or get_engine(settings.database_url)
    initialize_database(engine)

    started = datetime.now(timezone.utc)
    rows_loaded = 0
    status = "success"
    messages: list[str] = []
    tickers_text = ",".join(settings.tickers)

    try:
        for ticker in settings.tickers:
            try:
                raw = fetch_ticker(ticker, settings.history_period)
                clean = validate_and_engineer(raw, ticker)
                rows = [
                    {
                        key: _to_python_value(value)
                        for key, value in row.items()
                    }
                    for row in clean.to_dict(orient="records")
                ]
                _upsert_rows(engine, daily_prices, rows)
                rows_loaded += len(rows)
                LOG.info("%s: loaded %d validated rows", ticker, len(rows))
            except Exception as exc:
                status = "partial_failure"
                message = f"{ticker}: {type(exc).__name__}: {exc}"
                messages.append(message)
                LOG.exception("Failed processing %s", ticker)

        if not rows_loaded:
            status = "failure"
    except Exception as exc:
        status = "failure"
        messages.append(f"{type(exc).__name__}: {exc}")
        LOG.exception("Pipeline failed")

    finished = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            pipeline_runs.insert().values(
                started_at=started,
                finished_at=finished,
                status=status,
                tickers=tickers_text,
                rows_loaded=rows_loaded,
                message="; ".join(messages)[:4000] or None,
            )
        )

    LOG.info(
        "Pipeline finished: status=%s rows_loaded=%d tickers=%s",
        status,
        rows_loaded,
        tickers_text,
    )
    if status == "failure":
        raise RuntimeError("; ".join(messages) or "No data was loaded")
    return rows_loaded


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_pipeline()