"""Detect unusual daily returns and trading volume with two complementary methods."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sqlalchemy import select, update

from etl_pipeline import (
    anomaly_audit,
    daily_prices,
    get_engine,
    initialize_database,
)

LOG = logging.getLogger("anomaly_detector")


def detect_anomalies(
    frame: pd.DataFrame,
    z_threshold: float = 3.0,
    contamination: float = 0.02,
    min_samples: int = 20,
) -> pd.DataFrame:
    """Return records with rolling Z-score and Isolation Forest anomaly flags.

    Z-scores use preceding observations only, so the current value cannot
    materially shift its own baseline. Price behavior is represented by daily
    return; volume is log-transformed to reduce skew.
    """
    if frame.empty:
        return frame.assign(
            z_score=pd.Series(dtype=float),
            isolation_score=pd.Series(dtype=float),
            anomaly_z=pd.Series(dtype=bool),
            anomaly_isolation=pd.Series(dtype=bool),
            is_anomaly=pd.Series(dtype=bool),
        )

    result = frame.sort_values(["ticker", "date"]).copy()
    result["date"] = pd.to_datetime(result["date"])
    result["log_volume"] = np.log1p(result["volume"].clip(lower=0))

    z_values = pd.Series(0.0, index=result.index)
    z_flags = pd.Series(False, index=result.index)

    for _, group in result.groupby("ticker", sort=False):
        for feature in ("return_1d", "log_volume"):
            values = pd.to_numeric(group[feature], errors="coerce")
            baseline = values.shift(1).rolling(30, min_periods=7)
            mean = baseline.mean()
            std = baseline.std().replace(0, np.nan)
            z = ((values - mean) / std).replace([np.inf, -np.inf], np.nan)
            z_values.loc[group.index] = np.maximum(
                z_values.loc[group.index].to_numpy(),
                z.abs().fillna(0).to_numpy(),
            )
            z_flags.loc[group.index] |= z.abs().fillna(0) >= z_threshold

    result["z_score"] = z_values
    result["anomaly_z"] = z_flags
    result["isolation_score"] = np.nan
    result["anomaly_isolation"] = False

    features = ["return_1d", "log_volume"]
    for _, group in result.groupby("ticker", sort=False):
        usable = group[features].replace([np.inf, -np.inf], np.nan).dropna()
        if len(usable) < min_samples:
            continue
        model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
            n_jobs=-1,
        )
        labels = model.fit_predict(usable)
        scores = model.decision_function(usable)
        result.loc[usable.index, "anomaly_isolation"] = labels == -1
        # Lower decision_function values are more anomalous.
        result.loc[usable.index, "isolation_score"] = scores

    result["is_anomaly"] = result["anomaly_z"] | result["anomaly_isolation"]
    return result.drop(columns=["log_volume"])


def run_detector(
    database_url: str | None = None,
    z_threshold: float = 3.0,
    contamination: float = 0.02,
    min_samples: int = 20,
) -> int:
    """Score stored observations, update flags, and append anomaly audit rows."""
    engine = get_engine(
        database_url or os.getenv("DATABASE_URL", "sqlite:///./data/market_data.db")
    )
    initialize_database(engine)

    with engine.connect() as connection:
        frame = pd.read_sql(
            select(
                daily_prices.c.date,
                daily_prices.c.ticker,
                daily_prices.c.close,
                daily_prices.c.volume,
                daily_prices.c.return_1d,
            ).order_by(daily_prices.c.ticker, daily_prices.c.date),
            connection,
        )

    if frame.empty:
        LOG.warning("No daily prices found; run etl_pipeline.py first")
        return 0

    scored = detect_anomalies(frame, z_threshold, contamination, min_samples)
    now = datetime.now(timezone.utc)
    audit_rows = []

    with engine.begin() as connection:
        for row in scored.to_dict(orient="records"):
            date_value = pd.Timestamp(row["date"]).date()
            connection.execute(
                update(daily_prices)
                .where(
                    daily_prices.c.ticker == row["ticker"],
                    daily_prices.c.date == date_value,
                )
                .values(
                    z_score=float(row["z_score"]) if pd.notna(row["z_score"]) else None,
                    isolation_score=(
                        float(row["isolation_score"])
                        if pd.notna(row["isolation_score"])
                        else None
                    ),
                    anomaly_z=bool(row["anomaly_z"]),
                    anomaly_isolation=bool(row["anomaly_isolation"]),
                    is_anomaly=bool(row["is_anomaly"]),
                )
            )
            if row["is_anomaly"]:
                methods = []
                if row["anomaly_z"]:
                    methods.append("z_score")
                if row["anomaly_isolation"]:
                    methods.append("isolation_forest")
                audit_rows.append(
                    {
                        "created_at": now,
                        "ticker": row["ticker"],
                        "date": date_value,
                        "close": float(row["close"]),
                        "volume": float(row["volume"]),
                        "z_score": (
                            float(row["z_score"]) if pd.notna(row["z_score"]) else None
                        ),
                        "isolation_score": (
                            float(row["isolation_score"])
                            if pd.notna(row["isolation_score"])
                            else None
                        ),
                        "methods": ",".join(methods),
                        "details": "Anomaly flagged by configured statistical/model threshold",
                    }
                )

        if audit_rows:
            connection.execute(anomaly_audit.insert(), audit_rows)

    LOG.info("Scored %d rows; flagged %d anomalies", len(scored), len(audit_rows))
    return len(audit_rows)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_detector()