import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from dagster import asset

OUT_TABLE = "analytics.weather_forecast_7d"


def _resolve_duckdb_path() -> str:
    if os.getenv("WEATHER_DUCKDB_PATH"):
        return os.environ["WEATHER_DUCKDB_PATH"]
    if os.getenv("WEATHER_DBT_DUCKDB_PATH"):
        return os.environ["WEATHER_DBT_DUCKDB_PATH"]
    project_root = os.getenv("DAGSTER_PROJECT_ROOT")
    if project_root:
        return str(Path(project_root) / "src" / "weather_ingest.duckdb")
    return str(Path(__file__).resolve().parents[4] / "src" / "weather_ingest.duckdb")


def _load_daily_series() -> pd.DataFrame:
    con = duckdb.connect(_resolve_duckdb_path(), read_only=True)
    try:
        df = con.execute(
            """
            select
              day_utc,
              avg_temp_2m
            from analytics.mart_weather_daily
            where avg_temp_2m is not null
            order by day_utc asc
            """
        ).df()
    finally:
        con.close()
    df["day_utc"] = pd.to_datetime(df["day_utc"])
    return df


@asset(group_name="ml")
def forecast_temp_next_7d(context, train_temp_forecast_model: dict) -> None:
    df = _load_daily_series()
    if df.empty:
        raise ValueError("Cannot forecast without data in analytics.mart_weather_daily.")

    last_day = df["day_utc"].max()

    trained = bool(train_temp_forecast_model.get("trained", False))
    coef = float(train_temp_forecast_model.get("coef", 0.0))
    intercept = float(train_temp_forecast_model.get("intercept", 0.0))
    strategy = str(train_temp_forecast_model.get("strategy", "ridge_linear"))

    start_idx = len(df)
    X_future = np.arange(start_idx, start_idx + 7).reshape(-1, 1)
    if trained:
        y_future = coef * X_future.flatten() + intercept
    else:
        baseline_temp = float(train_temp_forecast_model.get("baseline_temp", intercept))
        y_future = np.full(7, baseline_temp, dtype=float)

    out = pd.DataFrame(
        {
            "forecast_day_utc": [last_day + pd.Timedelta(days=i) for i in range(1, 8)],
            "pred_avg_temp_2m": y_future.astype(float),
        }
    )

    con = duckdb.connect(_resolve_duckdb_path())
    try:
        con.execute("create schema if not exists analytics;")
        con.register("tmp_forecast", out)
        con.execute(
            f"""
            create or replace table {OUT_TABLE} as
            select * from tmp_forecast
            """
        )
    finally:
        try:
            con.unregister("tmp_forecast")
        except Exception:
            pass
        con.close()

    context.add_output_metadata(
        {
            "table": OUT_TABLE,
            "rows_written": len(out),
            "train_mae": (
                float(train_temp_forecast_model["mae"])
                if train_temp_forecast_model.get("mae") is not None
                else None
            ),
            "model_type": strategy,
            "trained": trained,
        }
    )
