import os
from pathlib import Path

import duckdb
import mlflow
import numpy as np
import pandas as pd
from dagster import AssetKey, asset
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


def _log_mlflow(context, payload: dict, model: Ridge | None = None) -> None:
    try:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
        experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "weather-forecasting")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        nested = mlflow.active_run() is not None
        with mlflow.start_run(run_name="train_temp_forecast_model", nested=nested):
            mlflow.log_param("strategy", payload.get("strategy"))
            mlflow.log_param("trained", bool(payload.get("trained", False)))
            mlflow.log_param("required_days", int(payload.get("required_days", 5)))
            mlflow.log_metric("available_days", int(payload.get("available_days", 0)))

            mae = payload.get("mae")
            if mae is not None:
                mlflow.log_metric("mae", float(mae))

            if payload.get("trained", False):
                mlflow.log_param("model", "Ridge")
                mlflow.log_param("alpha", 1.0)
                mlflow.log_param("horizon_days", int(payload.get("horizon_days", 1)))
                if model is not None:
                    mlflow.sklearn.log_model(model, artifact_path="model")
            else:
                mlflow.log_metric("fallback_temp", float(payload.get("baseline_temp", 0.0)))
    except Exception as exc:
        # MLflow observability must not block data pipeline execution.
        context.log.warning("MLflow logging skipped: %s", exc)


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
    df["avg_temp_2m"] = df["avg_temp_2m"].astype(float)
    return df


def _make_supervised(df: pd.DataFrame, horizon_days: int = 1) -> tuple[np.ndarray, np.ndarray]:
    # X = index time, y = shifted temperature
    y = df["avg_temp_2m"].shift(-horizon_days).dropna().to_numpy()
    x = np.arange(len(y)).reshape(-1, 1)
    return x, y


@asset(group_name="ml", deps=[AssetKey("mart_weather_daily")])
def train_temp_forecast_model(context) -> dict:
    df = _load_daily_series()
    required_days = int(os.getenv("MIN_ML_TRAIN_DAYS", "5"))
    available_days = int(len(df))
    horizon = 1

    if available_days == 0:
        fallback_temp = 20.0
    elif available_days < 3:
        fallback_temp = float(df["avg_temp_2m"].iloc[-1])
    else:
        fallback_temp = float(df["avg_temp_2m"].tail(3).mean())

    if available_days < required_days:
        context.log.warning(
            "Insufficient history for model training (%s/%s days). "
            "Using fallback strategy.",
            available_days,
            required_days,
        )
        context.add_output_metadata(
            {
                "status": "fallback",
                "strategy": "naive_recent_average",
                "available_days": available_days,
                "required_days": required_days,
                "fallback_temp": fallback_temp,
            }
        )
        payload = {
            "trained": False,
            "strategy": "naive_recent_average",
            "available_days": available_days,
            "required_days": required_days,
            "baseline_temp": fallback_temp,
            "mae": None,
            "trained_points": 0,
            "test_points": 0,
            "coef": 0.0,
            "intercept": fallback_temp,
            "horizon_days": horizon,
        }
        _log_mlflow(context, payload=payload, model=None)
        return payload

    X, y = _make_supervised(df, horizon_days=horizon)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    model = Ridge(alpha=1.0)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = float(mean_absolute_error(y_test, preds))

    context.add_output_metadata(
        {"status": "trained", "rows": len(df), "mae": mae, "strategy": "ridge_linear"}
    )

    payload = {
        "trained": True,
        "strategy": "ridge_linear",
        "available_days": available_days,
        "required_days": required_days,
        "mae": mae,
        "trained_points": int(len(X_train)),
        "test_points": int(len(X_test)),
        "coef": float(model.coef_[0]),
        "intercept": float(model.intercept_),
        "horizon_days": horizon,
    }
    _log_mlflow(context, payload=payload, model=model)
    return payload
