from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import duckdb
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AutomationCondition,
    MetadataValue,
    asset_check,
)

from dagster_weather_intelligence_platform.resources import GreatExpectationsResource


ASSET_KEY = ("raw_weather", "open_meteo_hourly")
DUCKDB_PATH_ENV_VAR = "WEATHER_DUCKDB_PATH"
DEFAULT_DUCKDB_PATH = Path(__file__).resolve().parents[2] / "weather_ingest.duckdb"


def _resolve_duckdb_path() -> Path:
    override = os.getenv(DUCKDB_PATH_ENV_VAR)
    return Path(override).expanduser() if override else DEFAULT_DUCKDB_PATH


def _read_raw_hourly_from_duckdb(db_path: Path | None = None) -> pd.DataFrame:
    path = db_path or _resolve_duckdb_path()
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(
            """
            select
              ts_utc,
              latitude,
              longitude,
              temperature_2m,
              relative_humidity_2m,
              precipitation,
              wind_speed_10m
            from raw_weather.open_meteo_hourly
            """
        ).df()
    finally:
        con.close()


def _result_from_expectations(
    context,
    ge: GreatExpectationsResource,
    expectation_factories: dict[str, Callable],
) -> AssetCheckResult:
    try:
        df = _read_raw_hourly_from_duckdb()
    except Exception as exc:
        context.log.exception("Failed to read raw_weather.open_meteo_hourly from DuckDB")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "error": MetadataValue.text(str(exc)),
                "db_path": MetadataValue.path(str(_resolve_duckdb_path())),
            },
        )

    validator = ge.get_validator(df)
    results = {name: factory(validator) for name, factory in expectation_factories.items()}
    passed = all(bool(result.success) for result in results.values())

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            **{name: result.result for name, result in results.items()},
            "row_count": len(df),
        },
    )


@asset_check(asset=ASSET_KEY, blocking=True, automation_condition=AutomationCondition.eager())
def ge_raw_hourly_basic_validations(
    context,
    ge: GreatExpectationsResource,
) -> AssetCheckResult:
    return _result_from_expectations(
        context=context,
        ge=ge,
        expectation_factories={
            "ts_utc_not_null": lambda validator: validator.expect_column_values_to_not_be_null(
                "ts_utc"
            ),
            "temperature_not_null": lambda validator: validator.expect_column_values_to_not_be_null(
                "temperature_2m"
            ),
            "temperature_range": lambda validator: validator.expect_column_values_to_be_between(
                "temperature_2m",
                min_value=-50,
                max_value=60,
                mostly=0.99,
            ),
        },
    )


@asset_check(asset=ASSET_KEY, blocking=True, automation_condition=AutomationCondition.eager())
def ge_raw_hourly_temperature_validations(
    context,
    ge: GreatExpectationsResource,
) -> AssetCheckResult:
    return _result_from_expectations(
        context=context,
        ge=ge,
        expectation_factories={
            "temperature_not_null": lambda validator: validator.expect_column_values_to_not_be_null(
                "temperature_2m"
            ),
        },
    )
