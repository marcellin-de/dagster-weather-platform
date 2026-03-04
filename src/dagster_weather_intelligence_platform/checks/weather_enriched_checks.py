import duckdb
import os
from pathlib import Path

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

TABLE_FQN = "analytics.weather_daily_enriched"
ALLOWED = {"clear", "cloudy", "rainy", "windy"}


def _resolve_duckdb_path() -> str:
    if os.getenv("WEATHER_DUCKDB_PATH"):
        return os.environ["WEATHER_DUCKDB_PATH"]
    if os.getenv("WEATHER_DBT_DUCKDB_PATH"):
        return os.environ["WEATHER_DBT_DUCKDB_PATH"]
    project_root = os.getenv("DAGSTER_PROJECT_ROOT")
    if project_root:
        return str(Path(project_root) / "src" / "weather_ingest.duckdb")
    return str(Path(__file__).resolve().parents[3] / "src" / "weather_ingest.duckdb")


def _fetch_stats(db_path: str):
    con = duckdb.connect(db_path, read_only=True)
    try:
        total = con.execute(f"select count(*) from {TABLE_FQN}").fetchone()[0]
        null_labels = con.execute(
            f"select count(*) from {TABLE_FQN} where label is null"
        ).fetchone()[0]
        invalid_labels = con.execute(
            f"""
            select count(*)
            from {TABLE_FQN}
            where label is not null
              and label not in ('clear','cloudy','rainy','windy')
            """
        ).fetchone()[0]
        return int(total), int(null_labels), int(invalid_labels)
    finally:
        con.close()


@asset_check(asset="weather_daily_enriched", blocking=True)
def enriched_labels_quality_gate(context: AssetCheckExecutionContext) -> AssetCheckResult:
    db_path = _resolve_duckdb_path()
    total, null_labels, invalid_labels = _fetch_stats(db_path)

    null_rate = (null_labels / total) if total else 1.0
    passed = (total > 0) and (invalid_labels == 0) and (null_rate <= 0.01)

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "total_rows": total,
            "null_labels": null_labels,
            "null_rate": null_rate,
            "invalid_labels": invalid_labels,
            "allowed_labels": sorted(list(ALLOWED)),
            "table": TABLE_FQN,
            "db_path": db_path,
        },
    )
