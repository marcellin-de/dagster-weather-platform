import duckdb
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_weather_intelligence_platform.utils import resolve_duckdb_path

TABLE_FQN = "analytics.weather_daily_enriched"
ALLOWED = {"clear", "cloudy", "rainy", "windy"}


def _fetch_stats(db_path: str) -> tuple[int, int, int]:
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
    db_path = resolve_duckdb_path()
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
