import os
from pathlib import Path

import duckdb


def main() -> None:
    db_path = Path(os.getenv("WEATHER_DUCKDB_PATH", "/data/weather_ingest.duckdb"))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("create schema if not exists raw_weather;")
        con.execute("create schema if not exists analytics;")
        # Ensure downstream services (Evidence, ML assets) can start cleanly
        # before Dagster materializes real data.
        con.execute(
            """
            create table if not exists analytics.mart_weather_daily (
                day_utc timestamp,
                avg_temp_2m double,
                min_temp_2m double,
                max_temp_2m double,
                avg_wind_10m double,
                total_precipitation double
            );
            """
        )
        con.execute(
            """
            create table if not exists analytics.weather_forecast_7d (
                forecast_day_utc timestamp,
                pred_avg_temp_2m double
            );
            """
        )
        con.execute(
            """
            create table if not exists analytics.weather_daily_enriched (
                day_utc varchar,
                avg_temp_2m double,
                min_temp_2m double,
                max_temp_2m double,
                avg_wind_10m double,
                total_precipitation double,
                label varchar,
                summary varchar
            );
            """
        )
    finally:
        con.close()

    print(f"[duckdb] ready: {db_path}")


if __name__ == "__main__":
    main()
