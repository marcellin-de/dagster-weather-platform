# weather_dbt

dbt project for weather transformations:

- `stg_open_meteo_hourly`: typed incremental table over `raw_weather.open_meteo_hourly`
- `mart_weather_daily`: daily aggregates for analytics

`stg_open_meteo_hourly` is incremental with:
- `unique_key: ts_utc`
- `incremental_strategy: delete+insert`
- a 3-day lookback window to update late/corrected values safely

## Run locally

```bash
uv run dbt parse
uv run dbt build
```

## Grouping

- dbt model group: `weather_analytics`
- Dagster ingestion group (raw source asset): `weather_ingestion`
