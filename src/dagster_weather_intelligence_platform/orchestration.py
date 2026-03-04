from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    build_schedule_from_partitioned_job,
    define_asset_job,
)


WEATHER_SOURCE_ASSET_KEY = "raw_weather/open_meteo_hourly"
WEATHER_PIPELINE_SELECTION = AssetSelection.assets(WEATHER_SOURCE_ASSET_KEY).downstream(
    include_self=True
)
WEATHER_SELECTION = WEATHER_PIPELINE_SELECTION | AssetSelection.checks_for_assets(
    WEATHER_SOURCE_ASSET_KEY
)

weather_daily_materialization_job = define_asset_job(
    name="weather_daily_materialization_job",
    selection=WEATHER_SELECTION,
    description="Materialize ingestion + dbt weather assets and run checks for the daily partition.",
)

weather_daily_schedule = build_schedule_from_partitioned_job(
    job=weather_daily_materialization_job,
    name="weather_daily_schedule",
    default_status=DefaultScheduleStatus.STOPPED,
)
