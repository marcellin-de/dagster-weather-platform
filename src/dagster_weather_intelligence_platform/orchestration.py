from datetime import UTC, datetime

import dagster as dg


WEATHER_SOURCE_ASSET_KEY = "raw_weather/open_meteo_hourly"

INGESTION_ASSET_KEYS = (
    WEATHER_SOURCE_ASSET_KEY,
    "stg_open_meteo_hourly",
    "mart_weather_daily",
    "weather_daily_enriched",
)
TRAINING_ASSET_KEYS = ("train_temp_forecast_model", "forecast_temp_next_7d")

INGESTION_SELECTION = (
    dg.AssetSelection.assets(*INGESTION_ASSET_KEYS)
    | dg.AssetSelection.checks_for_assets(*INGESTION_ASSET_KEYS)
)
TRAINING_SELECTION = (
    dg.AssetSelection.assets(*TRAINING_ASSET_KEYS)
    | dg.AssetSelection.checks_for_assets("train_temp_forecast_model")
)


weather_ingestion_hourly_job = dg.define_asset_job(
    name="weather_ingestion_hourly_job",
    selection=INGESTION_SELECTION,
    description="Ingest weather API data, run transforms and quality checks every hour.",
)

weather_model_training_job = dg.define_asset_job(
    name="weather_model_training_job",
    selection=TRAINING_SELECTION,
    description="Train and score weather forecast model assets.",
)

weather_ingestion_hourly_schedule = dg.ScheduleDefinition(
    name="weather_ingestion_hourly_schedule",
    job=weather_ingestion_hourly_job,
    cron_schedule="0 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

weather_model_training_daily_schedule = dg.ScheduleDefinition(
    name="weather_model_training_daily_schedule",
    job=weather_model_training_job,
    cron_schedule="15 2 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[weather_ingestion_hourly_job],
    request_job=weather_model_training_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def trigger_training_after_ingestion_success(context: dg.RunStatusSensorContext):
    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    if context.cursor == run_date:
        return dg.SkipReason("Training already requested for today.")

    context.update_cursor(run_date)
    return dg.RunRequest(
        run_key=f"training-{run_date}",
        tags={
            "trigger": "ingestion_success_sensor",
            "run_date": run_date,
        },
    )


# Backward compatibility aliases
weather_daily_materialization_job = weather_ingestion_hourly_job
weather_daily_schedule = weather_ingestion_hourly_schedule
