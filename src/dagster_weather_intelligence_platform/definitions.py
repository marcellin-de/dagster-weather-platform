from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from dagster_weather_intelligence_platform.assets.ml.predict_next_7d import forecast_temp_next_7d
from dagster_weather_intelligence_platform.assets.ml.train_forecast_model import (
    train_temp_forecast_model,
)
from dagster_weather_intelligence_platform.assets.weather_enriched import weather_daily_enriched
from dagster_weather_intelligence_platform.checks import (
    enriched_labels_quality_gate,
    ge_raw_hourly_basic_validations,
    ge_raw_hourly_temperature_validations,
    model_mae_threshold,
)
from dagster_weather_intelligence_platform.orchestration import (
    trigger_training_after_ingestion_success,
    weather_ingestion_hourly_job,
    weather_ingestion_hourly_schedule,
    weather_model_training_daily_schedule,
    weather_model_training_job,
)
from dagster_weather_intelligence_platform.resources import (
    GreatExpectationsResource,
    mlflow_resource,
)


def build_extra_defs() -> Definitions:
    return Definitions(
        assets=[weather_daily_enriched, train_temp_forecast_model, forecast_temp_next_7d],
        asset_checks=[
            ge_raw_hourly_basic_validations,
            ge_raw_hourly_temperature_validations,
            enriched_labels_quality_gate,
            model_mae_threshold,
        ],
        jobs=[
            weather_ingestion_hourly_job,
            weather_model_training_job,
        ],
        schedules=[
            weather_ingestion_hourly_schedule,
            weather_model_training_daily_schedule,
        ],
        sensors=[trigger_training_after_ingestion_success],
        resources={
            "ge": GreatExpectationsResource(),
            "mlflow": mlflow_resource,
        },
    )


@definitions
def defs() -> Definitions:
    component_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    extra_defs = build_extra_defs()
    return Definitions.merge(component_defs, extra_defs)
