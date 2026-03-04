from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder
from dagster_weather_intelligence_platform.checks import (
    ge_raw_hourly_basic_validations,
    ge_raw_hourly_temperature_validations,
)
from dagster_weather_intelligence_platform.orchestration import (
    weather_daily_materialization_job,
    weather_daily_schedule,
)
from dagster_weather_intelligence_platform.resources import GreatExpectationsResource
from dagster_weather_intelligence_platform.assets.weather_enriched import weather_daily_enriched


def build_extra_defs() -> Definitions:
    return Definitions(
        assets=[weather_daily_enriched],
        asset_checks=[
            ge_raw_hourly_basic_validations,
            ge_raw_hourly_temperature_validations,
        ],
        jobs=[weather_daily_materialization_job],
        schedules=[weather_daily_schedule],
        resources={
            "ge": GreatExpectationsResource(),
        },
    )


@definitions
def defs() -> Definitions:
    component_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)
    extra_defs = build_extra_defs()
    return Definitions.merge(component_defs, extra_defs)
