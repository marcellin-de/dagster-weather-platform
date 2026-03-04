from dataclasses import dataclass

import dagster as dg
from dagster._core.execution.context.asset_execution_context import AssetExecutionContext
from dagster_dlt.asset_decorator import dlt_assets
from dagster_dlt.components.dlt_load_collection.component import (
    DltComponentTranslator,
    DltLoadCollectionComponent,
)


WEATHER_DAILY_PARTITIONS_DEF = dg.DailyPartitionsDefinition(
    start_date="2026-01-01",
    timezone="UTC",
    hour_offset=6,
)


@dataclass
class PartitionedDltLoadCollectionComponent(DltLoadCollectionComponent):
    """Dlt component variant with daily Dagster partitions."""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        del context
        output = []

        for load in self.loads:
            translator = DltComponentTranslator(self, load)

            @dlt_assets(
                dlt_source=load.source,
                dlt_pipeline=load.pipeline,
                name=f"dlt_assets_{load.source.name}_{load.pipeline.dataset_name}",
                dagster_dlt_translator=translator,
                partitions_def=WEATHER_DAILY_PARTITIONS_DEF,
            )
            def dlt_assets_def(context: AssetExecutionContext):
                yield from self.execute(context, self.dlt_pipeline_resource)

            output.append(dlt_assets_def)

        return dg.Definitions(assets=output)
