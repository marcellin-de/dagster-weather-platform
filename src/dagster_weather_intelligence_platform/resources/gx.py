from __future__ import annotations

from typing import Any

import great_expectations as gx
import pandas as pd
from dagster import ConfigurableResource
from great_expectations import ExpectationSuite
from great_expectations.exceptions import DataContextError


class GreatExpectationsResource(ConfigurableResource):
    """
    GX validator factory using an ephemeral Data Context, as recommended for a light setup.
    We rely on Dagster metadata logging for observability.
    """

    suite_name: str = "raw_weather_suite"
    datasource_name: str = "pandas_datasource"
    data_asset_name: str = "asset_check_df"

    def _get_or_create_datasource(self, context: Any) -> Any:
        try:
            return context.data_sources.get(self.datasource_name)
        except KeyError:
            return context.data_sources.add_pandas(name=self.datasource_name)

    def _get_or_create_data_asset(self, datasource: Any) -> Any:
        try:
            return datasource.get_asset(self.data_asset_name)
        except LookupError:
            return datasource.add_dataframe_asset(name=self.data_asset_name)

    def _get_or_create_suite(self, context: Any) -> None:
        try:
            context.suites.get(self.suite_name)
        except DataContextError:
            context.suites.add(ExpectationSuite(name=self.suite_name))

    def get_validator(self, df: pd.DataFrame) -> Any:
        context = gx.get_context()
        datasource = self._get_or_create_datasource(context)
        data_asset = self._get_or_create_data_asset(datasource)
        self._get_or_create_suite(context)

        batch_request = data_asset.build_batch_request(options={"dataframe": df})
        return context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=self.suite_name,
        )
