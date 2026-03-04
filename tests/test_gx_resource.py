import unittest
import warnings

import pandas as pd

from dagster_weather_intelligence_platform.resources import GreatExpectationsResource


class GreatExpectationsResourceTestCase(unittest.TestCase):
    def test_get_validator_supports_dataframe_expectations(self) -> None:
        resource = GreatExpectationsResource()
        df = pd.DataFrame(
            {
                "ts_utc": ["2026-03-04T00:00:00Z", "2026-03-04T01:00:00Z"],
                "temperature_2m": [15.2, 16.1],
            }
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            warnings.simplefilter("ignore", ResourceWarning)
            validator = resource.get_validator(df)
            result = validator.expect_column_values_to_not_be_null("temperature_2m")

        self.assertTrue(result.success)


if __name__ == "__main__":
    unittest.main()
