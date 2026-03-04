import unittest

from dagster_weather_intelligence_platform.defs.weather_duckdb_ingest.loads import (
    _extract_hourly_records,
)


class WeatherIngestUtilsTestCase(unittest.TestCase):
    def test_extract_hourly_records_handles_missing_series_values(self) -> None:
        hourly_payload = {
            "time": ["2026-03-04T00:00", "2026-03-04T01:00"],
            "temperature_2m": [20.1],
            "relative_humidity_2m": [50, 51],
            "precipitation": [0.0, 0.0],
            "wind_speed_10m": [7.5, 8.0],
        }

        rows = list(_extract_hourly_records(hourly_payload, latitude=36.8, longitude=10.1))

        self.assertEqual(2, len(rows))
        self.assertEqual("2026-03-04T00:00", rows[0]["ts_utc"])
        self.assertEqual(20.1, rows[0]["temperature_2m"])
        self.assertIsNone(rows[1]["temperature_2m"])
        self.assertEqual(51, rows[1]["relative_humidity_2m"])


if __name__ == "__main__":
    unittest.main()
