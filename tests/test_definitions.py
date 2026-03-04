import unittest

from dagster_weather_intelligence_platform.definitions import defs


class DefinitionsTestCase(unittest.TestCase):
    def test_definitions_include_checks_and_resources(self) -> None:
        definitions = defs()

        self.assertIn("ge", definitions.resources)

        check_names = {
            spec.name
            for check_def in (definitions.asset_checks or [])
            for spec in check_def.check_specs
        }
        self.assertEqual(
            {
                "ge_raw_hourly_basic_validations",
                "ge_raw_hourly_temperature_validations",
            },
            check_names,
        )

        schedule_names = {schedule.name for schedule in (definitions.schedules or [])}
        self.assertIn("weather_daily_schedule", schedule_names)

        self.assertEqual(
            "weather_daily_materialization_job",
            definitions.resolve_job_def("weather_daily_materialization_job").name,
        )


if __name__ == "__main__":
    unittest.main()
