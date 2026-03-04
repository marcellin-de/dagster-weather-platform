import unittest

from dagster_weather_intelligence_platform.definitions import build_extra_defs


class DefinitionsTestCase(unittest.TestCase):
    def test_definitions_include_checks_and_resources(self) -> None:
        definitions = build_extra_defs()

        self.assertIn("ge", definitions.resources)

        check_names = {
            spec.name
            for check_def in (definitions.asset_checks or [])
            for spec in check_def.check_specs
        }
        self.assertEqual(
            {
                "enriched_labels_quality_gate",
                "ge_raw_hourly_basic_validations",
                "ge_raw_hourly_temperature_validations",
            },
            check_names,
        )

        schedule_names = {schedule.name for schedule in (definitions.schedules or [])}
        self.assertIn("weather_daily_schedule", schedule_names)

        job_names = {job_def.name for job_def in (definitions.jobs or [])}
        self.assertIn("weather_daily_materialization_job", job_names)


if __name__ == "__main__":
    unittest.main()
