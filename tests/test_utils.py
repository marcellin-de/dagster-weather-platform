import os
import unittest
from pathlib import Path
from unittest import mock

from dagster_weather_intelligence_platform.utils import resolve_duckdb_path


class ResolveDuckdbPathTestCase(unittest.TestCase):
    @mock.patch.dict(os.environ, {"WEATHER_DUCKDB_PATH": "/custom/path.duckdb"}, clear=False)
    def test_weather_duckdb_path_env_takes_precedence(self) -> None:
        self.assertEqual("/custom/path.duckdb", resolve_duckdb_path())

    @mock.patch.dict(
        os.environ,
        {"WEATHER_DBT_DUCKDB_PATH": "/dbt/path.duckdb"},
        clear=False,
    )
    def test_weather_dbt_duckdb_path_env_is_second_priority(self) -> None:
        os.environ.pop("WEATHER_DUCKDB_PATH", None)
        self.assertEqual("/dbt/path.duckdb", resolve_duckdb_path())

    @mock.patch.dict(
        os.environ,
        {"DAGSTER_PROJECT_ROOT": "/project"},
        clear=False,
    )
    def test_dagster_project_root_env_builds_expected_path(self) -> None:
        os.environ.pop("WEATHER_DUCKDB_PATH", None)
        os.environ.pop("WEATHER_DBT_DUCKDB_PATH", None)
        self.assertEqual(
            str(Path("/project") / "src" / "weather_ingest.duckdb"),
            resolve_duckdb_path(),
        )

    def test_fallback_finds_pyproject_toml(self) -> None:
        # When no env vars are set, the utility walks up to find pyproject.toml
        os.environ.pop("WEATHER_DUCKDB_PATH", None)
        os.environ.pop("WEATHER_DBT_DUCKDB_PATH", None)
        os.environ.pop("DAGSTER_PROJECT_ROOT", None)
        result = resolve_duckdb_path()
        result_path = Path(result)
        # The resolved path must be <project_root>/src/weather_ingest.duckdb
        # and pyproject.toml must exist at the project root
        self.assertEqual(result_path.name, "weather_ingest.duckdb")
        self.assertEqual(result_path.parent.name, "src")
        project_root = result_path.parent.parent
        self.assertTrue(
            (project_root / "pyproject.toml").exists(),
            f"pyproject.toml not found at {project_root}",
        )


if __name__ == "__main__":
    unittest.main()
