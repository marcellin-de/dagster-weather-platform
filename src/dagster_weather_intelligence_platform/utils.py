"""Shared utilities for the weather intelligence platform."""

import os
from pathlib import Path

_PROJECT_ROOT_SENTINEL = "pyproject.toml"


def resolve_duckdb_path() -> str:
    """Return the absolute path to the project DuckDB file.

    Resolution order:
    1. ``WEATHER_DUCKDB_PATH`` env var (explicit override)
    2. ``WEATHER_DBT_DUCKDB_PATH`` env var (dbt-oriented override)
    3. ``DAGSTER_PROJECT_ROOT`` env var + ``src/weather_ingest.duckdb``
    4. Walk up from this file until ``pyproject.toml`` is found
    """
    if os.getenv("WEATHER_DUCKDB_PATH"):
        return os.environ["WEATHER_DUCKDB_PATH"]
    if os.getenv("WEATHER_DBT_DUCKDB_PATH"):
        return os.environ["WEATHER_DBT_DUCKDB_PATH"]
    project_root = os.getenv("DAGSTER_PROJECT_ROOT")
    if project_root:
        return str(Path(project_root) / "src" / "weather_ingest.duckdb")
    return str(_find_project_root() / "src" / "weather_ingest.duckdb")


def _find_project_root() -> Path:
    """Walk up from this file's directory until ``pyproject.toml`` is found."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / _PROJECT_ROOT_SENTINEL).exists():
            return current
        current = current.parent
    raise FileNotFoundError(
        f"Could not locate {_PROJECT_ROOT_SENTINEL} in any parent of {Path(__file__).resolve()}. "
        "Set the DAGSTER_PROJECT_ROOT environment variable to the project root directory."
    )
