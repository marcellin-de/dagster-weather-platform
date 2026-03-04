# Dagster Weather Intelligence Platform

Ingestion and quality checks for Open-Meteo hourly weather data using Dagster, dlt, DuckDB, and Great Expectations.

## Project structure

```text
src/dagster_weather_intelligence_platform/
├── defs/weather_duckdb_ingest/   # dlt source + pipeline component
├── checks/                       # Dagster asset checks
├── resources/                    # Shared resources (Great Expectations)
└── definitions.py                # Composition root (assets + checks + resources)
```

## Quick start

1. Install dependencies:

```bash
uv sync
```

2. Activate environment:

```bash
source .venv/bin/activate
```

3. Validate definitions:

```bash
make check
```

4. Start Dagster UI:

```bash
dg dev
```

Open `http://localhost:3000`.

## Common commands

- `make list`: list Dagster definitions.
- `make check`: run `dg check defs`.
- `make test`: run unit tests.
- `make verify`: run checks + tests.

## Configuration

- Default DuckDB path: `src/weather_ingest.duckdb`.
- Override DuckDB path for checks with env var:

```bash
export WEATHER_DUCKDB_PATH=/absolute/path/to/weather_ingest.duckdb
```

## Partitions and schedule

- Weather assets are partitioned daily (UTC) from `2026-01-01`.
- A partitioned job runs assets + checks: `weather_daily_materialization_job`.
- A daily schedule is defined: `weather_daily_schedule` (`0 6 * * *`, UTC).
- Asset checks are configured with eager automation conditions.

## Collaboration

Contribution guidelines live in [`CONTRIBUTING.md`](CONTRIBUTING.md).
