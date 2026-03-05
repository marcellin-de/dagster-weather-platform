# Dagster Weather Intelligence Platform

Ingestion and quality checks for Open-Meteo hourly weather data using Dagster, dlt, DuckDB, and Great Expectations.

## Project structure

```text
src/dagster_weather_intelligence_platform/
├── defs/weather_duckdb_ingest/   # dlt source + pipeline component
├── defs/transform/               # dbt project component
├── checks/                       # Dagster asset checks
├── resources/                    # Shared resources (Great Expectations)
├── orchestration.py              # Asset jobs + schedules + sensors
└── definitions.py                # Composition root (components + checks + resources)
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

## Full platform with Docker Compose

Run the full stack with one command:

```bash
make compose-up
```

Services:

- Dagster UI: `http://localhost:3000`
- MLflow UI: `http://localhost:5000`
- Evidence UI: `http://localhost:3001`

Stop everything:

```bash
make compose-down
```

The Python services use `uv` inside Docker for reproducible dependency resolution from `pyproject.toml` + `uv.lock`.

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
- Override dbt DuckDB path with:

```bash
export WEATHER_DBT_DUCKDB_PATH=/absolute/path/to/weather_ingest.duckdb
```

- Configure AI enrichment with Hugging Face:

```bash
export WEATHER_ENRICHMENT_BACKEND=huggingface
export HF_TOKEN=hf_xxx
export WEATHER_HF_MODEL=Qwen/Qwen2.5-7B-Instruct
# optional:
export HF_CHAT_URL=https://router.huggingface.co/v1/chat/completions
```

`make up` also sets `DAGSTER_PROJECT_ROOT` automatically so dbt assets executed by Dagster resolve the shared DuckDB path correctly.

## Orchestration (Dagster)

- Hourly ingestion schedule: `weather_ingestion_hourly_schedule` (`0 * * * *`, UTC).
- Daily ML training schedule: `weather_model_training_daily_schedule` (`15 2 * * *`, UTC).
- Sensor: `trigger_training_after_ingestion_success` triggers one training run per UTC day after successful hourly ingestion.
- Dagster group names:
  - `weather_ingestion` for raw source ingestion assets.
  - `weather_analytics` for dbt transformed assets.
- AI enrichment asset persists output to DuckDB table: `analytics.weather_daily_enriched`.

## Data Contracts (dbt)

- Contract enforcement enabled on:
  - `stg_open_meteo_hourly` (incremental model)
  - `mart_weather_daily` (table model)
- Schema drift protection via `on_schema_change: fail`.
- Advanced tests include recency, range checks, freshness, and row-count expectations.

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) includes:

- Python quality gate: lint + Dagster definition check + unit tests.
- dbt contracts pipeline: `dbt deps` + `dbt build` on seeded CI dataset.
- Evidence pipeline: `npm run sources` + `npm run build`.
- Docker pipeline: `docker compose build`.

## Collaboration

Contribution guidelines live in [`CONTRIBUTING.md`](CONTRIBUTING.md).
