# Dagster Weather Intelligence Platform

End-to-end data platform to collect, transform, validate, and operationalize hourly weather data, orchestrated with Dagster and observed with MLflow.

## Overview

This project demonstrates a modern Analytics Engineering / MLOps architecture:

- Weather API ingestion with `dlt` (Open-Meteo, no API key)
- Analytical storage in `DuckDB`
- SQL transformations and data contracts with `dbt`
- Orchestration and quality checks with `Dagster`
- Model training + inference for 7-day temperature forecasts
- ML run tracking with `MLflow`
- Business-facing analytics with `Evidence`

## Global Asset Lineage

![Global Asset Lineage](docs/Global_Asset_Lineage.svg)

Source file: [docs/Global_Asset_Lineage.svg](docs/Global_Asset_Lineage.svg)

## Technical Architecture

```text
Open-Meteo API
   -> dlt ingestion (raw_weather.open_meteo_hourly)
   -> dbt staging (analytics.stg_open_meteo_hourly)
   -> dbt mart (analytics.mart_weather_daily)
   -> Dagster AI enrichment (analytics.weather_daily_enriched)
   -> ML training (Ridge, MAE gate) + 7-day forecast (analytics.weather_forecast_7d)
   -> Evidence dashboards
```

Core code:

```text
src/dagster_weather_intelligence_platform/
├── defs/weather_duckdb_ingest/   # dlt source + pipeline
├── defs/transform/               # dbt component
├── assets/                       # enrichment + ML assets
├── checks/                       # quality gates (GX + ML metrics)
├── resources/                    # shared resources (GE, MLflow)
├── orchestration.py              # jobs, schedules, sensor
└── definitions.py                # Dagster composition root
```

## Value

- Production-grade, modular, testable data pipeline design
- Data contract implementation with `dbt` tests
- Event-driven automation using schedules and sensors
- End-to-end ML use case integrated into the same asset graph
- Unified observability (data quality + model quality + experiment tracking)
- Reproducible local stack (`uv`, `Makefile`, `Docker Compose`, GitHub Actions CI)

## Quick Start (Local)

Prerequisites:

- Python `>=3.10,<3.15`
- `uv`

Install:

```bash
uv sync
source .venv/bin/activate
```

Verify:

```bash
make check
make test
```

Start Dagster:

```bash
make up
```

UI Dagster: `http://localhost:3000`

## Full Stack Run (Docker)

```bash
make compose-up
```

Available services:

- Dagster UI: `http://localhost:3000`
- MLflow UI: `http://localhost:5000`
- Evidence UI: `http://localhost:3001`

Stop:

```bash
make compose-down
```

## Orchestration and Automation

- Hourly ingestion job: `weather_ingestion_hourly_job`
- Model training job: `weather_model_training_job`
- Ingestion schedule: `0 * * * *` (UTC)
- Training schedule: `15 2 * * *` (UTC)
- Sensor: `trigger_training_after_ingestion_success` (max 1 training run per UTC day after successful ingestion)

## Data Quality and Model Quality

- Great Expectations checks on raw data (`raw_weather`)
- Additional checks on enriched assets
- Model quality threshold through MAE check
- dbt data contracts enabled on key models:
  - `analytics.stg_open_meteo_hourly`
  - `analytics.mart_weather_daily`

## Useful Environment Variables

DuckDB:

```bash
export WEATHER_DUCKDB_PATH=/absolute/path/to/weather_ingest.duckdb
export WEATHER_DBT_DUCKDB_PATH=/absolute/path/to/weather_ingest.duckdb
```

AI enrichment (Hugging Face):

```bash
export WEATHER_ENRICHMENT_BACKEND=huggingface
export HF_TOKEN=hf_xxx
export WEATHER_HF_MODEL=Qwen/Qwen2.5-7B-Instruct
export HF_CHAT_URL=https://router.huggingface.co/v1/chat/completions
```

## CI/CD

GitHub Actions pipeline (`.github/workflows/ci.yml`):

- Lint + Dagster definitions validation + Python tests
- dbt build with contracts/tests on seeded CI dataset
- Build Evidence
- Build Docker Compose images

## Useful Commands

- `make help`: list commands
- `make list`: list Dagster definitions
- `make check`: validate definitions
- `make test`: run unit tests
- `make verify`: check + tests
- `make pipeline`: check + tests + dbt build

## Additional Documentation

- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- README dbt: [dbt/weather_dbt/README.md](dbt/weather_dbt/README.md)
- Evidence app: [evidence/README.md](evidence/README.md)
