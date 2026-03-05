#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${WEATHER_DUCKDB_PATH:-/data/weather_ingest.duckdb}"
PORT="${EVIDENCE_PORT:-3001}"

mkdir -p /opt/platform/src
ln -sf "${DB_PATH}" /opt/platform/src/weather_ingest.duckdb

echo "[evidence] generating sources..."
npm run sources

echo "[evidence] building static app..."
npm run build

echo "[evidence] starting preview server on :${PORT}..."
exec npm run preview -- --listen "tcp://0.0.0.0:${PORT}"
