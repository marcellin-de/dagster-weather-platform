#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${DAGSTER_HOME:-/opt/platform/.dg}"
uv run python /opt/platform/docker/init_duckdb.py

if [[ -n "${MLFLOW_TRACKING_URI:-}" ]]; then
  echo "[dagster] waiting for MLflow at ${MLFLOW_TRACKING_URI} ..."
  for _ in {1..60}; do
    if uv run python -c "import urllib.request; urllib.request.urlopen('${MLFLOW_TRACKING_URI}', timeout=2)"; then
      echo "[dagster] MLflow is reachable"
      break
    fi
    sleep 2
  done
fi

exec uv run dagster dev -h 0.0.0.0 -p 3000
