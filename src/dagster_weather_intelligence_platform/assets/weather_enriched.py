import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import requests

from dagster import AssetKey, asset


HF_API_BASE_URL = os.getenv("HF_API_BASE_URL", "https://api-inference.huggingface.co/models")
HF_MODEL = os.getenv("WEATHER_HF_MODEL", "google/flan-t5-base")
WEATHER_ENRICHMENT_BACKEND = os.getenv("WEATHER_ENRICHMENT_BACKEND", "huggingface")


def _default_duckdb_path() -> str:
    if os.getenv("WEATHER_DUCKDB_PATH"):
        return os.environ["WEATHER_DUCKDB_PATH"]
    if os.getenv("WEATHER_DBT_DUCKDB_PATH"):
        return os.environ["WEATHER_DBT_DUCKDB_PATH"]
    project_root = os.getenv("DAGSTER_PROJECT_ROOT")
    if project_root:
        return str(Path(project_root) / "src" / "weather_ingest.duckdb")
    return str(Path(__file__).resolve().parents[3] / "src" / "weather_ingest.duckdb")


def _read_daily(db_path: str | None = None) -> pd.DataFrame:
    path = db_path or _default_duckdb_path()
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(
            """
            select
              day_utc,
              avg_temp_2m,
              min_temp_2m,
              max_temp_2m,
              avg_wind_10m,
              total_precipitation
            from analytics.mart_weather_daily
            order by day_utc desc
            limit 30
            """
        ).df()
    finally:
        con.close()


@asset(group_name="weather_ai_enrichment", deps=[AssetKey("mart_weather_daily")])
def weather_daily_enriched(
    context,
) -> pd.DataFrame:
    df = _read_daily()

    payload_df = df.copy()
    payload_df["day_utc"] = pd.to_datetime(
        payload_df["day_utc"], errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d")
    rows = payload_df.to_dict(orient="records")
    enriched, backend_used = _enrich_rows(rows=rows, context=context)

    out = pd.DataFrame(enriched)
    if "day_utc" in out.columns:
        out["day_utc"] = out["day_utc"].astype(str)
    merged = payload_df.merge(out, on="day_utc", how="left")

    context.add_output_metadata(
        {
            "input_rows": len(df),
            "enriched_rows": int(merged["label"].notna().sum()),
            "null_labels": int(merged["label"].isna().sum()),
            "backend": backend_used,
        }
    )
    return merged


def _enrich_rows(rows: list[dict], context) -> tuple[list[dict], str]:
    if WEATHER_ENRICHMENT_BACKEND in {"huggingface", "hf"}:
        try:
            return _enrich_with_huggingface(rows), "huggingface"
        except Exception as exc:
            context.log.warning("Hugging Face enrichment failed, fallback to heuristic: %s", exc)
            return _enrich_with_heuristic(rows), "heuristic_fallback"
    return _enrich_with_heuristic(rows), "heuristic"


def _extract_json_array(text: str) -> list[dict]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.replace("json\n", "", 1).strip()

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Could not find a JSON array in model output")
    parsed = json.loads(cleaned[start : end + 1])
    if not isinstance(parsed, list):
        raise ValueError("Parsed output is not a JSON array")
    return parsed


def _enrich_with_huggingface(rows: list[dict]) -> list[dict]:
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise RuntimeError("Missing HF_TOKEN environment variable")

    prompt = (
        "You are a weather enrichment service.\n"
        "Return ONLY a JSON array with the same length as input.\n"
        "For each row include fields:\n"
        "- day_utc (copy exactly)\n"
        "- label: clear|cloudy|rainy|windy\n"
        "- summary: short French sentence (max 20 words)\n\n"
        f"Input rows:\n{json.dumps(rows)}"
    )

    response = requests.post(
        f"{HF_API_BASE_URL}/{HF_MODEL}",
        headers={
            "Authorization": f"Bearer {hf_token}",
            "Content-Type": "application/json",
        },
        json={
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 700,
                "temperature": 0.2,
                "return_full_text": False,
            },
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(payload["error"])

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        raw_text = payload[0].get("generated_text", "")
    elif isinstance(payload, dict):
        raw_text = payload.get("generated_text", "")
    else:
        raise RuntimeError("Unexpected Hugging Face response format")

    parsed = _extract_json_array(raw_text)
    if len(parsed) != len(rows):
        raise ValueError(
            f"Hugging Face returned {len(parsed)} rows for {len(rows)} input rows"
        )
    return parsed


def _enrich_with_heuristic(rows: list[dict]) -> list[dict]:
    enriched: list[dict] = []
    for row in rows:
        precipitation = float(row.get("total_precipitation") or 0.0)
        wind = float(row.get("avg_wind_10m") or 0.0)
        temp = float(row.get("avg_temp_2m") or 0.0)

        if precipitation >= 2.0:
            label = "rainy"
            summary = "Pluie probable, prevoir parapluie."
        elif wind >= 30.0:
            label = "windy"
            summary = "Vent soutenu, sorties a proteger."
        elif precipitation < 0.2 and temp >= 24.0:
            label = "clear"
            summary = "Temps plutot degage et agreable."
        else:
            label = "cloudy"
            summary = "Ciel nuageux avec conditions stables."

        enriched.append(
            {
                "day_utc": str(row.get("day_utc", "")),
                "label": label,
                "summary": summary,
            }
        )
    return enriched
