import json
import os

import duckdb
import pandas as pd
import requests
from dagster import AssetExecutionContext, AssetKey, asset

from dagster_weather_intelligence_platform.utils import resolve_duckdb_path

HF_CHAT_URL = os.getenv("HF_CHAT_URL", "https://router.huggingface.co/v1/chat/completions")
HF_MODEL = os.getenv("WEATHER_HF_MODEL", "Qwen/Qwen2.5-7B-Instruct")
WEATHER_ENRICHMENT_BACKEND = os.getenv("WEATHER_ENRICHMENT_BACKEND", "huggingface")


def _read_daily(db_path: str | None = None) -> pd.DataFrame:
    path = db_path or resolve_duckdb_path()
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


def _persist_enriched_to_duckdb(df: pd.DataFrame, db_path: str | None = None) -> str:
    path = db_path or resolve_duckdb_path()
    con = duckdb.connect(path, read_only=False)
    try:
        con.execute("create schema if not exists analytics")
        con.register("weather_daily_enriched_df", df)
        con.execute(
            """
            create or replace table analytics.weather_daily_enriched as
            select * from weather_daily_enriched_df
            """
        )
        con.unregister("weather_daily_enriched_df")
    finally:
        con.close()
    return path


@asset(group_name="weather_ai_enrichment", deps=[AssetKey("mart_weather_daily")])
def weather_daily_enriched(
    context: AssetExecutionContext,
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
    db_path = _persist_enriched_to_duckdb(merged)

    context.add_output_metadata(
        {
            "input_rows": len(df),
            "enriched_rows": int(merged["label"].notna().sum()),
            "null_labels": int(merged["label"].isna().sum()),
            "backend": backend_used,
            "hf_model": HF_MODEL if backend_used == "huggingface" else "",
            "persisted_rows": len(merged),
            "duckdb_table": "analytics.weather_daily_enriched",
            "duckdb_path": db_path,
        }
    )
    return merged


def _enrich_rows(rows: list[dict], context: AssetExecutionContext) -> tuple[list[dict], str]:
    # Utilisation stricte du backend Hugging Face
    try:
        enriched = _enrich_with_huggingface(rows)
        return enriched, "huggingface"
    except Exception as exc:
        context.log.error("Hugging Face enrichment failed: %s", exc)
        # On stoppe le pipeline si le modèle échoue
        raise


def _extract_json_array(text: str | list[dict]) -> list[dict]:
    if isinstance(text, list):
        text = "".join(
            part.get("text", "") if isinstance(part, dict) else str(part) for part in text
        )

    cleaned = text.strip()
    if cleaned.startswith("```"):
        first_newline = cleaned.find("\n")
        if first_newline != -1:
            cleaned = cleaned[first_newline + 1 :]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

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

    system_prompt = (
        "You are a weather enrichment service. Return ONLY valid JSON array output. No markdown."
    )
    user_prompt = (
        "Given daily weather aggregates, return a JSON array with the same length.\n"
        "For each row include:\n"
        "- day_utc (copy exactly)\n"
        "- label: clear|cloudy|rainy|windy\n"
        "- summary: short French sentence (max 20 words)\n\n"
        f"Input rows:\n{json.dumps(rows)}"
    )

    response = requests.post(
        HF_CHAT_URL,
        headers={
            "Authorization": f"Bearer {hf_token}",
            "Content-Type": "application/json",
        },
        json={
            "model": HF_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 700,
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(payload["error"])
    try:
        raw_text = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected Hugging Face chat response format: {payload}") from exc

    parsed = _extract_json_array(raw_text)
    if len(parsed) != len(rows):
        raise ValueError(f"Hugging Face returned {len(parsed)} rows for {len(rows)} input rows")
    return parsed
