import requests

import dlt


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_FIELDS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)


def _extract_hourly_records(hourly: dict, latitude: float, longitude: float):
    times = hourly.get("time", [])

    for idx, ts_utc in enumerate(times):
        row = {
            "ts_utc": ts_utc,
            "latitude": latitude,
            "longitude": longitude,
        }
        for field in HOURLY_FIELDS:
            values = hourly.get(field, [])
            row[field] = values[idx] if idx < len(values) else None
        yield row


@dlt.source
def open_meteo_source(latitude: float = 36.8065, longitude: float = 10.1815):
    """Open-Meteo source (no API key) for hourly weather data."""

    @dlt.resource(name="open_meteo_hourly", write_disposition="append")
    def open_meteo_hourly():
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(HOURLY_FIELDS),
            "timezone": "UTC",
        }

        response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        response.raise_for_status()

        payload = response.json()
        hourly = payload.get("hourly")
        if not isinstance(hourly, dict):
            raise ValueError("Open-Meteo payload is missing the 'hourly' object")

        yield from _extract_hourly_records(hourly, latitude=latitude, longitude=longitude)

    return open_meteo_hourly


# dlt objects referenced by Dagster in defs.yaml
weather_source = open_meteo_source()
weather_pipeline = dlt.pipeline(
    pipeline_name="weather_ingest",
    destination="duckdb",
    dataset_name="raw_weather",
)
