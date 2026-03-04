{{ config(
    materialized='incremental',
    unique_key='ts_utc',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with source_rows as (
  select
    ts_utc,
    latitude,
    longitude,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    cast(_dlt_load_id as double) as dlt_load_id,
    _dlt_id
  from {{ source('raw_weather', 'open_meteo_hourly') }}
  {% if is_incremental() %}
    where cast(ts_utc as timestamp) >= (
      select coalesce(max(ts_utc) - interval '3 day', timestamp '1970-01-01')
      from {{ this }}
    )
  {% endif %}
),
ranked as (
  select
    ts_utc,
    latitude,
    longitude,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    row_number() over (
      partition by ts_utc
      order by dlt_load_id desc, _dlt_id desc
    ) as row_num
  from source_rows
)

select
    cast(ts_utc as timestamp) as ts_utc,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(temperature_2m as double) as temperature_2m,
    cast(
        relative_humidity_2m as double
    ) as relative_humidity_2m,
    cast(precipitation as double) as precipitation,
    cast(wind_speed_10m as double) as wind_speed_10m
from ranked
where
    row_num = 1