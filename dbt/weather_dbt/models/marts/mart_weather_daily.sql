{{ config(
    materialized='table',
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
  cast(date_trunc('day', ts_utc) as timestamp) as day_utc,
  cast(avg(temperature_2m) as double) as avg_temp_2m,
  cast(min(temperature_2m) as double) as min_temp_2m,
  cast(max(temperature_2m) as double) as max_temp_2m,
  cast(avg(wind_speed_10m) as double) as avg_wind_10m,
  cast(sum(precipitation) as double) as total_precipitation
from {{ ref('stg_open_meteo_hourly') }}
group by 1
