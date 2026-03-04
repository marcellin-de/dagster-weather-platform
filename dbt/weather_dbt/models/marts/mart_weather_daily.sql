select
  date_trunc('day', ts_utc) as day_utc,
  avg(temperature_2m) as avg_temp_2m,
  min(temperature_2m) as min_temp_2m,
  max(temperature_2m) as max_temp_2m,
  avg(wind_speed_10m) as avg_wind_10m,
  sum(precipitation) as total_precipitation
from {{ ref('stg_open_meteo_hourly') }}
group by 1