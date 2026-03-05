select
    day_utc,
    avg_temp_2m,
    min_temp_2m,
    max_temp_2m,
    total_precipitation
from analytics.mart_weather_daily
order by day_utc
