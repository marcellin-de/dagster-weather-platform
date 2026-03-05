select
    forecast_day_utc,
    pred_avg_temp_2m
from analytics.weather_forecast_7d
order by forecast_day_utc
