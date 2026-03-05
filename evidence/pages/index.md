# Weather Intelligence Platform

## Historical Temperature

```sql weather_daily
select
  day_utc,
  avg_temp_2m,
  min_temp_2m,
  max_temp_2m,
  total_precipitation
from memory.needful_things.weather_daily
order by day_utc
```

<LineChart
    data={weather_daily}
    x=day_utc
    y=avg_temp_2m
/>

---

## Temperature Forecast (7 days)

```sql weather_forecast
select
  forecast_day_utc,
  pred_avg_temp_2m
from memory.needful_things.weather_forecast
order by forecast_day_utc
```

<LineChart
    data={weather_forecast}
    x=forecast_day_utc
    y=pred_avg_temp_2m
/>

---

## Weather Enrichment

```sql weather_labels
select
  day_utc,
  case
    when total_precipitation > 1 then 'rainy'
    when avg_temp_2m >= 26 then 'clear'
    when avg_temp_2m <= 8 then 'cloudy'
    else 'mild'
  end as label,
  concat('Temp moyenne: ', round(avg_temp_2m, 1), '°C') as summary
from memory.needful_things.weather_daily
order by day_utc desc
limit 20
```

<DataTable data={weather_labels} />
