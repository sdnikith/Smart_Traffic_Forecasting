{{
  config(
    materialized='incremental',
    unique_key=['sensor_id', 'hour_of_day', 'day_of_week'],
    incremental_strategy='merge'
  )
}}

SELECT
    sensor_id,
    hour_of_day,
    day_of_week,
    is_weekend,
    weather_main,
    temp_bucket,
    AVG(traffic_volume) AS avg_volume,
    STDDEV(traffic_volume) AS std_volume,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY traffic_volume) AS p90_volume,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY traffic_volume) AS p95_volume,
    COUNT(*) AS sample_size,
    AVG(CASE WHEN is_precipitation THEN traffic_volume END) AS avg_volume_precipitation,
    AVG(CASE WHEN NOT is_precipitation THEN traffic_volume END) AS avg_volume_clear,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ ref('mart_hourly_traffic') }}
GROUP BY sensor_id, hour_of_day, day_of_week, is_weekend, weather_main, temp_bucket
HAVING COUNT(*) >= 30
