{{
  config(
    materialized='incremental',
    unique_key=['sensor_id', 'traffic_date'],
    incremental_strategy='merge'
  )
}}

SELECT
    sensor_id,
    reading_timestamp::DATE AS traffic_date,
    COUNT(*) AS total_readings,
    AVG(traffic_volume) AS avg_volume,
    MAX(traffic_volume) AS peak_volume,
    MIN(traffic_volume) AS min_volume,
    AVG(CASE WHEN is_rush_hour THEN traffic_volume END) AS avg_rush_hour_volume,
    AVG(CASE WHEN NOT is_rush_hour THEN traffic_volume END) AS avg_off_peak_volume,
    AVG(weather_severity) AS avg_weather_severity,
    SUM(CASE WHEN traffic_level = 'congested' THEN 1 ELSE 0 END) AS congested_hours,
    is_holiday,
    holiday_name,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ ref('mart_hourly_traffic') }}
GROUP BY sensor_id, reading_timestamp::DATE, is_holiday, holiday_name
