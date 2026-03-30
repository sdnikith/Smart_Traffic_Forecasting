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
    MAX(traffic_volume) AS max_volume,
    AVG(speed_mph) AS avg_speed,
    MAX(speed_mph) AS max_speed,
    MIN(speed_mph) AS min_speed,
    AVG(occupancy_pct) AS avg_occupancy,
    MAX(occupancy_pct) AS max_occupancy,
    -- Performance metrics
    COUNT(CASE WHEN traffic_level = 'congested' THEN 1 END) AS congestion_events,
    COUNT(CASE WHEN is_rush_hour THEN 1 END) AS rush_hour_events,
    COUNT(CASE WHEN is_precipitation THEN 1 END) AS precipitation_events,
    -- Reliability metrics
    COUNT(CASE WHEN traffic_volume > 0 THEN 1 END) AS valid_readings,
    COUNT(CASE WHEN traffic_volume = 0 THEN 1 END) AS zero_volume_events,
    -- Weather impact
    AVG(weather_severity) AS avg_weather_severity,
    MAX(weather_severity) AS max_weather_severity,
    -- Time-based metrics
    AVG(CASE WHEN is_weekend THEN traffic_volume END) AS avg_weekend_volume,
    AVG(CASE WHEN NOT is_weekend THEN traffic_volume END) AS avg_weekday_volume,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ ref('mart_hourly_traffic') }}
GROUP BY sensor_id, reading_timestamp::DATE
