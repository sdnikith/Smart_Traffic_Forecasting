WITH source AS (
    SELECT * FROM {{ source('raw', 'traffic_events') }}
),
cleaned AS (
    SELECT
        sensor_id,
        timestamp AS reading_timestamp,
        traffic_volume,
        speed_mph,
        occupancy_pct,
        road_name,
        direction,
        latitude,
        longitude,
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM timestamp) AS day_of_week,
        CASE WHEN EXTRACT(DAYOFWEEK FROM timestamp) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(HOUR FROM timestamp) BETWEEN {{ var('rush_hour_start') }} AND {{ var('rush_hour_end') }}
             AND EXTRACT(DAYOFWEEK FROM timestamp) NOT IN (0, 6)
             THEN TRUE ELSE FALSE END AS is_rush_hour,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(YEAR FROM timestamp) AS year,
        CURRENT_TIMESTAMP() AS etl_loaded_at
    FROM source
    WHERE traffic_volume BETWEEN {{ var('volume_min') }} AND {{ var('volume_max') }}
      AND timestamp IS NOT NULL
      AND sensor_id IS NOT NULL
)
SELECT * FROM cleaned
