WITH source AS (
    SELECT * FROM {{ source('raw', 'weather_updates') }}
),
cleaned AS (
    SELECT
        timestamp AS weather_timestamp,
        temp_fahrenheit,
        humidity_pct,
        wind_speed_mph,
        weather_main,
        weather_description,
        visibility_miles,
        clouds_pct,
        COALESCE(rain_1h_mm, 0) AS rain_1h_mm,
        COALESCE(snow_1h_mm, 0) AS snow_1h_mm,
        CASE WHEN COALESCE(rain_1h_mm, 0) > 0 OR COALESCE(snow_1h_mm, 0) > 0 THEN TRUE ELSE FALSE END AS is_precipitation,
        CASE
            WHEN temp_fahrenheit < 32 THEN 'freezing'
            WHEN temp_fahrenheit < 50 THEN 'cold'
            WHEN temp_fahrenheit < 70 THEN 'mild'
            WHEN temp_fahrenheit < 85 THEN 'warm'
            ELSE 'hot'
        END AS temp_bucket,
        CASE
            WHEN visibility_miles < 2 THEN 'poor'
            WHEN visibility_miles < 5 THEN 'moderate'
            ELSE 'good'
        END AS visibility_category,
        -- Weather severity: 0 (clear) to 10 (blizzard)
        LEAST(10, (COALESCE(rain_1h_mm, 0) * 2 + COALESCE(snow_1h_mm, 0) * 5 + GREATEST(0, wind_speed_mph - 20) * 0.3 + GREATEST(0, 5 - visibility_miles) * 1.5)::NUMERIC(4,1)) AS weather_severity,
        CURRENT_TIMESTAMP() AS etl_loaded_at
    FROM source
    WHERE temp_fahrenheit BETWEEN -50 AND 130
      AND timestamp IS NOT NULL
)
SELECT * FROM cleaned
