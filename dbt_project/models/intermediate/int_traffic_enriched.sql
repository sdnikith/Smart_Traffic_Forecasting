WITH traffic AS (
    SELECT * FROM {{ ref('stg_traffic_readings') }}
),
weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
),
holidays AS (
    SELECT * FROM {{ ref('stg_holidays') }}
),
sensors AS (
    SELECT * FROM {{ ref('stg_sensors') }}
    WHERE is_current = TRUE
),
traffic_with_weather AS (
    SELECT
        t.*,
        w.temp_fahrenheit, w.humidity_pct, w.wind_speed_mph, w.weather_main,
        w.is_precipitation, w.temp_bucket, w.visibility_category, w.weather_severity,
        -- Join weather on nearest hour
        ROW_NUMBER() OVER (
            PARTITION BY t.sensor_id, t.reading_timestamp
            ORDER BY ABS(DATEDIFF('minute', t.reading_timestamp, w.weather_timestamp))
        ) AS weather_rank
    FROM traffic t
    LEFT JOIN weather w
        ON ABS(DATEDIFF('minute', t.reading_timestamp, w.weather_timestamp)) <= 60
),
traffic_weather_filtered AS (
    SELECT * FROM traffic_with_weather WHERE weather_rank = 1
),
enriched AS (
    SELECT
        {{ generate_surrogate_key(['twf.sensor_id', 'twf.reading_timestamp']) }} AS surrogate_key,
        twf.*,
        COALESCE(h.is_holiday, FALSE) AS is_holiday,
        h.holiday_name,
        s.city, s.road_name AS sensor_road_name,
        -- Rolling averages via window functions
        AVG(twf.traffic_volume) OVER (PARTITION BY twf.sensor_id ORDER BY twf.reading_timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_3hr,
        AVG(twf.traffic_volume) OVER (PARTITION BY twf.sensor_id ORDER BY twf.reading_timestamp ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_avg_24hr,
        -- Percent change
        (twf.traffic_volume - LAG(twf.traffic_volume) OVER (PARTITION BY twf.sensor_id ORDER BY twf.reading_timestamp))::FLOAT
        / NULLIF(LAG(twf.traffic_volume) OVER (PARTITION BY twf.sensor_id ORDER BY twf.reading_timestamp), 0) * 100 AS volume_pct_change,
        -- Traffic level classification
        {{ classify_traffic_level('twf.traffic_volume') }} AS traffic_level
    FROM traffic_weather_filtered twf
    LEFT JOIN holidays h ON twf.reading_timestamp::DATE = h.holiday_date
    LEFT JOIN sensors s ON twf.sensor_id = s.sensor_id
)
SELECT * FROM enriched
