SELECT COUNT(*) AS violations
FROM {{ ref('stg_weather') }}
WHERE weather_severity > 5 AND is_precipitation = FALSE
HAVING COUNT(*) > 0
