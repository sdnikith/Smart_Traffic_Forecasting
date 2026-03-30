SELECT COUNT(*) AS violations
FROM {{ ref('mart_hourly_traffic') }}
WHERE reading_timestamp > CURRENT_TIMESTAMP()
HAVING COUNT(*) > 0
