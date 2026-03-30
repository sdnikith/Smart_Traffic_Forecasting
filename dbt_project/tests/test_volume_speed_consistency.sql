SELECT COUNT(*) AS violations
FROM {{ ref('mart_hourly_traffic') }}
WHERE traffic_volume > 5000 AND traffic_level = 'low'
HAVING COUNT(*) > 0
