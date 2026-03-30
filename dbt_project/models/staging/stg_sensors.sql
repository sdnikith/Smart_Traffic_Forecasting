SELECT
    sensor_id,
    road_name,
    city,
    direction,
    latitude,
    longitude,
    is_active,
    installed_date,
    updated_at,
    -- SCD Type 2 fields
    updated_at AS valid_from,
    COALESCE(LEAD(updated_at) OVER (PARTITION BY sensor_id ORDER BY updated_at), '9999-12-31'::TIMESTAMP) AS valid_to,
    CASE WHEN LEAD(updated_at) OVER (PARTITION BY sensor_id ORDER BY updated_at) IS NULL THEN TRUE ELSE FALSE END AS is_current,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ source('raw', 'sensor_metadata') }}
