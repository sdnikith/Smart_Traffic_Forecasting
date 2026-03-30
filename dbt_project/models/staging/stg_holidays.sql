SELECT
    date AS holiday_date,
    name AS holiday_name,
    TRUE AS is_holiday,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM {{ source('raw', 'holidays') }}
