{{
  config(
    materialized='incremental',
    unique_key='surrogate_key',
    incremental_strategy='merge',
    cluster_by=['sensor_id', 'reading_timestamp']
  )
}}

SELECT * FROM {{ ref('int_traffic_enriched') }}

{% if is_incremental() %}
WHERE reading_timestamp > (SELECT MAX(reading_timestamp) FROM {{ this }})
{% endif %}
