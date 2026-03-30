{% macro classify_traffic_level(volume_column) %}
    CASE
        WHEN {{ volume_column }} < 1000 THEN 'low'
        WHEN {{ volume_column }} < 3000 THEN 'medium'
        WHEN {{ volume_column }} < 5000 THEN 'high'
        ELSE 'congested'
    END
{% endmacro %}
