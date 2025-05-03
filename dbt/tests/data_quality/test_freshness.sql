{% set freshness_threshold = 48 %}

WITH latest_loads AS (
    SELECT
        table_name,
        MAX(_loaded_at) as last_loaded_at,
        EXTRACT(HOUR FROM (CURRENT_TIMESTAMP - MAX(_loaded_at))) as hours_since_load
    FROM (
        SELECT 'stg_subscription_boxes' as table_name, _loaded_at FROM {{ ref('stg_subscription_boxes') }}
        UNION ALL
        SELECT 'stg_gift_cards' as table_name, _loaded_at FROM {{ ref('stg_gift_cards') }}
        UNION ALL
        SELECT 'stg_digital_music' as table_name, _loaded_at FROM {{ ref('stg_digital_music') }}
    ) all_tables
    GROUP BY 1
)

SELECT
    table_name,
    last_loaded_at,
    hours_since_load
FROM latest_loads
WHERE hours_since_load > {{ freshness_threshold }}