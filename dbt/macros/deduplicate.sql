{% macro deduplicate(relation, partition_by, order_by) %}
    {#
        Generic deduplication macro that can be used across models
        Parameters:
            - relation: The source table/view to deduplicate
            - partition_by: Column(s) to partition by for deduplication
            - order_by: Column(s) to determine which row to keep
    #}
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ) as row_num
        FROM {{ relation }}
    )
    
    SELECT * EXCLUDE (row_num)
    FROM ranked
    WHERE row_num = 1
{% endmacro %}