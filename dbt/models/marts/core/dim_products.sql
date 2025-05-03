{{
  config(
    materialized='table',
    tags=['core', 'dimension']
  )
}}

{% set categories = ['subscription_boxes', 'gift_cards', 'digital_music'] %}

WITH products_union AS (
  {% for category in categories %}
    SELECT
      product_id,
      parent_asin,
      '{{ category }}' as product_category,
      _loaded_at
    FROM {{ ref('stg_' ~ category) }}
    {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
),

deduplicated AS (
    SELECT
        product_id,
        parent_asin,
        product_category,
        _loaded_at,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) as rn
    FROM products_union
)

SELECT
    product_id,
    parent_asin,
    product_category,
    _loaded_at
FROM deduplicated
WHERE rn = 1