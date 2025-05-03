{{
  config(
    materialized='table',
    tags=['core', 'fact']
  )
}}

WITH reviews AS (
    SELECT
        r.review_id,
        r.product_id,
        r.reviewer_id,
        r.reviewer_name,
        r.overall_rating,
        r.review_time,
        r.helpful_vote,
        r.verified_purchase,
        r.review_length,
        r.is_helpful,
        p.product_category,
        r._loaded_at
    FROM {{ ref('stg_subscription_boxes') }} r
    JOIN {{ ref('dim_products') }} p ON r.product_id = p.product_id
    
    UNION ALL
    
    SELECT
        r.review_id,
        r.product_id,
        r.reviewer_id,
        r.reviewer_name,
        r.overall_rating,
        r.review_time,
        r.helpful_vote,
        r.verified_purchase,
        r.review_length,
        r.is_helpful,
        p.product_category,
        r._loaded_at
    FROM {{ ref('stg_gift_cards') }} r
    JOIN {{ ref('dim_products') }} p ON r.product_id = p.product_id
    
    UNION ALL
    
    SELECT
        r.review_id,
        r.product_id,
        r.reviewer_id,
        r.reviewer_name,
        r.overall_rating,
        r.review_time,
        r.helpful_vote,
        r.verified_purchase,
        r.review_length,
        r.is_helpful,
        p.product_category,
        r._loaded_at
    FROM {{ ref('stg_digital_music') }} r
    JOIN {{ ref('dim_products') }} p ON r.product_id = p.product_id
)

SELECT
    review_id,
    product_id,
    reviewer_id,
    reviewer_name,
    overall_rating,
    review_time,
    helpful_vote,
    verified_purchase,
    review_length,
    is_helpful,
    product_category,
    _loaded_at,
    -- Add some business metrics
    CASE
        WHEN overall_rating >= 4 THEN 'positive'
        WHEN overall_rating <= 2 THEN 'negative'
        ELSE 'neutral'
    END as sentiment,
    DATE_TRUNC('month', review_time) as review_month
FROM reviews