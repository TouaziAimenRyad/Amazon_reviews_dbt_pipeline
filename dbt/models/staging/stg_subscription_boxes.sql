{{
  config(
    materialized='view',
    tags=['staging']
  )
}}

WITH source AS (
    SELECT
        review_id,
        asin as product_id,
        parent_asin,
        user_id as reviewer_id,
        reviewer_name,
        review_text,
        overall_rating,
        summary as review_title,
        review_time,
        helpful_vote,
        verified_purchase,
        images,
        _loaded_at,
        _source_file
    FROM {{ source('amazon_raw', 'subscription_boxes') }}
)

SELECT
    review_id,
    product_id,
    parent_asin,
    reviewer_id,
    reviewer_name,
    review_text,
    overall_rating,
    review_title,
    review_time,
    helpful_vote,
    verified_purchase,
    images,
    _loaded_at,
    _source_file,
    -- Add some derived columns
    LENGTH(review_text) as review_length,
    CASE 
        WHEN helpful_vote > 0 THEN TRUE 
        ELSE FALSE 
    END as is_helpful
FROM source