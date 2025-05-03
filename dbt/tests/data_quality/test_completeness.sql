WITH completeness_metrics AS (
    SELECT
        'stg_subscription_boxes' as table_name,
        COUNT(*) as total_rows,
        COUNT(review_id) as review_id_count,
        COUNT(product_id) as product_id_count,
        COUNT(reviewer_id) as reviewer_id_count,
        COUNT(overall_rating) as rating_count
    FROM {{ ref('stg_subscription_boxes') }}
    
    UNION ALL
    
    SELECT
        'stg_gift_cards' as table_name,
        COUNT(*) as total_rows,
        COUNT(review_id) as review_id_count,
        COUNT(product_id) as product_id_count,
        COUNT(reviewer_id) as reviewer_id_count,
        COUNT(overall_rating) as rating_count
    FROM {{ ref('stg_gift_cards') }}
    
    UNION ALL
    
    SELECT
        'stg_digital_music' as table_name,
        COUNT(*) as total_rows,
        COUNT(review_id) as review_id_count,
        COUNT(product_id) as product_id_count,
        COUNT(reviewer_id) as reviewer_id_count,
        COUNT(overall_rating) as rating_count
    FROM {{ ref('stg_digital_music') }}
)

SELECT
    table_name,
    total_rows,
    review_id_count,
    product_id_count,
    reviewer_id_count,
    rating_count,
    -- Calculate completeness percentages
    (review_id_count::float / total_rows * 100) as review_id_completeness,
    (product_id_count::float / total_rows * 100) as product_id_completeness,
    (reviewer_id_count::float / total_rows * 100) as reviewer_id_completeness,
    (rating_count::float / total_rows * 100) as rating_completeness
FROM completeness_metrics
WHERE 
    review_id_count != total_rows OR
    product_id_count != total_rows OR
    reviewer_id_count != total_rows OR
    rating_count != total_rows