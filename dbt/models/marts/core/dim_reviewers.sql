{{
  config(
    materialized='table',
    tags=['core', 'dimension']
  )
}}

WITH reviewers AS (
    SELECT
        reviewer_id,
        reviewer_name,
        COUNT(DISTINCT product_id) as products_reviewed,
        AVG(overall_rating) as avg_rating,
        SUM(helpful_vote) as total_helpful_votes,
        MIN(review_time) as first_review_date,
        MAX(review_time) as last_review_date,
        CURRENT_TIMESTAMP as _loaded_at
    FROM {{ ref('fact_reviews') }}
    GROUP BY 1, 2
),

final AS (
    SELECT
        reviewer_id,
        reviewer_name,
        products_reviewed,
        avg_rating,
        total_helpful_votes,
        first_review_date,
        last_review_date,
        -- Calculate reviewer tenure in days
        DATE_PART('day', last_review_date - first_review_date) as reviewer_tenure_days,
        -- Categorize reviewers
        CASE
            WHEN products_reviewed > 10 THEN 'power_reviewer'
            WHEN products_reviewed > 3 THEN 'active_reviewer'
            ELSE 'casual_reviewer'
        END as reviewer_type,
        _loaded_at
    FROM reviewers
)

SELECT * FROM final