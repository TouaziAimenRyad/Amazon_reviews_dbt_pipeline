{{
  config(
    materialized='table',
    tags=['business', 'analysis']
  )
}}

WITH reviewer_activity AS (
    SELECT
        r.reviewer_id,
        dr.reviewer_name,
        dr.reviewer_type,
        COUNT(DISTINCT r.product_id) as unique_products_reviewed,
        COUNT(r.review_id) as total_reviews,
        AVG(r.overall_rating) as avg_rating,
        SUM(r.helpful_vote) as total_helpful_votes,
        AVG(r.review_length) as avg_review_length,
        MIN(r.review_time) as first_review_date,
        MAX(r.review_time) as last_review_date
    FROM {{ ref('fact_reviews') }} r
    JOIN {{ ref('dim_reviewers') }} dr ON r.reviewer_id = dr.reviewer_id
    GROUP BY 1, 2, 3
),

reviewer_impact AS (
    SELECT
        reviewer_id,
        CORR(overall_rating, helpful_vote) as rating_helpfulness_correlation,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY helpful_vote) as median_helpful_votes
    FROM {{ ref('fact_reviews') }}
    GROUP BY 1
)

SELECT
    ra.*,
    ri.rating_helpfulness_correlation,
    ri.median_helpful_votes,
    -- Calculate derived metrics
    total_helpful_votes::float / NULLIF(total_reviews, 0) as avg_helpful_votes_per_review,
    CASE
        WHEN rating_helpfulness_correlation > 0.3 THEN 'high_impact'
        WHEN rating_helpfulness_correlation > 0.1 THEN 'medium_impact'
        ELSE 'low_impact'
    END as reviewer_impact_category
FROM reviewer_activity ra
JOIN reviewer_impact ri ON ra.reviewer_id = ri.reviewer_id