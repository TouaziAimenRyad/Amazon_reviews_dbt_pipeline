{{
  config(
    materialized='incremental',
    tags=['business', 'analysis'],
    unique_key='product_id'
  )
}}

WITH latest_reviews AS (
    SELECT 
        product_id,
        MAX(review_time) as latest_review_time
    FROM {{ ref('fact_reviews') }}
    {% if is_incremental() %}
    WHERE review_time >= (SELECT MAX(last_review_date) FROM {{ this }})
    {% endif %}
    GROUP BY 1
),

product_stats AS (
    SELECT
        p.product_id,
        p.product_category,
        COUNT(r.review_id) as review_count,
        AVG(r.overall_rating) as avg_rating,
        SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) as positive_reviews,
        SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
        SUM(r.helpful_vote) as total_helpful_votes,
        MIN(r.review_time) as first_review_date,
        MAX(r.review_time) as last_review_date,
        AVG(r.review_length) as avg_review_length
    FROM {{ ref('dim_products') }} p
    JOIN {{ ref('fact_reviews') }} r ON p.product_id = r.product_id
    {% if is_incremental() %}
    JOIN latest_reviews lr ON r.product_id = lr.product_id
    WHERE r.review_time >= lr.latest_review_time
    {% endif %}
    GROUP BY 1, 2
),

monthly_trends AS (
    SELECT
        product_id,
        review_month,
        monthly_avg_rating,
        LAG(monthly_avg_rating) OVER (PARTITION BY product_id ORDER BY review_month) as previous_month_rating
    FROM (
        SELECT
            product_id,
            DATE_TRUNC('month', review_time) as review_month,
            AVG(overall_rating) as monthly_avg_rating
        FROM {{ ref('fact_reviews') }}
        {% if is_incremental() %}
        WHERE review_time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 month')
        {% endif %}
        GROUP BY 1, 2
    ) monthly_data
),

latest_trends AS (
    SELECT
        product_id,
        monthly_avg_rating as latest_month_rating,
        previous_month_rating
    FROM (
        SELECT
            product_id,
            monthly_avg_rating,
            previous_month_rating,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY review_month DESC) as rn
        FROM monthly_trends
    ) ranked
    WHERE rn = 1
)

SELECT
    ps.*,
    -- Calculate derived metrics
    positive_reviews::float / NULLIF(review_count, 0) as positive_review_ratio,
    negative_reviews::float / NULLIF(review_count, 0) as negative_review_ratio,
    total_helpful_votes::float / NULLIF(review_count, 0) as avg_helpful_votes_per_review,
    -- Add trend analysis
    lt.latest_month_rating,
    lt.previous_month_rating
FROM product_stats ps
LEFT JOIN latest_trends lt ON ps.product_id = lt.product_id