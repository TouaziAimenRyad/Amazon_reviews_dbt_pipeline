WITH vote_stats AS (
    SELECT
        product_id,
        AVG(helpful_vote) as avg_helpful_votes,
        STDDEV(helpful_vote) as stddev_helpful_votes,
        COUNT(*) as review_count
    FROM {{ ref('fact_reviews') }}
    GROUP BY 1
),

outliers AS (
    SELECT
        r.product_id,
        r.review_id,
        r.helpful_vote,
        vs.avg_helpful_votes,
        vs.stddev_helpful_votes,
        (r.helpful_vote - vs.avg_helpful_votes) / NULLIF(vs.stddev_helpful_votes, 0) as z_score
    FROM {{ ref('fact_reviews') }} r
    JOIN vote_stats vs ON r.product_id = vs.product_id
    WHERE vs.review_count > 10  -- Only check products with enough reviews
)

SELECT
    product_id,
    review_id,
    helpful_vote,
    avg_helpful_votes,
    stddev_helpful_votes,
    z_score
FROM outliers
WHERE ABS(z_score) > 3  -- Flag extreme outliers