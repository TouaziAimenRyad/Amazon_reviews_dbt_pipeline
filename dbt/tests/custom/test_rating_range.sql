WITH reviews AS (
    SELECT * FROM {{ ref('fact_reviews') }}
)

SELECT
    review_id,
    overall_rating
FROM reviews
WHERE overall_rating < 1 OR overall_rating > 5