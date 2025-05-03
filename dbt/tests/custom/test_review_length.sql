WITH reviews AS (
    SELECT * FROM {{ ref('fact_reviews') }}
)

SELECT
    review_id,
    review_length
FROM reviews
WHERE review_length > 10000  