WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_reviews') }}
)

SELECT
    -- IDs
    review_id,
    order_id, -- Foreign key to order

    -- Metrics
    CAST(review_score AS INT64) AS review_score,

    -- Text Data
    review_comment_title,
    review_comment_message,

    -- Timestamps
    CAST(review_creation_date AS TIMESTAMP) AS review_created_at,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answered_at

FROM source