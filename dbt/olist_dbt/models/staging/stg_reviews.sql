WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_reviews') }}
)

SELECT
    JSON_EXTRACT_SCALAR(data, '$.review_id') AS review_id,
    JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.review_score'), '') AS INT64) AS review_score,
    JSON_EXTRACT_SCALAR(data, '$.review_comment_title') AS review_comment_title,
    JSON_EXTRACT_SCALAR(data, '$.review_comment_message') AS review_comment_message,
    
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.review_creation_date'), '') AS TIMESTAMP) AS review_created_at,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.review_answer_timestamp'), '') AS TIMESTAMP) AS review_answered_at

FROM source