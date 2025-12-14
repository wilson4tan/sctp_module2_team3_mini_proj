WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_payments') }}
)

SELECT
    JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.payment_sequential'), '') AS INT64) AS payment_sequential,
    JSON_EXTRACT_SCALAR(data, '$.payment_type') AS payment_type,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.payment_installments'), '') AS INT64) AS payment_installments,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.payment_value'), '') AS NUMERIC) AS payment_value

FROM source