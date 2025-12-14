WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_sellers') }}
)

SELECT
    JSON_EXTRACT_SCALAR(data, '$.seller_id') AS seller_id,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.seller_zip_code_prefix'), '') AS INT64) AS seller_zip_code_prefix,
    JSON_EXTRACT_SCALAR(data, '$.seller_city') AS seller_city,
    JSON_EXTRACT_SCALAR(data, '$.seller_state') AS seller_state

FROM source