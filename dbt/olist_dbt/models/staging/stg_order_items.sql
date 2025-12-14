WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_items') }}
)

SELECT
    JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
    CAST(JSON_EXTRACT_SCALAR(data, '$.order_item_id') AS INT64) AS order_item_id,
    JSON_EXTRACT_SCALAR(data, '$.product_id') AS product_id,
    JSON_EXTRACT_SCALAR(data, '$.seller_id') AS seller_id,
    
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.shipping_limit_date'), '') AS TIMESTAMP) AS shipping_limit_date,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.price'), '') AS NUMERIC) AS price,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.freight_value'), '') AS NUMERIC) AS freight_value

FROM source