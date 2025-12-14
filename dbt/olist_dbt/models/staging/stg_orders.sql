WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_orders') }}
)

SELECT
    -- Extract fields from the data JSON
    JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
    JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
    JSON_EXTRACT_SCALAR(data, '$.order_status') AS order_status,

    -- Extract the time string and convert it to TIMESTAMP
    -- If it is an empty string '', NULLIF will convert it to NULL to avoid CAST errors
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_purchase_timestamp'), '') AS TIMESTAMP) AS order_purchase_at,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_approved_at'), '') AS TIMESTAMP) AS order_approved_at,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_delivered_carrier_date'), '') AS TIMESTAMP) AS order_delivered_carrier_at,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_delivered_customer_date'), '') AS TIMESTAMP) AS order_delivered_customer_at,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_estimated_delivery_date'), '') AS TIMESTAMP) AS order_estimated_delivery_at

FROM source