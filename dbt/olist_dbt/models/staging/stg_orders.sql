WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_orders') }}
)

SELECT
    -- ID
    order_id,
    customer_id,

    -- Status
    order_status,

    -- Timestamps (convert BigQuery to TIMESTAMP)
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_at,
    CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_at,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_at,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_at

FROM source