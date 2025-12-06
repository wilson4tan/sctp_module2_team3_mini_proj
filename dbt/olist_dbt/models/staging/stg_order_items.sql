WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_order_items') }}
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS NUMERIC) AS price,
    CAST(freight_value AS NUMERIC) AS freight_value

FROM source