WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_payments') }}
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS NUMERIC) as payment_value
FROM source