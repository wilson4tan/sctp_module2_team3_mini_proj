SELECT 
    order_id,
    payment_type,
    sum(payment_value) as total_payment
FROM {{ ref('stg_payments') }}
GROUP BY 1, 2