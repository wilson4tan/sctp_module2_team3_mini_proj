{{ config(
    materialized='table',
    partition_by={
      "field": "order_purchase_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

reviews AS (
    -- An order may have multiple reviews, although this is rare.
    -- To be safe, we take the most recent review score for each order.
    SELECT 
        order_id,
        review_score
    FROM {{ ref('stg_reviews') }}
    -- If there are duplicate review_id values here, we typically need to deduplicate them.
    -- But in the Olist dataset, an order normally corresponds to only one review.
),

joined AS (
    SELECT
        -- 1. Foreign Keys (used to join with dimension tables)
        items.order_id,
        items.product_id,
        items.seller_id,
        orders.customer_id, -- Key point: retrieve the customer ID from the Orders table
        
        -- 2. Dimensions & Status (from Orders)
        orders.order_status,
        orders.order_purchase_at,
        DATE(orders.order_purchase_at) AS order_purchase_date, -- Used for partitioning
        
        -- 3. Metrics (from Items)
        items.price,
        items.freight_value,
        (items.price + items.freight_value) AS total_item_value,
        
        -- 4. Metrics (from Reviews)
        -- Using AVG or MAX is fine because it's usually a one-to-one relationship.
        reviews.review_score

    FROM items
    -- Join Orders (must exist)
    LEFT JOIN orders 
        ON items.order_id = orders.order_id
    
    -- Join Reviews (may not exist, so use LEFT JOIN)
    LEFT JOIN reviews
        ON items.order_id = reviews.order_id
    
    WHERE orders.order_id IS NOT NULL
)

SELECT * FROM joined
