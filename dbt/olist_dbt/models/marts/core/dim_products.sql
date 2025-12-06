{{ config(materialized='table') }}

SELECT
    product_id,
    product_category_name AS category_name, -- remane for clarity
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM {{ ref('stg_products') }}