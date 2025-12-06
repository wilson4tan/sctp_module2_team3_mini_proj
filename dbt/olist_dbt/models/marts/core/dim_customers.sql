{{ config(materialized='table') }}

WITH customers AS (
    SELECT 
        customer_id,        -- link to Fact table
        customer_unique_id, -- natural customer key
        customer_city,
        customer_state,
        customer_zip_code_prefix
    FROM {{ ref('stg_customers') }}
),

geo AS (
    SELECT 
        geolocation_zip_code_prefix, 
        AVG(geolocation_lat) AS lat, 
        AVG(geolocation_lng) AS lng
    FROM {{ source('olist_raw', 'raw_geolocation') }}
    GROUP BY geolocation_zip_code_prefix
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix,
    g.lat,
    g.lng
FROM customers c
LEFT JOIN geo g 
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
