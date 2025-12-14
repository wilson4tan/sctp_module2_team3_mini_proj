WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

geolocation AS (
    SELECT * FROM {{ ref('stg_geolocation') }}
),

-- Zip codes in the geographic location table are not unique (the same zip code may have multiple latitude/longitude points)
-- We need to deduplicate, usually taking the first record for each zip code or an average value
geo_unique AS (
    SELECT
        geolocation_zip_code_prefix,
        ANY_VALUE(geolocation_lat) as geolocation_lat,
        ANY_VALUE(geolocation_lng) as geolocation_lng,
        ANY_VALUE(geolocation_city) as geolocation_city,
        ANY_VALUE(geolocation_state) as geolocation_state
    FROM geolocation
    GROUP BY geolocation_zip_code_prefix
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    
    -- 加入地理位置信息
    g.geolocation_lat,
    g.geolocation_lng

FROM customers c
LEFT JOIN geo_unique g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix