WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_sellers') }}
)

SELECT
    seller_id, -- Primary key
    seller_zip_code_prefix,
    seller_city,
    seller_state

FROM source