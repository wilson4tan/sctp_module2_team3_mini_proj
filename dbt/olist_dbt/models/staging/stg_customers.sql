WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_customers') }}
)

SELECT
    customer_id,        -- PK
    customer_unique_id, -- PK unique ID
    customer_zip_code_prefix,
    customer_city,
    customer_state

FROM source