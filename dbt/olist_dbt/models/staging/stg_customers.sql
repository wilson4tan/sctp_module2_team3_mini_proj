WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_customers') }}
)

SELECT
    JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
    JSON_EXTRACT_SCALAR(data, '$.customer_unique_id') AS customer_unique_id,
    
    -- Zip code — Even though it’s a number, when it comes out of JSON it is by default a string; later, if needed, it can be CAST to INT64.
    CAST(JSON_EXTRACT_SCALAR(data, '$.customer_zip_code_prefix') AS INT64) AS customer_zip_code_prefix,
    JSON_EXTRACT_SCALAR(data, '$.customer_city') AS customer_city,
    JSON_EXTRACT_SCALAR(data, '$.customer_state') AS customer_state

FROM source