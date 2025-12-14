WITH products AS (
    SELECT * FROM {{ source('olist_raw', 'raw_products') }}
),

translations AS (
    SELECT * FROM {{ source('olist_raw', 'raw_category_translation') }}
)

SELECT
    JSON_EXTRACT_SCALAR(p.data, '$.product_id') AS product_id,

    -- Category Logic
    COALESCE(
        JSON_EXTRACT_SCALAR(t.data, '$.product_category_name_english'), 
        JSON_EXTRACT_SCALAR(p.data, '$.product_category_name'), 
        'Unknown'
    ) AS product_category_name,

    -- Specs
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_name_lenght'), '') AS INT64) AS product_name_length,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_description_lenght'), '') AS INT64) AS product_description_length,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_photos_qty'), '') AS INT64) AS product_photos_qty,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_weight_g'), '') AS INT64) AS product_weight_g,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_length_cm'), '') AS INT64) AS product_length_cm,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_height_cm'), '') AS INT64) AS product_height_cm,
    CAST(NULLIF(JSON_EXTRACT_SCALAR(p.data, '$.product_width_cm'), '') AS INT64) AS product_width_cm

FROM products p
LEFT JOIN translations t 
    ON JSON_EXTRACT_SCALAR(p.data, '$.product_category_name') = JSON_EXTRACT_SCALAR(t.data, '$.product_category_name')