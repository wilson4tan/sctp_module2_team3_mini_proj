WITH products AS (
    SELECT * 
    FROM {{ source('olist_raw', 'raw_products') }}
),

translations AS (
    -- Before using this LEFT JOIN, make sure the translation table is loaded.
    -- If it is not available yet, you can temporarily comment out the JOIN section.
    SELECT * 
    FROM {{ source('olist_raw', 'raw_category_translation') }}
)

SELECT
    p.product_id,

    -- Prefer the English category name; if it's null, use the original.
    -- If both are null, default to 'Unknown'.
    COALESCE(
        t.product_category_name_english, 
        p.product_category_name, 
        'Unknown'
    ) AS product_category_name,

    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

FROM products p
LEFT JOIN translations t 
    ON p.product_category_name = t.product_category_name
