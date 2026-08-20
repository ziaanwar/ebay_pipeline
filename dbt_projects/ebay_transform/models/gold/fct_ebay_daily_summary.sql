WITH silver_data AS (
    SELECT * FROM {{ ref('stg_ebay_items') }}
)

SELECT
    DATE(file_last_modified) AS listing_date,
    COUNT(DISTINCT item_id) AS total_listings,
    ROUND(AVG(price), 2) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    currency
FROM silver_data
WHERE price IS NOT NULL
GROUP BY 1, 6
ORDER BY listing_date DESC