WITH raw_data AS (
    SELECT 
        $1 AS json_data,
        METADATA$FILENAME AS file_name,
        METADATA$FILE_LAST_MODIFIED AS file_last_modified
    FROM @{{ source('bronze_s3', 's3_ebay_stage') }}
),

flattened_items AS (
    SELECT
        r.file_name,
        r.file_last_modified,
        item.value:itemId::STRING AS item_id,
        item.value:title::STRING AS title,
        item.value:price:value::NUMBER(10,2) AS price,
        item.value:price:currency::STRING AS currency,
        item.value:condition::STRING AS condition,
        item.value:itemWebUrl::STRING AS item_url,
        item.value:seller:username::STRING AS seller_username,
        item.value:seller:feedbackPercentage::FLOAT AS seller_feedback_pct
    FROM raw_data r,
    LATERAL FLATTEN(input => r.json_data:itemSummaries) item
)

SELECT * FROM flattened_items