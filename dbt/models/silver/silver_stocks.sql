WITH parsed AS (
    SELECT
        COALESCE(after.store_id, before.store_id)     AS store_id,
        COALESCE(after.product_id, before.product_id) AS product_id,
        after.quantity AS quantity,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM lakehouse.bronze.stocks
),

ranked AS (
    SELECT
        *,
        op = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY store_id, product_id
            ORDER BY lsn DESC NULLS LAST, ts_ms DESC
        ) AS rn
    FROM parsed
    WHERE store_id IS NOT NULL AND product_id IS NOT NULL
)

SELECT
    CONCAT(store_id, '-', product_id) AS store_product_key,
    store_id,
    product_id,
    quantity
FROM ranked
WHERE rn = 1 AND NOT is_deleted
