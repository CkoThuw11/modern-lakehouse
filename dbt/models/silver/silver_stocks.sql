{{ config(unique_key='store_product_key') }}

WITH parsed AS (
    SELECT
        COALESCE(after.store_id, before.store_id)     AS store_id,
        COALESCE(after.product_id, before.product_id) AS product_id,
        after.quantity AS quantity,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM {{ source('bronze', 'stocks') }}
    WHERE {{ cdc_new_events() }}
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
    quantity,
    ts_ms      AS _cdc_ts_ms,
    is_deleted AS _is_deleted
FROM ranked
WHERE rn = 1
