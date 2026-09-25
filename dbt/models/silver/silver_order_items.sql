{{ config(unique_key='order_item_key') }}

WITH parsed AS (
    SELECT
        COALESCE(after.order_id, before.order_id) AS order_id,
        COALESCE(after.item_id, before.item_id)   AS item_id,
        after.product_id AS product_id,
        after.quantity   AS quantity,
        after.list_price AS list_price,
        after.discount   AS discount,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM {{ source('bronze', 'order_items') }}
    WHERE {{ cdc_new_events() }}
),

ranked AS (
    SELECT
        *,
        op = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, item_id
            ORDER BY lsn DESC NULLS LAST, ts_ms DESC
        ) AS rn
    FROM parsed
    WHERE order_id IS NOT NULL AND item_id IS NOT NULL
)

SELECT
    CONCAT(order_id, '-', item_id) AS order_item_key,
    order_id,
    item_id,
    product_id,
    quantity,
    CAST(list_price AS DECIMAL(10, 2)) AS list_price,
    CAST(discount AS DECIMAL(4, 2)) AS discount,
    CAST(quantity * list_price * (1 - discount) AS DECIMAL(12, 2)) AS line_revenue,
    ts_ms      AS _cdc_ts_ms,
    is_deleted AS _is_deleted
FROM ranked
WHERE rn = 1
