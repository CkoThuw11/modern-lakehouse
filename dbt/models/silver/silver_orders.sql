WITH parsed AS (
    SELECT
        COALESCE(after.order_id, before.order_id) AS order_id,
        after.customer_id    AS customer_id,
        after.order_status   AS order_status,
        after.order_date     AS order_date,
        after.required_date  AS required_date,
        after.shipped_date   AS shipped_date,
        after.store_id       AS store_id,
        after.staff_id       AS staff_id,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM lakehouse.bronze.orders
),

ranked AS (
    SELECT
        *,
        op = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY lsn DESC NULLS LAST, ts_ms DESC
        ) AS rn
    FROM parsed
    WHERE order_id IS NOT NULL
)

SELECT
    order_id,
    customer_id,
    order_status,
    CASE order_status
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'processing'
        WHEN 3 THEN 'rejected'
        WHEN 4 THEN 'completed'
    END AS order_status_name,
    DATE_ADD(DATE'1970-01-01', order_date) AS order_date,
    DATE_ADD(DATE'1970-01-01', required_date) AS required_date,
    CASE
        WHEN shipped_date IS NULL THEN NULL
        ELSE DATE_ADD(DATE'1970-01-01', shipped_date)
    END AS shipped_date,
    store_id,
    staff_id
FROM ranked
WHERE rn = 1 AND NOT is_deleted
