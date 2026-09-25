{{ config(unique_key='customer_id') }}

WITH parsed AS (
    SELECT
        COALESCE(after.customer_id, before.customer_id) AS customer_id,
        after.first_name AS first_name,
        after.last_name  AS last_name,
        after.phone      AS phone,
        after.email      AS email,
        after.street     AS street,
        after.city       AS city,
        after.state      AS state,
        after.zip_code   AS zip_code,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM {{ source('bronze', 'customers') }}
    WHERE {{ cdc_new_events() }}
),

ranked AS (
    SELECT
        *,
        op = 'd' AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY lsn DESC NULLS LAST, ts_ms DESC
        ) AS rn
    FROM parsed
    WHERE customer_id IS NOT NULL
)

SELECT
    customer_id,
    first_name,
    last_name,
    phone,
    email,
    street,
    city,
    state,
    zip_code,
    ts_ms      AS _cdc_ts_ms,
    is_deleted AS _is_deleted
FROM ranked
WHERE rn = 1
