{{ config(
    materialized='incremental',
    file_format='iceberg',
    schema='silver',
    incremental_strategy='merge',
    unique_key='customer_id',
    on_schema_change='append_new_columns',
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted"
) }}

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
    FROM lakehouse.bronze.customers
    {% if is_incremental() %}
    -- Only events newer than silver's watermark, minus a lookback for late bronze
    -- commits. ts_ms (arrival order), not LSN: a lower LSN can arrive after a higher one.
    WHERE ts_ms > (SELECT COALESCE(MAX(_cdc_ts_ms), 0) FROM {{ this }}) - {{ var('cdc_lookback_ms') }}
    {% endif %}
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
