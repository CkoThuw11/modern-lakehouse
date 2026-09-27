{{ config(
    materialized='incremental',
    file_format='iceberg',
    schema='silver',
    incremental_strategy='merge',
    unique_key='store_product_key',
    on_schema_change='append_new_columns',
    post_hook="DELETE FROM {{ this }} WHERE _is_deleted"
) }}

WITH parsed AS (
    SELECT
        COALESCE(after.store_id, before.store_id)     AS store_id,
        COALESCE(after.product_id, before.product_id) AS product_id,
        after.quantity AS quantity,
        op,
        source.lsn AS lsn,
        ts_ms
    FROM lakehouse.bronze.stocks
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
