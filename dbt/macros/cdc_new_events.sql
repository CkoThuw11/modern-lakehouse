{#
  Filter for incremental silver models: only bronze events newer than silver's
  watermark, minus a lookback that re-reads recent events so late bronze commits are
  still picked up. Watermark on ts_ms (arrival order), not LSN: Postgres emits
  transactions in commit order, so a lower LSN can arrive after a higher one.
  Re-reading events is safe because the merge is idempotent.
#}
{% macro cdc_new_events() -%}
    {%- if is_incremental() -%}
    ts_ms > (SELECT COALESCE(MAX(_cdc_ts_ms), 0) FROM {{ this }}) - {{ var('cdc_lookback_ms') }}
    {%- else -%}
    TRUE
    {%- endif -%}
{%- endmacro %}
