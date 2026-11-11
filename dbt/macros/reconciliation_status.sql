{#
  FR-R3: classify one product-minute. Expects the columns produced by
  int_reconciliation_1m: has_stream, has_official, stream_* and official_* OHLCV.
  Order matters: missing sides first, then prices, then volume tolerance (FR-R5).
#}
{% macro reconciliation_status() -%}
    case
        when not has_official then 'MISSING_IN_BATCH'
        when not has_stream then 'MISSING_IN_STREAM'
        when stream_open <> official_open
          or stream_high <> official_high
          or stream_low <> official_low
          or stream_close <> official_close then 'PRICE_MISMATCH'
        when abs(stream_volume - official_volume)
             > official_volume * {{ var('volume_tolerance_pct') }} / 100 then 'VOLUME_DRIFT'
        else 'MATCH'
    end
{%- endmacro %}
