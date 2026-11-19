{#
  Classify one product-minute (FR-R3, extended by ADR-012). Expects the columns produced
  by int_reconciliation_1m: has_stream, has_official, stream_complete, and stream_* and
  official_* OHLCV.

  stream_complete means the stream provably has every trade of the minute: its trade ids
  have no internal gap and connect to the neighbouring stream minutes on both sides
  (ADR-002). A disagreement on such a minute is an official-candle error, not ours.
#}
{% macro reconciliation_status() -%}
    case
        when not has_official then
            case when stream_complete then 'OFFICIAL_MISMATCH' else 'MISSING_IN_BATCH' end
        when not has_stream then 'MISSING_IN_STREAM'
        when stream_open = official_open
         and stream_high = official_high
         and stream_low = official_low
         and stream_close = official_close
         and abs(stream_volume - official_volume)
             <= official_volume * {{ var('volume_tolerance_pct') }} / 100 then 'MATCH'
        when stream_complete then 'OFFICIAL_MISMATCH'
        when stream_open <> official_open
          or stream_high <> official_high
          or stream_low <> official_low
          or stream_close <> official_close then 'PRICE_MISMATCH'
        else 'VOLUME_DRIFT'
    end
{%- endmacro %}
