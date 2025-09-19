{% macro coerce_to_timestamp(col, fallback=None) -%}
-- Macro: coerce_to_timestamp
-- Safely attempt to coerce a raw column (string or numeric) into a TIMESTAMP_NTZ.
-- Attempts (in order):
-- 1) try_to_timestamp_ntz with common format
-- 2) try_to_timestamp_ntz generic
-- 3) if numeric and looks like milliseconds since epoch -> to_timestamp_ntz(col/1000)
-- 4) if string of 10-13 digits -> cast to double and treat as ms -> to_timestamp_ntz(...)
-- 5) optional fallback column cast to timestamp_ntz
(
  coalesce(
    try_to_timestamp_ntz({{ col }}, 'MM/DD/YYYY HH24:MI'),
    try_to_timestamp_ntz({{ col }}),
    -- if the column can be cast to numeric and is large (likely ms since epoch)
    case when try_cast({{ col }} as double) is not null and try_cast({{ col }} as double) > 10000000000
      then dateadd(second, floor(try_cast({{ col }} as double) / 1000), to_timestamp_ntz('1970-01-01'))
    end,
    -- if the column is a string of 10-13 digits, treat as ms
    case when regexp_like(cast({{ col }} as varchar), '^[0-9]{10,13}$')
      then dateadd(second, floor(cast(cast({{ col }} as double) as double) / 1000), to_timestamp_ntz('1970-01-01'))
    end
    {% if fallback %}, cast({{ fallback }} as timestamp_ntz) {% endif %}
  )
)
{%- endmacro %}
