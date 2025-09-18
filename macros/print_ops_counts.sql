{% macro print_ops_counts() -%}
{%- set sql -%}
select mart_name, row_count from {{ ref('mart_row_counts') }}
{%- endset -%}

{% set results = run_query(sql) %}
{% if not results %}
  {{ log("print_ops_counts: run_query returned no results", info=True) }}
{% else %}
  {# results is an agate table; iterate rows #}
  {% for row in results.rows %}
    {{ log("ops: " ~ row[0] ~ " => " ~ row[1] ~ " rows", info=True) }}
  {% endfor %}
{% endif %}

{%- endmacro %}
