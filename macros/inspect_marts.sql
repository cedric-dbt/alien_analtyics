{% macro inspect_marts() -%}
{#
Run simple COUNT(*) queries against the two mart models and return a small text summary.
This macro is intended to be invoked via `dbt run-operation inspect_marts`.
#}
{%- set db = 'CEDRIC_TURNER_DEMO' -%}
{%- set schema = 'UFO_MART' -%}
{% set sql1 %}
select count(*) as cnt from {{ db }}.{{ schema }}.mart_crashes_by_day
{% endset %}

{% set sql2 %}
select count(*) as cnt from {{ db }}.{{ schema }}.mart_ufo_with_weather
{% endset %}

{%- set res1 = run_query(sql1) -%}
{%- set res2 = run_query(sql2) -%}

{%- set table1 = res1.table if res1 is not none else none -%}
{%- set table2 = res2.table if res2 is not none else none -%}

{% set c1 = (table1.rows[0][0]) if table1 and table1.rows and table1.rows[0] is not none else 'NULL' %}
{% set c2 = (table2.rows[0][0]) if table2 and table2.rows and table2.rows[0] is not none else 'NULL' %}

{{ log('mart_crashes_by_day row count: ' ~ c1, info=True) }}
{{ log('mart_ufo_with_weather row count: ' ~ c2, info=True) }}

{% do return({'mart_crashes_by_day': c1, 'mart_ufo_with_weather': c2}) -%}
{%- endmacro %}
