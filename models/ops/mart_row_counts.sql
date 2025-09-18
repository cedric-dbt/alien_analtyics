{{ config(materialized='table', description='Counts for key mart models for quick inspection', tags=['ops']) }}

select
  'mart_comprehensive_analysis_summary_simple' as mart_name,
  (select count(*) from {{ ref('mart_comprehensive_analysis_summary_simple') }}) as row_count
union all
select
  'mart_ufo_dashboard_summary' as mart_name,
  (select count(*) from {{ ref('mart_ufo_dashboard_summary') }}) as row_count
union all
select
  'mart_ufo_debunking_analysis_simple' as mart_name,
  (select count(*) from {{ ref('mart_ufo_debunking_analysis_simple') }}) as row_count
union all
select
  'mart_ufo_debunking_analysis' as mart_name,
  (select count(*) from {{ ref('mart_ufo_debunking_analysis') }}) as row_count
union all
select
  'mart_ufo_sightings_by_country_time' as mart_name,
  (select count(*) from {{ ref('mart_ufo_sightings_by_country_time') }}) as row_count
union all
select
  'mart_ufo_top_locations' as mart_name,
  (select count(*) from {{ ref('mart_ufo_top_locations') }}) as row_count
