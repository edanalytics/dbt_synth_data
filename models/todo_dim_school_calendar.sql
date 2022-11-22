{#
    k_school_calendar,
    k_school,
    tenant_code,
    school_year,
    calendar_code,
    calendar_type,
    applicable_grade_levels_array
#}
{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = var('num_schools'),
  columns = [
    dbt_synth.column_primary_key(name='k_session'),
    dbt_synth.column_foreign_key(name='k_school'),
    dbt_synth.column_lookup(name='tenant_code', value_col='k_school', lookup_table='dim_school', from_col='k_school', to_col='tenant_code'),
    dbt_synth.column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    dbt_synth.column_values(name='session_name', values=var('session_types')),
  ]
) }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}
