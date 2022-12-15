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
{{ synth_table(
  rows = var('num_schools'),
  columns = [
    synth_column_primary_key(name='k_session'),
    synth_column_foreign_key(name='k_school'),
    synth_column_lookup(name='tenant_code', value_col='k_school', lookup_table='dim_school', from_col='k_school', to_col='tenant_code'),
    synth_column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    synth_column_values(name='session_name', values=var('session_types')),
  ]
) }}
{{ config(post_hook=synth_get_post_hooks())}}
