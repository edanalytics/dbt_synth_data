-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = var('num_schools')*var('avg_periods_per_school'),
  columns = [
    dbt_synth.column_primary_key(name='k_class_period'),
    dbt_synth.column_foreign_key(name='k_school', table='dim_school', column='k_school'),
    dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
    dbt_synth.column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    dbt_synth.column_integer(name='period_num', min=1, max=9, distribution='uniform'),
    dbt_synth.column_expression(name='class_period_name', expression="'Period ' || period_num", type='varchar'),
    dbt_synth.column_boolean(name='is_official_attendance_period', pct_true=0.9),
    dbt_synth.column_expression(name='start_time', expression="(7+period_num)::varchar || ':00'", type='varchar'),
    dbt_synth.column_values(name='period_duration', values=['15', '25', '30', '45', '55']),
    dbt_synth.column_expression(name='end_time', expression="(7+period_num)::varchar || ':' || period_duration", type='varchar'),
  ]
) }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column period_num") or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}