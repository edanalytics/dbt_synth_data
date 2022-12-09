-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
{{ synth_table(
  rows = var('num_schools')*var('avg_periods_per_school'),
  columns = [
    synth_column_primary_key(name='k_class_period'),
    synth_column_foreign_key(name='k_school', table='dim_school', column='k_school'),
    synth_column_values(name='tenant_code', values=var('tenant_codes')),
    synth_column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    synth_column_integer(name='period_num', min=1, max=9, distribution='uniform'),
    synth_column_expression(name='class_period_name', expression="'Period ' || period_num", type='varchar'),
    synth_column_boolean(name='is_official_attendance_period', pct_true=0.9),
    synth_column_expression(name='start_time', expression="(7+period_num)::varchar || ':00'", type='varchar'),
    synth_column_values(name='period_duration', values=['15', '25', '30', '45', '55']),
    synth_column_expression(name='end_time', expression="(7+period_num)::varchar || ':' || period_duration", type='varchar'),
  ]
) }}
{{ synth_add_cleanup_hook("alter table {{this}} drop column period_num") or "" }}
{{ config(post_hook=synth_get_post_hooks())}}