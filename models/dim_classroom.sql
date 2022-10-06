-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
{{ dbt_synth.table(
  rows = var('num_schools')*var('avg_classrooms_per_school'),
  columns = [
    dbt_synth.column_primary_key(name='k_classroom'),
    dbt_synth.column_foreign_key(name='k_school', table='dim_school', column='k_school'),
    dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
    dbt_synth.column_value(name='classroom_id_code', value='Room        '),
    dbt_synth.column_integer(name='classroom_id_num', min=100, max=800, distribution='uniform'),
    dbt_synth.column_value(name='maximum_seating', value=100),
    dbt_synth.column_integer(name='optimum_seating', min=var('classroom_min_seating'), max=var('classroom_max_seating')-10, distribution='uniform'),
  ]
) }}

{{ dbt_synth.add_update_hook("update {{this}} set classroom_id_code = 'Room ' || classroom_id_num") or "" }}
{{ dbt_synth.add_cleanup_hook("alter table {{this}} drop column classroom_id_num") or "" }}
{{ dbt_synth.add_update_hook("update {{this}} set maximum_seating = optimum_seating + 10") or "" }}

{{ config(post_hook=dbt_synth.get_post_hooks())}}