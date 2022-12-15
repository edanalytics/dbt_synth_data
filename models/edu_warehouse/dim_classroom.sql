-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
{{ synth_table(
  rows = var('num_schools')*var('avg_classrooms_per_school'),
  columns = [
    synth_column_primary_key(name='k_classroom'),
    synth_column_foreign_key(name='k_school', table='dim_school', column='k_school'),
    synth_column_values(name='tenant_code', values=var('tenant_codes')),
    synth_column_value(name='classroom_id_code', value='Room        '),
    synth_column_integer(name='classroom_id_num', min=100, max=800, distribution='uniform'),
    synth_column_value(name='maximum_seating', value=100),
    synth_column_integer(name='optimum_seating', min=var('classroom_min_seating'), max=var('classroom_max_seating')-10, distribution='uniform'),
  ]
) }}

synth_add_update_hook("update {{this}} set classroom_id_code = 'Room ' || classroom_id_num") or "" }}
synth_add_cleanup_hook("alter table {{this}} drop column classroom_id_num") or "" }}
synth_add_update_hook("update {{this}} set maximum_seating = optimum_seating + 10") or "" }}

{{ config(post_hook=synth_get_post_hooks())}}