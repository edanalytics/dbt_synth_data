-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
select
    {{ synth_primary_key() }} as k_classroom,
    {{ synth_foreign_key(table='dim_school', column='k_school') }} as k_school,
    {{ synth_values(values=var('tenant_codes')) }} as tenant_code,
    {{ synth_value(value='Room        ') }} as classroom_id_code,
    {{ synth_integer(min=100, max=800, distribution='uniform') }} as classroom_id_num,
    {{ synth_value(value=100) }} as maximum_seating,
    {{ synth_integer(min=var('classroom_min_seating'), max=var('classroom_max_seating')-10) }} as optimum_seating
from {{ synth_table(rows=var('num_schools')*var('avg_classrooms_per_school')) }}

{{ synth_add_update_hook("update {{this}} set classroom_id_code = 'Room ' || classroom_id_num") or "" }}
{{ synth_add_cleanup_hook("alter table {{this}} drop column classroom_id_num") or "" }}
{{ synth_add_update_hook("update {{this}} set maximum_seating = optimum_seating + 10") or "" }}

{{ config(post_hook=synth_get_post_hooks())}}