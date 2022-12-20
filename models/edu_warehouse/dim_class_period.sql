-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}
select
    {{ synth_primary_key() }} as k_class_period,
    {{ synth_foreign_key(table='dim_school', column='k_school') }} as k_school,
    {{ synth_values(values=var('tenant_codes')) }} as tenant_code,
    {{ synth_integer(min=var('min_school_year'), max=var('max_school_year'), distribution='uniform') }} as school_year,
    {{ synth_integer(min=1, max=9, distribution='uniform') }} as period_num,
    {{ synth_expression(expression="'Period ' || period_num", type='varchar') }} as class_period_name,
    {{ synth_boolean(pct_true=0.9) }} as is_official_attendance_period,
    {{ synth_expression(expression="(7+period_num)::varchar || ':00'", type='varchar') }} as start_time,
    {{ synth_values(values=['15', '25', '30', '45', '55']) }} as period_duration,
    {{ synth_expression(expression="(7+period_num)::varchar || ':' || period_duration", type='varchar') }} as end_time
from {{ synth_table(rows=var('num_schools')*var('avg_periods_per_school')) }}

{{ synth_add_cleanup_hook("alter table {{this}} drop column period_num") or "" }}
{{ config(post_hook=synth_get_post_hooks())}}