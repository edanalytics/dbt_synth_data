{{ config(materialized='table') }}

{% if target.type=='postgres' %}
{% set calendar_event_array_expression = 'ARRAY[calendar_event]' %}
{% elif target.type=='snowflake' %}
{% set calendar_event_array_expression = 'ARRAY_CONSTRUCT(calendar_event)' %}
{% else %}
{% set calendar_event_array_expression = '???' %}
{% endif %}

with 
{% for i in range(var('max_school_year')-var('min_school_year')+1) %}
{% set year = (var('min_school_year') + i)|string %}
synth{{year}} as (
    {{ dbt_synth.table(
    rows = 296,
    columns = [
        dbt_synth.column_primary_key(name='k_calendar_date'),
        dbt_synth.column_value(name='k_school_calendar', value='ComingSoon'),
        dbt_synth.column_foreign_key(name='k_lea', table='dim_lea', column='k_lea'),
        dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
        dbt_synth.column_value(name='calendar_code', value='Normal'),
        dbt_synth.column_date_sequence(name='calendar_date', start_date=year+'-08-10'),
        dbt_synth.column_value(name='school_year', value=year|int),
        dbt_synth.column_expression(name='week_day', expression="case extract(dow from calendar_date) when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue' when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri' when 6 then 'Sat' end"),
        dbt_synth.column_boolean(name='is_school_day', pct_true=0.96),
        dbt_synth.column_mapping(name='calendar_event', expression='is_school_day', mapping=({ true:'Instructional day', false:'Non-instructional day' })),
        dbt_synth.column_expression(name='calendar_event_array', expression=calendar_event_array_expression, type='array'),
        dbt_synth.column_integer_sequence(name='day_of_school_year', step=1),
        dbt_synth.column_expression(name='week_of_calendar_year', type='int', expression="DATE_PART('week', calendar_date)::int"),
        dbt_synth.column_expression(name='week_of_school_year', type='int', expression='case when not is_school_day then null when week_of_calendar_year::int >= 33 then week_of_calendar_year::int - 33 else week_of_calendar_year::int + 52 - 33 end'),
        dbt_synth.column_geopoint(name='lat_long'),
    ]
    ) }}
),
{% endfor %}

{# dbt_synth.column_foreign_key(name='k_school_calendar', table='dim_school_calendar', column='k_school_calendar'), #}
{# dbt_synth.column_foreign_key(name='k_school', table='dim_school', column='k_school'), #}

stacked as (
    {% for i in range(var('max_school_year')-var('min_school_year')+1) %}
    {% set year = var('min_school_year') + i %}
    select * from synth{{year}}
    {% if not loop.last %}union{% endif %}
{% endfor %}
)
select *
from stacked

-- get rid of weekend days:
{{ dbt_synth.add_cleanup_hook("delete from {{this}} where week_day='Sat' or week_day='Sun'") or "" }}


{{ config(post_hook=dbt_synth.get_post_hooks())}}