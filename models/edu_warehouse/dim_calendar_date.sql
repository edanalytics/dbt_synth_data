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
    {{ synth_table(
    rows = 296,
    columns = [
        synth_column_primary_key(name='k_calendar_date'),
        synth_column_value(name='k_school_calendar', value='ComingSoon'),
        synth_column_foreign_key(name='k_lea', table='dim_lea', column='k_lea'),
        synth_column_values(name='tenant_code', values=var('tenant_codes')),
        synth_column_value(name='calendar_code', value='Normal'),
        synth_column_date_sequence(name='calendar_date', start_date=year+'-08-10'),
        synth_column_value(name='school_year', value=year|int),
        synth_column_expression(name='week_day', expression="case extract(dow from calendar_date) when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue' when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri' when 6 then 'Sat' end"),
        synth_column_boolean(name='is_school_day', pct_true=0.96),
        synth_column_mapping(name='calendar_event', expression='is_school_day', mapping=({ true:'Instructional day', false:'Non-instructional day' })),
        synth_column_expression(name='calendar_event_array', expression=calendar_event_array_expression, type='array'),
        synth_column_integer_sequence(name='day_of_school_year', step=1),
        synth_column_expression(name='week_of_calendar_year', type='int', expression="DATE_PART('week', calendar_date)::int"),
        synth_column_expression(name='week_of_school_year', type='int', expression='case when not is_school_day then null when week_of_calendar_year::int >= 33 then week_of_calendar_year::int - 33 else week_of_calendar_year::int + 52 - 33 end'),
        synth_column_geopoint(name='lat_long'),
    ]
    ) }}
),
{% endfor %}

{# synth_column_foreign_key(name='k_school_calendar', table='dim_school_calendar', column='k_school_calendar'), #}
{# synth_column_foreign_key(name='k_school', table='dim_school', column='k_school'), #}

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
{{ synth_add_cleanup_hook("delete from {{this}} where week_day='Sat' or week_day='Sun'") or "" }}


{{ config(post_hook=synth_get_post_hooks())}}