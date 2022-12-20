{{ config(materialized='table') }}

{% if target.type=='postgres' %}
{% set calendar_event_array_expression = 'ARRAY[calendar_event]' %}
{% elif target.type=='snowflake' %}
{% set calendar_event_array_expression = 'ARRAY_CONSTRUCT(calendar_event)' %}
{% else %}
{% set calendar_event_array_expression = '???' %}
{% endif %}

with dim_school_calendar as (
  select * from {{ ref('dim_school_calendar') }}
),
{% for d in range(296) %}
synth{{d}} as (
    select 
        md5(k_school_calendar || '{{d}}') as k_calendar_date,
        k_school_calendar,
        k_school,
        tenant_code,
        calendar_code,
        dateadd(day, {{d}}, (school_year||'-08-10')::date) as calendar_date,
        school_year,
        (
            case extract(dow from calendar_date)
                when 0 then 'Sun'
                when 1 then 'Mon'
                when 2 then 'Tue'
                when 3 then 'Wed'
                when 4 then 'Thu'
                when 5 then 'Fri'
                when 6 then 'Sat'
            end
        ) as week_day,
        case when RANDOM({{synth_get_randseed()}})<0.96 then true else false end as is_school_day,
        case when is_school_day then 'Instructional day' else 'Non-instructional day' end as calendar_event,
        {{calendar_event_array_expression}} as calendar_event_array,
        {{d + 1}} as day_of_school_year,
        DATE_PART('week', calendar_date)::int as week_of_calendar_year,
        (
            case
                when not is_school_day then null
                when week_of_calendar_year::int >= 33 then week_of_calendar_year::int - 33
                else week_of_calendar_year::int + 52 - 33
            end
        ) as week_of_school_year
    from dim_school_calendar
),
{% endfor %}

stacked as (
    {% for d in range(296) %}
    select * from synth{{d}}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select *
from stacked

-- get rid of weekend days:
{{ synth_add_cleanup_hook("delete from {{this}} where week_day='Sat' or week_day='Sun'") or "" }}


{{ config(post_hook=synth_get_post_hooks())}}