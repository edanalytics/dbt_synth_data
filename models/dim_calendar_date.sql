{{ config(materialized='table') }}

with synth2020 as (
    {{ dbt_synth.table(
    rows = 296,
    columns = [
        dbt_synth.column_primary_key(name='k_calendar_date'),
        dbt_synth.column_value(name='k_school_calendar', value='ComingSoon'),
        dbt_synth.column_foreign_key(name='k_lea', table='dim_lea', column='k_lea'),
        dbt_synth.column_value(name='tenant_code', value='Primary Tenant'),
        dbt_synth.column_value(name='calendar_code', value='Normal'),
        dbt_synth.column_date_sequence(name='calendar_date', start_date='2020-08-10'),
        dbt_synth.column_value(name='school_year', value=2020),
        dbt_synth.column_expression(name='week_day', expression="case extract(dow from calendar_date) when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue' when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri' when 6 then 'Sat' end"),
        dbt_synth.column_boolean(name='is_school_day', pct_true=0.96),
        dbt_synth.column_map(name='calendar_event', expression='is_school_day', mapping=({ true:'Instructional day', false:'Non-instructional day' })),
        dbt_synth.column_expression(name='calendar_event_array', expression='ARRAY[calendar_event]'),
        dbt_synth.column_integer_sequence(name='day_of_school_year', step=1),
        dbt_synth.column_expression(name='week_of_calendar_year', type='int', expression="DATE_PART('week', calendar_date)::int"),
        dbt_synth.column_expression(name='week_of_school_year', type='int', expression='case when not is_school_day then null when week_of_calendar_year >= 33 then week_of_calendar_year - 33 else week_of_calendar_year + 52 - 33 end'),
    ]
    ) }}
),
synth2021 as (
    {{ dbt_synth.table(
    rows = 296,
    columns = [
        dbt_synth.column_primary_key(name='k_calendar_date'),
        dbt_synth.column_value(name='k_school_calendar', value='ComingSoon'),
        dbt_synth.column_value(name='k_school', value='ComingSoon'),
        dbt_synth.column_value(name='tenant_code', value='Primary Tenant'),
        dbt_synth.column_value(name='calendar_code', value='Normal'),
        dbt_synth.column_date_sequence(name='calendar_date', start_date='2021-08-09'),
        dbt_synth.column_value(name='school_year', value=2021),
        dbt_synth.column_expression(name='week_day', expression="case extract(dow from calendar_date) when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue' when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri' when 6 then 'Sat' end"),
        dbt_synth.column_boolean(name='is_school_day', pct_true=0.848),
        dbt_synth.column_map(name='calendar_event', expression='is_school_day', mapping=({ true:'Instructional day', false:'Non-instructional day' })),
        dbt_synth.column_expression(name='calendar_event_array', expression='ARRAY[calendar_event]'),
        dbt_synth.column_integer_sequence(name='day_of_school_year', step=1),
        dbt_synth.column_expression(name='week_of_calendar_year', type='int', expression="DATE_PART('week', calendar_date)::int"),
        dbt_synth.column_expression(name='week_of_school_year', type='int', expression='case when not is_school_day then null when week_of_calendar_year >= 32 then week_of_calendar_year - 32 else week_of_calendar_year + 52 - 32 end'),
    ]
    ) }}
),
{# dbt_synth.column_foreign_key(name='k_school_calendar', table='dim_school_calendar', column='k_school_calendar'), #}
{# dbt_synth.column_foreign_key(name='k_school', table='dim_school', column='k_school'), #}
stacked as (
    select * from synth2020
    union
    select * from synth2021
)
select *
from stacked

-- get rid of weekend days:
{{ dbt_synth.add_post_hook("delete from {{this}} where week_day='Sat' or week_day='Sun'") or "" }}


{{ config(post_hook=dbt_synth.get_post_hooks())}}