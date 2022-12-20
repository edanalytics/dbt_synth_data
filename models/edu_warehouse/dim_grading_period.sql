-- depends_on: {{ ref('dim_school') }}

{{ config(materialized='table') }}

{% set grading_period_types = ['First Semester', 'Second Semester'] %}

with dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),

{% for i in range(grading_period_types|length) %}
{% set grading_period_type = grading_period_types[i] %}
grading_period{{i}} as (
    select
        md5(k_school || school_year || '{{grading_period_type}}') as k_grading_period,
        k_school,
        tenant_code,
        '{{grading_period_type}}' as grading_period,
        {{i+1}} as period_sequence,
        school_year,
        {% if i==0 %}
            min(calendar_date)
        {% else %}
            {# first day in next year #}
            min(case when date_part(year, calendar_date)=school_year then calendar_date end)
        {% endif %} as begin_date,
        {% if i==0 %}
            {# last day of current year #}
            max(case when date_part(year, calendar_date)!=school_year then calendar_date end)
        {% else %}
            max(calendar_date)
        {% endif %} as end_date,
        sum(case when is_school_day then 1 else 0 end) as total_instructional_days
    from dim_calendar_date
    group by tenant_code, k_school, school_year
),
{% endfor %}

stacked as (
    {% for i in range(grading_period_types|length) %}
    select * from grading_period{{i}}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select * from stacked