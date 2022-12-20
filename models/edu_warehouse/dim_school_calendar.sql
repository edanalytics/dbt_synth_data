{{ config(materialized='table') }}

with dim_school as (
  select * from {{ ref('dim_school') }}
),

{% for i in range(var('max_school_year')-var('min_school_year')+1) %}
{% set year = (var('min_school_year') + i)|string %}
synth{{year}} as (
  select
    md5(k_school || {{year}}) as k_school_calendar,
    k_school,
    tenant_code,
    {{year}} as school_year,
    'Normal' as calendar_code,
    'School' as calendar_type,
    ['Preschool/Prekindergarten','Kindergarten','First grade','Second grade','Third grade','Fourth grade','Fifth grade','Sixth grade','Seventh grade','Eighth grade','Ninth grade','Tenth grade','Eleventh grade','Twelfth grade'] as applicable_grade_levels_array
  from dim_school
),
{% endfor %}

stacked as (
    {% for i in range(var('max_school_year')-var('min_school_year')+1) %}
    {% set year = var('min_school_year') + i %}
    select * from synth{{year}}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
)
select *
from stacked
