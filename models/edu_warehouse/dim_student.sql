-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}
{% set birthyear_grade_correlations = ({
  "randseed": synth_get_randseed(),
  "columns": {
    "birth_year": [ 2010, 2009, 2008, 2007, 2006, 2005, 2004 ],
    "grade": [ 'Eighth grade', 'Ninth grade', 'Tenth grade', 'Eleventh grade', 'Twelfth grade' ]
  },
  "probabilities": [
    [ 0.02, 0.00, 0.00, 0.00, 0.00 ],
    [ 0.15, 0.02, 0.00, 0.00, 0.00 ],
    [ 0.03, 0.15, 0.02, 0.00, 0.00 ],
    [ 0.00, 0.03, 0.15, 0.02, 0.00 ],
    [ 0.00, 0.00, 0.03, 0.15, 0.02 ],
    [ 0.00, 0.00, 0.00, 0.03, 0.15 ],
    [ 0.00, 0.00, 0.00, 0.00, 0.03 ]
  ]
  })
%}
{% if target.type=='postgres' %}
{% set race_array_expression = 'ARRAY[race_ethnicity]' %}
{% elif target.type=='snowflake' %}
{% set race_array_expression = 'ARRAY_CONSTRUCT(race_ethnicity)' %}
{% else %}
{% set race_array_expression = '???' %}
{% endif %}

select
    {{ synth_primary_key() }} as k_student,
    {{ synth_primary_key() }} as k_student_xyear,
    {{ synth_values(values=var('tenant_codes')) }} as tenant_code,
    {{ synth_integer(min=var('min_school_year'), max=var('max_school_year')) }} as school_year,
    {{ synth_integer(min=1000000, max=9999999) }} as student_unique_id,
    {{ synth_firstname() }} as first_name,
    {{ synth_firstname() }} as middle_name,
    {{ synth_lastname() }} as last_name,
    {{ synth_expression(expression="concat(last_name, ', ', first_name, coalesce(' ' || left(middle_name, 1), ''))") }} as display_name,
    {{ synth_correlation(data=birthyear_grade_correlations, column='birth_year') }} as birth_year,
    {{ synth_boolean(pct_true=var('students_pct_lep')) }} as is_lep,
    {{ synth_expression(expression="case when is_lep then 'Limited' else 'NotLimited' end") }} as lep_code,
    {{ synth_lookup(value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']) }} as gender,
    {{ synth_correlation(data=birthyear_grade_correlations, column='grade') }} as grade,
    {{ synth_values(
      values=['Latinx', 'White', 'Black - African American', 'Asian', 'Multiple', 'Native Hawaiian - Pacific Islander', 'American Indian - Alaska Native'],
      probabilities=[0.334, 0.180, 0.332, 0.109, 0.042, 0.001, 0.002]
    ) }} as race_ethnicity,
    {{ synth_boolean(pct_true=var('students_pct_is_special_education_annual')) }} as is_special_education_annual,
    {{ synth_boolean(pct_true=var('students_pct_is_special_education_active')) }} as is_special_education_active,
    {{ synth_boolean(pct_true=var('students_pct_is_economic_disadvantaged')) }} as is_economic_disadvantaged,
    {{ synth_boolean(pct_true=var('students_pct_is_immigrant')) }} as is_immigrant,
    {{ synth_expression(expression=race_array_expression, type='array') }} as race_array,
    {{ synth_expression(expression="concat(display_name, ' (', student_unique_id, ')')") }} as safe_display_name
from {{ synth_table(rows=var('num_students')) }}

{{ synth_add_cleanup_hook('alter table {{ this }} drop column is_lep') or "" }}
{{ config(post_hook=synth_get_post_hooks())}}

{# synth_column_values(name='gender', values=['Male', 'Female'], weights=[0.488, 0.512]), #}
{# indegree=synth_distribution(type='uniform'), pct_null='0.03'), #}
{# indegree=synth_distribution(type='zipf', N=12, s=2), pct_null='0.03'), #}
    {# synth_column(name='guardian',     type='fkey',
                     table_ref=ref('dim_guardian'), column_ref='k_guardian',
                     indegree=synth_distribution(type='normal', avg=2.2, stddev=0.5), pct_null='0.03'), #}
