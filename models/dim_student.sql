-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}
{% set birthyear_grade_correlations = ({
  "randseed": dbt_synth.get_randseed(),
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

{{ dbt_synth.table(
  rows = var('num_students'),
  columns = [
    dbt_synth.column_primary_key(name='k_student'),
    dbt_synth.column_primary_key(name='k_student_xyear'),
    dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
    dbt_synth.column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    dbt_synth.column_integer(name='student_unique_id', min=1000000, max=9999999, distribution='uniform'),
    dbt_synth.column_firstname(name='first_name'),
    dbt_synth.column_firstname(name='middle_name'),
    dbt_synth.column_lastname(name='last_name'),
    dbt_synth.column_expression(name='display_name', expression="concat(last_name, ', ', first_name, coalesce(' ' || left(middle_name, 1), ''))"),
    dbt_synth.column_correlation(data=birthyear_grade_correlations, column='birth_year'),
    dbt_synth.column_boolean(name='is_lep', pct_true=var('students_pct_lep')),
    dbt_synth.column_expression(name='lep_code', expression="case when is_lep then 'Limited' else 'NotLimited' end"),
    dbt_synth.column_lookup(name='genders', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
    dbt_synth.column_correlation(data=birthyear_grade_correlations, column='grade'),
    dbt_synth.column_values(name='race_ethnicity', values=['Latinx', 'White', 'Black - African American', 'Asian', 'Multiple', 'Native Hawaiian - Pacific Islander', 'American Indian - Alaska Native'], probabilities=[0.334, 0.180, 0.332, 0.109, 0.042, 0.001, 0.002]),
    dbt_synth.column_boolean(name='is_special_education_annual', pct_true=var('students_pct_is_special_education_annual')),
    dbt_synth.column_boolean(name='is_special_education_active', pct_true=var('students_pct_is_special_education_active')),
    dbt_synth.column_boolean(name='is_economic_disadvantaged', pct_true=var('students_pct_is_economic_disadvantaged')),
    dbt_synth.column_boolean(name='is_immigrant', pct_true=var('students_pct_is_immigrant')),
    dbt_synth.column_expression(name='race_array', expression=race_array_expression, type='array'),
    dbt_synth.column_expression(name='safe_display_name', expression="concat(display_name, ' (', student_unique_id, ')')"),
  ]
) }}

{{ dbt_synth.add_cleanup_hook('alter table {{ this }} drop column is_lep') or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}

{# dbt_synth.column_values(name='gender', values=['Male', 'Female'], weights=[0.488, 0.512]), #}
{# indegree=dbt_synth.distribution(type='uniform'), pct_null='0.03'), #}
{# indegree=dbt_synth.distribution(type='zipf', N=12, s=2), pct_null='0.03'), #}
    {# dbt_synth.column(name='guardian',     type='fkey',
                     table_ref=ref('dim_guardian'), column_ref='k_guardian',
                     indegree=dbt_synth.distribution(type='normal', avg=2.2, stddev=0.5), pct_null='0.03'), #}
