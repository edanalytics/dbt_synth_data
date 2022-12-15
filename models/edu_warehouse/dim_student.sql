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

{{ synth_table(
  rows = var('num_students'),
  columns = [
    synth_column_primary_key(name='k_student'),
    synth_column_primary_key(name='k_student_xyear'),
    synth_column_values(name='tenant_code', values=var('tenant_codes')),
    synth_column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year')),
    synth_column_integer(name='student_unique_id', min=1000000, max=9999999),
    synth_column_firstname(name='first_name'),
    synth_column_firstname(name='middle_name'),
    synth_column_lastname(name='last_name'),
    synth_column_expression(name='display_name', expression="concat(last_name, ', ', first_name, coalesce(' ' || left(middle_name, 1), ''))"),
    synth_column_correlation(data=birthyear_grade_correlations, column='birth_year'),
    synth_column_boolean(name='is_lep', pct_true=var('students_pct_lep')),
    synth_column_expression(name='lep_code', expression="case when is_lep then 'Limited' else 'NotLimited' end"),
    synth_column_lookup(name='genders', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
    synth_column_correlation(data=birthyear_grade_correlations, column='grade'),
    synth_column_values(name='race_ethnicity', values=['Latinx', 'White', 'Black - African American', 'Asian', 'Multiple', 'Native Hawaiian - Pacific Islander', 'American Indian - Alaska Native'], probabilities=[0.334, 0.180, 0.332, 0.109, 0.042, 0.001, 0.002]),
    synth_column_boolean(name='is_special_education_annual', pct_true=var('students_pct_is_special_education_annual')),
    synth_column_boolean(name='is_special_education_active', pct_true=var('students_pct_is_special_education_active')),
    synth_column_boolean(name='is_economic_disadvantaged', pct_true=var('students_pct_is_economic_disadvantaged')),
    synth_column_boolean(name='is_immigrant', pct_true=var('students_pct_is_immigrant')),
    synth_column_expression(name='race_array', expression=race_array_expression, type='array'),
    synth_column_expression(name='safe_display_name', expression="concat(display_name, ' (', student_unique_id, ')')"),
  ]
) }}

{{ synth_add_cleanup_hook('alter table {{ this }} drop column is_lep') or "" }}
{{ config(post_hook=synth_get_post_hooks())}}

{# synth_column_values(name='gender', values=['Male', 'Female'], weights=[0.488, 0.512]), #}
{# indegree=synth_distribution(type='uniform'), pct_null='0.03'), #}
{# indegree=synth_distribution(type='zipf', N=12, s=2), pct_null='0.03'), #}
    {# synth_column(name='guardian',     type='fkey',
                     table_ref=ref('dim_guardian'), column_ref='k_guardian',
                     indegree=synth_distribution(type='normal', avg=2.2, stddev=0.5), pct_null='0.03'), #}
