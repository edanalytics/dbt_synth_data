-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}
{% set birthyear_grade_correlations = ({
  "randseed": dbt_synth.get_randseed(),
  "columns": {
    "birth_year": [ 2010, 2009, 2008, 2007, 2006, 2005, 2004 ],
    "grade": [ 8, 9, 10, 11, 12 ]
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
{{ dbt_synth.table(
  rows = 10000000,
  columns = [
    dbt_synth.column_primary_key(name='k_student'),
    dbt_synth.column_values(name='school', values=['15628', '15631', '15632', '60918', '60919', '60920'], distribution=[0.05, 0.1, 0.15, 0.15, 0.15, 0.4]),
    dbt_synth.column_integer(name='school_year', min=2020, max=2022, distribution='uniform'),
    dbt_synth.column_integer(name='student_id', min=100000, max=999999, distribution='uniform'),
    dbt_synth.column_integer(name='state_student_id', min=1000000, max=9999999, distribution='uniform'),
    dbt_synth.column_firstname(name='first_name'),
    dbt_synth.column_firstname(name='middle_name'),
    dbt_synth.column_lastname(name='last_name'),
    dbt_synth.column_expression(name='display_name', expression="last_name || ', ' || first_name"),
    dbt_synth.column_lookup(name='gender', value_col='UPPER(first_name)', lookup_table='synth_firstnames', from_col='name', to_col='gender'),
    dbt_synth.column_values(name='race', values=['Hispanic', 'White', 'Black', 'Asian', 'Multiple'], distribution=[0.25, 0.35, 0.3, 0.05, 0.05]),
    dbt_synth.column_correlation(data=birthyear_grade_correlations, column='grade'),
    dbt_synth.column_correlation(data=birthyear_grade_correlations, column='birth_year'),
    dbt_synth.column_integer(name='birth_month', min=1, max=12, distribution='uniform'),
    dbt_synth.column_integer(name='birth_day', min=1, max=28, distribution='uniform'),
    dbt_synth.column_expression(name='birth_date', expression="birth_year || '-' || birth_month || '-' || birth_day"),
    dbt_synth.column_boolean(name='is_immigrant', pct_true=0.03),
  ]
) }}

{# add your own: {{ dbt_synth.add_post_hook('select 1') or "" }} #}
{{ config(post_hook=dbt_synth.get_post_hooks())}}



{# indegree=dbt_synth.distribution(type='uniform'), pct_null='0.03'), #}
{# indegree=dbt_synth.distribution(type='zipf', N=12, s=2), pct_null='0.03'), #}
    {# dbt_synth.column(name='guardian',     type='fkey',
                     table_ref=ref('dim_guardian'), column_ref='k_guardian',
                     indegree=dbt_synth.distribution(type='normal', avg=2.2, stddev=0.5), pct_null='0.03'), #}
