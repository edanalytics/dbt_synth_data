-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}

with 
{% for i in range(var('max_school_year')-var('min_school_year')+1) %}
{% set year = (var('min_school_year') + i)|string %}
synth{{year}} as (

{{ synth_table(
  rows = 100,
  columns = [
    synth_column_primary_key(name='k_session'),
    synth_column_value(name='tenant_code', value='SomeTenant'),
    synth_column_integer(name='staff_unique_id', min=1000000, max=9999999, distribution='uniform'),
    synth_column_expression(name='district_staff_id', expression="staff_unique_id", type='int'),
    synth_column_value(name='login_id', value=None),
    synth_column_firstname(name='first_name'),
    synth_column_firstname(name='middle_name'),
    synth_column_lastname(name='last_name'),
    synth_column_expression(name='display_name', expression="last_name || ', ' || first_name"),
    synth_column_expression(name='email_adddress', expression="lower(first_name) || '.' || lower(last_name) || '@schooldistrict.org'"),
    synth_column_value(name='email_type', value='Work'),
    synth_column_value(name='personal_title_prefix', value=None),
    synth_column_value(name='generation_code_suffix', value=None),
    synth_column_date(name='birth_date', min='1938-01-01', max='1994-12-31'),
    synth_column_lookup(name='gender', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
    synth_column_value(name='highest_completed_level_of_education', value=None),
    synth_column_value(name='is_highly_qualified_teacher', value=None),
    synth_column_value(name='years_of_prior_professional_experience', value=None),
    synth_column_value(name='years_of_prior_teaching_experience', value=None),
  ]
) }}

{% endfor %}

{#
  session_begin_date,
  session_end_date,
  total_instructional_days,
  academic_term
#}

{{ config(materialized='table') }}
{{ synth_table(
  rows = 5*var('num_schools'),
  columns = [
    synth_column_primary_key(name='k_session'),
    synth_column_foreign_key(name='k_school'),
    synth_column_lookup(name='tenant_code', value_col='k_school', lookup_table='dim_school', from_col='k_school', to_col='tenant_code'),
    synth_column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
    synth_column_values(name='session_name', values=var('session_types')),
  ]
) }}
{{ config(post_hook=synth_get_post_hooks())}}
