-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}

{{ synth_table(
  rows = var('num_staffs'),
  columns = [
    synth_column_primary_key(name='k_staff'),
    synth_column_values(name='tenant_code', values=var('tenant_codes')),
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
    synth_column_date(name='birth_date', min=var('staff_min_birthdate'), max=var('staff_max_birthdate')),
    synth_column_lookup(name='gender', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
    synth_column_value(name='highest_completed_level_of_education', value=None),
    synth_column_value(name='is_highly_qualified_teacher', value=None),
    synth_column_value(name='years_of_prior_professional_experience', value=None),
    synth_column_value(name='years_of_prior_teaching_experience', value=None),
  ]
) }}

{{ config(post_hook=synth_get_post_hooks())}}
