-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}

{{ dbt_synth.table(
  rows = var('num_staffs'),
  columns = [
    dbt_synth.column_primary_key(name='k_staff'),
    dbt_synth.column_values(name='tenant_code', values=var('tenant_codes')),
    dbt_synth.column_integer(name='staff_unique_id', min=1000000, max=9999999, distribution='uniform'),
    dbt_synth.column_expression(name='district_staff_id', expression="staff_unique_id", type='int'),
    dbt_synth.column_value(name='login_id', value=None),
    dbt_synth.column_firstname(name='first_name'),
    dbt_synth.column_firstname(name='middle_name'),
    dbt_synth.column_lastname(name='last_name'),
    dbt_synth.column_expression(name='display_name', expression="last_name || ', ' || first_name"),
    dbt_synth.column_expression(name='email_adddress', expression="lower(first_name) || '.' || lower(last_name) || '@schooldistrict.org'"),
    dbt_synth.column_value(name='email_type', value='Work'),
    dbt_synth.column_value(name='personal_title_prefix', value=None),
    dbt_synth.column_value(name='generation_code_suffix', value=None),
    dbt_synth.column_date(name='birth_date', min=var('staff_min_birthdate'), max=var('staff_max_birthdate')),
    dbt_synth.column_lookup(name='gender', value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']),
    dbt_synth.column_value(name='highest_completed_level_of_education', value=None),
    dbt_synth.column_value(name='is_highly_qualified_teacher', value=None),
    dbt_synth.column_value(name='years_of_prior_professional_experience', value=None),
    dbt_synth.column_value(name='years_of_prior_teaching_experience', value=None),
  ]
) }}

{{ config(post_hook=dbt_synth.get_post_hooks())}}
