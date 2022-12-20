-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}

select
    {{ synth_primary_key() }} as k_staff,
    {{ synth_values(values=var('tenant_codes')) }} as tenant_code,
    {{ synth_integer(min=1000000, max=9999999, distribution='uniform') }} as staff_unique_id,
    {{ synth_expression(expression="staff_unique_id", type='int') }} as district_staff_id,
    {{ synth_value(value=None) }} as login_id,
    {{ synth_firstname() }} as first_name,
    {{ synth_firstname() }} as middle_name,
    {{ synth_lastname() }} as last_name,
    {{ synth_expression(expression="last_name || ', ' || first_name") }} as display_name,
    {{ synth_expression(expression="lower(first_name) || '.' || lower(last_name) || '@schooldistrict.org'") }} as email_adddress,
    {{ synth_value(value='Work') }} as email_type,
    {{ synth_value(value=None) }} as personal_title_prefix,
    {{ synth_value(value=None) }} as generation_code_suffix,
    {{ synth_date(min=var('staff_min_birthdate'), max=var('staff_max_birthdate')) }} as birth_date,
    {{ synth_lookup(value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']) }} as gender,
    {{ synth_value(value=None) }} as highest_completed_level_of_education,
    {{ synth_value(value=None) }} as is_highly_qualified_teacher,
    {{ synth_value(value=None) }} as years_of_prior_professional_experience,
    {{ synth_value(value=None) }} as years_of_prior_teaching_experience
from {{ synth_table(rows=var('num_staffs')) }}

{{ config(post_hook=synth_get_post_hooks())}}