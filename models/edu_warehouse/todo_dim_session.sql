-- depends_on: {{ ref('synth_firstnames') }}
-- depends_on: {{ ref('synth_lastnames') }}

{{ config(materialized='table') }}

with 
{% for i in range(var('max_school_year')-var('min_school_year')+1) %}
{% set year = (var('min_school_year') + i)|string %}
synth{{year}} as (

select
    {{ synth_primary_key() }} as k_session,
    {{ synth_value(value='SomeTenant') }} as tenant_code,
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
    {{ synth_date(min='1938-01-01', max='1994-12-31') }} as birth_date,
    {{ synth_lookup(value_col='first_name', lookup_table='synth_firstnames', from_col='name', to_col='gender', funcs=['UPPER']) }} as gender,
    {{ synth_value(value=None) }} as highest_completed_level_of_education,
    {{ synth_value(value=None) }} as is_highly_qualified_teacher,
    {{ synth_value(value=None) }} as years_of_prior_professional_experience,
    {{ synth_value(value=None) }} as years_of_prior_teaching_experience
from {{ synth_table(rows=100) }}

{% endfor %}

{#
  session_begin_date,
  session_end_date,
  total_instructional_days,
  academic_term
#}

{{ config(materialized='table') }}
select
    {{ synth_column_primary_key() }} as k_session,
    {{ synth_column_foreign_key() }} as k_school,
    {{ synth_column_lookup(value_col='k_school', lookup_table='dim_school', from_col='k_school', to_col='tenant_code') }} as tenant_code,
    {{ synth_column_integer(min=var('min_school_year'), max=var('max_school_year'), distribution='uniform') }} as school_year,
    {{ synth_column_values(values=var('session_types')) }} as session_name
from {{ synth_table(rows=5*var('num_schools')) }}

{{ config(post_hook=synth_get_post_hooks())}}
