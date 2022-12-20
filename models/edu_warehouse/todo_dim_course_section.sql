{#
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_course') }}
-- depends_on: {{ ref('dim_session') }}
-- depends_on: {{ ref('dim_classroom') }}

,
,
k_school,
k_session,
k_classroom,
tenant_code,
section_id,
section_name,
local_course_code,
local_course_title,
course_code,
course_title,
school_year,
session_name,
academic_subject,
career_pathway,
instructional_time_planned,
sequence_of_course,
educational_environment_type,
instruction_language,
medium_of_instruction,
population_served,
available_credits,
available_credit_conversion,
available_credit_type,
is_official_attendance_period
#}

{{ config(materialized='table') }}
select
        {{ synth_primary_key() }} as k_course_section,
        {{ synth_foreign_key(table='dim_course', column='k_course') }} as k_course,

        {{ synth_foreign_key(table='dim_lea', column='k_lea') }} as k_lea,
        {{ synth_lookup(value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code') }} as tenant_code,
        {{ synth_integer_sequence(step=1, start=1000) }} as school_id,
        {{ synth_value(value='SchoolNameComingSoon') }} as school_name,
        {{ synth_value(value='SchoolShortNameComingSoon') }} as school_short_name,
        {{ synth_lookup(value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_name') }} as lea_name,
        {{ synth_lookup(value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_id') }} as lea_id,
        {{ synth_value(value=None) }} as school_category,
        {{ synth_value(value='Regular') }} as school_type,
        {{ synth_value(value=None) }} as operational_status,
        {{ synth_value(value=None) }} as administrative_funding_control,
        {{ synth_value(value=None) }} as internet_access,
        {{ synth_value(value=None) }} as title_i_part_a_school_designation,
        {{ synth_value(value=None) }} as charter_status,
        {{ synth_value(value=None) }} as charter_approval_agency,
        {{ synth_value(value=None) }} as magnet_type,
        {{ synth_value(value=None) }} as website,
        {{ synth_value(value='Physical') }} as address_type,
        {{ synth_address(name='street_address', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['street_address']) }} as street_address,
        {{ synth_address(name='city', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['city']) }} as city,
        {{ synth_value(value=None) }} as name_of_county,
        {{ synth_address(name='state_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['geo_region']) }} as state_code,
        {{ synth_address(name='postal_code', countries=['United States'], geo_regions=[var('state_code')], address_types=['house'], parts=['postal_code']) }} as postal_code,
        {{ synth_value(value=None) }} as building_site_number,
        {{ synth_value(value=None) }} as locale,
        {{ synth_value(value=None) }} as congressional_district,
        {{ synth_value(value=None) }} as county_fips_code,
        {{ synth_value(value=None) }} as latitude,
        {{ synth_value(value=None) }} as longitude
from {{ synth_table(rows=var('num_course_sections')) }}

{{ config(post_hook=synth_get_post_hooks())}}