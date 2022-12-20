-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('synth_words') }}

{{ config(materialized='table') }}

select
    {{ synth_primary_key() }} as k_course,
    {{ synth_select(
        value_col="lea_id",
        lookup_table='dim_lea',
        distribution='uniform',
        filter='k_sea is not null'
    ) }} as ed_org_id,
    {{ synth_value(value='LocalEducationAgency') }} as ed_org_type,
    {{ synth_lookup(value_col='ed_org_id', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code') }} as tenant_code,
    {{ synth_integer(min=var('min_school_year'), max=var('max_school_year')) }} as school_year,
    {{ synth_words(name='course_title', language_code='en', distribution="uniform", format_strings=[
        "{ADV} learning for {ADJ} {NOUN}s",
        "{ADV} {VERB} {NOUN} course"
        ], funcs=["INITCAP"]) }} as course_title,
    {{ synth_integer(min=100, max=900, distribution='uniform') }} as course_num,
    {{ synth_expression(expression="REPLACE(SUBSTR(course_title,0,5), ' ', '') || course_num") }} as course_code,
    {{ synth_expression(expression="'A course with Title \"' || course_title || '\" and Code \"' || course_code || '\"'") }} as course_description,
    {{ synth_values(
        values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'],
        probabilities=[0.2, 0.3, 0.15, 0.35]
    ) }} as academic_subject,
    {{ synth_value(value=None) }} as career_pathway,
    {{ synth_value(value=None) }} as course_defined_by,
    {{ synth_value(value=None) }} as gpa_applicability,
    {{ synth_value(value=None) }} as date_course_adopted,
    {{ synth_value(value=None) }} as is_high_school_course_requirement,
    {{ synth_value(value=None) }} as max_completions_for_credit,
    {{ synth_value(value=None) }} as maximum_available_credits,
    {{ synth_value(value=None) }} as maximum_available_credit_type,
    {{ synth_value(value=None) }} as maximum_available_credit_conversion,
    {{ synth_value(value=None) }} as minimum_available_credits,
    {{ synth_value(value=None) }} as minimum_available_credit_type,
    {{ synth_value(value=None) }} as minimum_available_credit_conversion,
    {{ synth_value(value=1) }} as number_of_parts,
    {{ synth_value(value=None) }} as time_required_for_completion
from {{ synth_table(rows=var('num_courses')) }}

{{ synth_add_cleanup_hook("alter table {{ this }} drop column course_num") or "" }}
{{ config(post_hook=synth_get_post_hooks())}}