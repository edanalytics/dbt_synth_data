-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('synth_words') }}

{{ config(materialized='table') }}
{{ synth_table(
    rows = var('num_courses'),
    columns = [
        synth_column_primary_key(name='k_course'),
        synth_column_select(
            name='ed_org_id',
            value_col="lea_id",
            lookup_table='dim_lea',
            distribution='uniform',
            filter='k_sea is not null'
        ),
        synth_column_value(name='ed_org_type', value='LocalEducationAgency'),
        synth_column_lookup(name='tenant_code', value_col='ed_org_id', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code'),
        synth_column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
        synth_column_words(name='course_title', language_code='en', distribution="uniform", format_strings=[
            "{ADV} learning for {ADJ} {NOUN}s",
            "{ADV} {VERB} {NOUN} course"
            ], funcs=["INITCAP"]),
        synth_column_integer(name='course_num', min=100, max=900, distribution='uniform'),
        synth_column_expression(name='course_code', expression="REPLACE(SUBSTR(course_title,0,5), ' ', '') || course_num"),
        synth_column_expression(name='course_description', expression="'A course with Title \"' || course_title || '\" and Code \"' || course_code || '\"'"),
        synth_column_values(name='academic_subject', values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'], probabilities=[0.2, 0.3, 0.15, 0.35]),
        synth_column_value(name='career_pathway', value=None),
        synth_column_value(name='course_defined_by', value=None),
        synth_column_value(name='gpa_applicability', value=None),
        synth_column_value(name='date_course_adopted', value=None),
        synth_column_value(name='is_high_school_course_requirement', value=None),
        synth_column_value(name='max_completions_for_credit', value=None),
        synth_column_value(name='maximum_available_credits', value=None),
        synth_column_value(name='maximum_available_credit_type', value=None),
        synth_column_value(name='maximum_available_credit_conversion', value=None),
        synth_column_value(name='minimum_available_credits', value=None),
        synth_column_value(name='minimum_available_credit_type', value=None),
        synth_column_value(name='minimum_available_credit_conversion', value=None),
        synth_column_value(name='number_of_parts', value=1),
        synth_column_value(name='time_required_for_completion', value=None),
    ]
) }}

{{ synth_add_cleanup_hook("alter table {{ this }} drop column course_num") or "" }}
{{ config(post_hook=synth_get_post_hooks())}}