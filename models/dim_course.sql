-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('synth_words') }}

{{ config(materialized='table') }}
{{ dbt_synth.table(
    rows = var('num_courses'),
    columns = [
        dbt_synth.column_primary_key(name='k_course'),
        dbt_synth.column_select(
            name='ed_org_id',
            value_col="lea_id",
            lookup_table='dim_lea',
            distribution='uniform',
            filter='k_sea is not null'
        ),
        dbt_synth.column_value(name='ed_org_type', value='LocalEducationAgency'),
        dbt_synth.column_lookup(name='tenant_code', value_col='ed_org_id', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code'),
        dbt_synth.column_integer(name='school_year', min=var('min_school_year'), max=var('max_school_year'), distribution='uniform'),
        dbt_synth.column_words(name='course_title', distribution="uniform", format_strings=[
            "{adverb} learning for {adjective} {noun}s",
            "{adverb} {verb} {noun} course"
            ], funcs=["INITCAP"]),
        dbt_synth.column_integer(name='course_num', min=100, max=900, distribution='uniform'),
        dbt_synth.column_expression(name='course_code', expression="REPLACE(SUBSTR(course_title,0,5), ' ', '') || course_num"),
        dbt_synth.column_expression(name='course_description', expression="'A course with Title \"' || course_title || '\" and Code \"' || course_code || '\"'"),
        dbt_synth.column_values(name='academic_subject', values=['Mathematics', 'Science', 'English Language Arts', 'Social Studies'], distribution=[0.2, 0.3, 0.15, 0.35]),
        dbt_synth.column_value(name='career_pathway', value=None),
        dbt_synth.column_value(name='course_defined_by', value=None),
        dbt_synth.column_value(name='gpa_applicability', value=None),
        dbt_synth.column_value(name='date_course_adopted', value=None),
        dbt_synth.column_value(name='is_high_school_course_requirement', value=None),
        dbt_synth.column_value(name='max_completions_for_credit', value=None),
        dbt_synth.column_value(name='maximum_available_credits', value=None),
        dbt_synth.column_value(name='maximum_available_credit_type', value=None),
        dbt_synth.column_value(name='maximum_available_credit_conversion', value=None),
        dbt_synth.column_value(name='minimum_available_credits', value=None),
        dbt_synth.column_value(name='minimum_available_credit_type', value=None),
        dbt_synth.column_value(name='minimum_available_credit_conversion', value=None),
        dbt_synth.column_value(name='number_of_parts', value=1),
        dbt_synth.column_value(name='time_required_for_completion', value=None),
    ]
) }}

{{ dbt_synth.add_cleanup_hook("alter table {{ this }} drop column course_num") or "" }}
{{ config(post_hook=dbt_synth.get_post_hooks())}}