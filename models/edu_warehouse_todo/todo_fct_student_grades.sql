{#
-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_course_section') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_grading_period') }}

k_student,
k_course_section,
k_school,
k_grading_period,
grade_type,
tenant_code,
letter_grade_earned,
numeric_grade_earned,
diagnostic_statement,
performance_base_conversion,
unweighted_gpa_points,
exclude_from_gpa,
is_dorf,
grade_sort_index
#}
select 1 as test