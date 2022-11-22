{#
-- depends_on: {{ ref('dim_staff') }}
-- depends_on: {{ ref('dim_course_section') }}

k_staff,
k_course_section,
tenant_code,
begin_date,
end_date,
classroom_position,
is_highly_qualified_teacher,
percentage_contribution,
teacher_student_data_link_exclusion,
is_active_assignment
#}
select 1 as test