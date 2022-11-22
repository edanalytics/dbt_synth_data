{#
-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_school_calendar') }}

k_student,
k_student_xyear,
k_lea,
k_school,
k_school_calendar,
tenant_code,
school_year,
entry_date,
exit_withdraw_date,
is_primary_school,
is_repeat_grade,
is_school_choice_transfer,
is_active_enrollment,
entry_grade_level,
entry_grade_level_reason,
entry_type,
exit_withdraw_type,
class_of_school_year,
graduation_plan_type,
residency_status,
is_latest_annual_entry
#}
select 1 as test