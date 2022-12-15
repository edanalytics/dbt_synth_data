{#
-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_student') }}

k_course,
k_student_academic_record,
k_lea,
k_school,
k_student,
tenant_code,
school_year,
academic_term,
course_attempt_result,
course_title,
alternative_course_code,
alternative_course_title,
when_taken_grade_level,
final_letter_grade_earned,
final_numeric_grade_earned,
earned_credits,
attempted_credits,
course_repeat_code,
method_credit_earned,
earned_credit_type,
earned_credit_conversion,
attempted_credit_type,
attempted_credit_conversion,
assigning_organization_identification_code,
course_catalog_url
#}
select 1 as test