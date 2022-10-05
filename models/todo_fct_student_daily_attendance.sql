{#
-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_calendar_date') }}
-- depends_on: {{ ref('dim_session') }}

k_student,
k_school,
k_calendar_date,
k_session,
tenant_code,
attendance_event_category,
is_absent,
is_present,
is_enrolled,
total_days_enrolled,
cumulative_days_absent,
cumulative_days_attended,
cumulative_days_enrolled,
cumulative_attendance_rate,
meets_enrollment_threshold,
is_chronic_absentee,
absentee_category_rank,
absentee_category_label
#}