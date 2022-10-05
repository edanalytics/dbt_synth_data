{#
-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_calendar_date') }}
-- depends_on: {{ ref('dim_session') }}

k_student,
k_student_xyear,
k_school,
k_calendar_date,
k_session,
tenant_code,
attendance_event_category,
attendance_event_reason,
is_absent,
event_duration,
arrival_time,
departure_time,
educational_environment
#}