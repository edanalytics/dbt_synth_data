{#
-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_course_section') }}

k_student,
k_course_section,
tenant_code,
attendance_event_date,
attendance_event_category,
attendance_event_reason,
is_absent,
event_duration,
section_attendance_duration,
arrival_time,
departure_time,
educational_environment
#}
select 1 as test