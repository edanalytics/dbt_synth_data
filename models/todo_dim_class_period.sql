-- depends_on: {{ ref('dim_school') }}

{#
k_class_period,
k_school,
tenant_code,
school_year,
class_period_name,
is_official_attendance_period,
start_time,
end_time,
timediff(minutes, start_time, end_time) as period_duration
#}