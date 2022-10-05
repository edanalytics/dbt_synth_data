-- depends_on: {{ ref('dim_student') }}
-- depends_on: {{ ref('dim_school') }}
-- depends_on: {{ ref('dim_staff') }}

{#
k_student,
k_school,
k_school__assignment,
k_school__responsibility,
k_staff,
discipline_action_id,
discipline_date,
discipline_action,
discipline_action_length,
actual_discipline_action_length,
triggered_iep_placement_meeting,
is_related_to_zero_tolerance_policy,
discipline_action_length_difference_reason,
k_staff_array,
is_oss,
is_iss,
is_exp,
is_minor,
severity_order
#}