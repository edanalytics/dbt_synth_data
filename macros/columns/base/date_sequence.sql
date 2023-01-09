{% macro synth_column_date_sequence(name, start_date='', step=1) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_date_sequence_base')(start_date, step) }} AS {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_date_sequence_base(start_date, step) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_column_date_sequence_base(start_date, step) %}
    {% if start_date|length ==0 %}
    (DATE('now', '+' || {{step}}*(__row_number-1) || ' days'))
    {% else %}
    (DATE('{{start_date}}', '+' || {{step}}*(__row_number-1) || ' days'))
    {% endif %}
{% endmacro %}

{% macro postgres__synth_column_date_sequence_base(start_date, step) %}
    {% if start_date|length ==0 %}
    (CURRENT_DATE + interval '{{step}} days' * (__row_number-1))::date
    {% else %}
    (TO_DATE('{{start_date}}', 'YYYY-MM-DD') + interval '{{step}} days' * (__row_number-1))::date
    {% endif %}
{% endmacro %}

{% macro snowflake__synth_column_date_sequence_base(start_date, step) %}
    {% if start_date|length ==0 %}
    dateadd(day, {{step}}*(__row_number-1), current_date())
    {% else %}
    dateadd(day, {{step}}*(__row_number-1), '{{start_date}}')
    {% endif %}
{% endmacro%}