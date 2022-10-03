{% macro column_date_sequence(name, start_date='') -%}
    {{ return(adapter.dispatch('column_date_sequence')(name, start_date)) }}
{%- endmacro %}

{% macro default__column_date_sequence(name, start_date='') -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_date_sequence(name, start_date='') %}
    {% if start_date|length ==0 %}
    CURRENT_DATE + interval '1 days' * s.idx
    {% else %}
    TO_DATE('{{start_date}}', 'YYYY-MM-DD') + interval '1 days' * s.idx
    {% endif %}
     AS {{name}}
{% endmacro %}

{% macro snowflake__column_date_sequence(name, start_date='') %}
    {% if start_date|length ==0 %}
    dateadd(day, '-' || seq4(), current_date())
    {% else %}
    dateadd(day, '-' || seq4(), '{{start_date}}')
    {% endif %}
     AS {{name}}
{% endmacro%}