{% macro synth_column_date_sequence(name, start_date='', step=1) -%}
    {{ return(adapter.dispatch('synth_column_date_sequence')(name, start_date, step)) }}
{%- endmacro %}

{% macro default__synth_column_date_sequence(name, start_date, step=1) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_date_sequence(name, start_date, step) %}
    {% if start_date|length ==0 %}
    (CURRENT_DATE + interval '{{step}} days' * s.idx)::date
    {% else %}
    (TO_DATE('{{start_date}}', 'YYYY-MM-DD') + interval '{{step}} days' * s.idx)::date
    {% endif %}
     AS {{name}}
{% endmacro %}

{% macro snowflake__synth_column_date_sequence(name, start_date, step) %}
    {% if start_date|length ==0 %}
    dateadd(day, {{step}}*seq4(), current_date())
    {% else %}
    dateadd(day, {{step}}*seq4(), '{{start_date}}')
    {% endif %}
     AS {{name}}
{% endmacro%}