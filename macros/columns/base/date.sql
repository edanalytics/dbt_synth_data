{% macro synth_column_date(name, min='1990-01-01', max='', distribution='uniform') -%}
    {{ return(adapter.dispatch('synth_column_date')(name, min, max, distribution)) }}
{%- endmacro %}

{% macro default__synth_column_date(name, min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_date(name, min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_date(name, min, max, distribution) %}
    dateadd(
        day,
        UNIFORM(
            0,
            datediff(day, '{{min}}'::date, '{{max}}'::date),
            RANDOM( {{ synth_get_randseed() }} )),
        '{{min}}'::date
    ) as {{name}}
{% endmacro%}