{% macro synth_date(min='1990-01-01', max='', distribution='uniform') -%}
    {{ return(adapter.dispatch('synth_date')(min, max, distribution)) }}
{%- endmacro %}

{% macro default__synth_date(min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_date(min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

{% macro snowflake__synth__date(min, max, distribution) %}
    dateadd(
        day,
        UNIFORM(
            0,
            datediff(day, '{{min}}'::date, '{{max}}'::date),
            RANDOM( {{ synth_get_randseed() }} )),
        '{{min}}'::date
    )
{% endmacro%}