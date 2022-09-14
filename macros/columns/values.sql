{% macro column_values(name, values, distribution) -%}
    {{ return(adapter.dispatch('column_values')(get_randseed(), name, values, distribution)) }}
{%- endmacro %}

{% macro default__column_values(randseed, name, values, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_values(randseed, name, values, distribution) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_values(randseed, name, values, distribution) %}
    CASE UNIFORM(1, {{values|length}}, RANDOM( {{randseed}} ))
        {% for i in range(0, values|length) %}
        WHEN {{i+1}} THEN '{{values[i]}}'
        {% endfor %}
    END AS {{name}}
{% endmacro%}