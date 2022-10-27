{% macro column_values(name, values, weights) -%}
    {{ return(adapter.dispatch('column_values')(get_randseed(), name, values, weights)) }}
{%- endmacro %}

{% macro default__column_values(randseed, name, values, weights) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_values(randseed, name, values, weights) %}
    CASE floor(RANDOM() * {{values|length}} + 1)
        {% for i in range(0, values|length) %}
        WHEN {{i+1}} THEN '{{values[i]}}'
        {% endfor %}
    END AS {{name}}
{% endmacro %}

{% macro snowflake__column_values(randseed, name, values, weights) %}
    CASE UNIFORM(1, {{values|length}}, RANDOM( {{randseed}} ))
        {% for i in range(0, values|length) %}
        WHEN {{i+1}} THEN '{{values[i]}}'
        {% endfor %}
    END AS {{name}}
{% endmacro%}