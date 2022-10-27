{% macro column_phone_number(name) -%}
    {{ return(adapter.dispatch('column_phone_number')(name)) }}
{%- endmacro %}

{% macro default__column_phone_number(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__column_phone_number(name) %}
    '(' || LPAD((floor(RANDOM() * 899 + 100)::int)::varchar, 3, '0')
    || ') ' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 3, '0')
    || '-' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 4, '0') as {{name}}
{% endmacro %}

{% macro snowflake__column_phone_number(name) %}
    '(' || LPAD(UNIFORM(100, 999, RANDOM( {{get_randseed()}} )), 3, '0')
    || ') ' || LPAD(UNIFORM(1, 999, RANDOM( {{get_randseed()}} )), 3, '0')
    || '-' || LPAD(UNIFORM(1, 9999, RANDOM( {{get_randseed()}} )), 4, '0') as {{name}}
{% endmacro%}