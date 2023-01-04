{% macro synth_column_phone_number(name) -%}
    {{ return(adapter.dispatch('synth_column_phone_number')(name)) }}
{%- endmacro %}

{% macro default__synth_column_phone_number(name) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_phone_number(name) %}
    '(' || LPAD((floor(RANDOM() * 899 + 100)::int)::varchar, 3, '0')
    || ') ' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 3, '0')
    || '-' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 4, '0')
    as {{name}}
{% endmacro %}

{% macro snowflake__synth_column_phone_number(name) %}
    '(' || LPAD(UNIFORM(100, 999, RANDOM( {{synth_get_randseed()}} )), 3, '0')
    || ') ' || LPAD(UNIFORM(1, 999, RANDOM( {{synth_get_randseed()}} )), 3, '0')
    || '-' || LPAD(UNIFORM(1, 9999, RANDOM( {{synth_get_randseed()}} )), 4, '0')
    as {{name}}
{% endmacro%}