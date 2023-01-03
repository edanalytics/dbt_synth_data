{% macro synth_phone_number() -%}
    {{ return(adapter.dispatch('synth_phone_number')()) }}
{%- endmacro %}

{% macro default__synth_phone_number() -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_phone_number() %}
    '(' || LPAD((floor(RANDOM() * 899 + 100)::int)::varchar, 3, '0')
    || ') ' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 3, '0')
    || '-' || LPAD((floor(RANDOM() * 998 + 1)::int)::varchar, 4, '0')
{% endmacro %}

{% macro snowflake__synth_phone_number() %}
    '(' || LPAD(UNIFORM(100, 999, RANDOM( {{synth_get_randseed()}} )), 3, '0')
    || ') ' || LPAD(UNIFORM(1, 999, RANDOM( {{synth_get_randseed()}} )), 3, '0')
    || '-' || LPAD(UNIFORM(1, 9999, RANDOM( {{synth_get_randseed()}} )), 4, '0')
{% endmacro%}