{% macro distribution_continuous_uniform(min=0, max=1, precision=-1) -%}
    {{ return(adapter.dispatch('distribution_continuous_uniform')(get_randseed(), min, max, precision)) }}
{%- endmacro %}

{% macro default__distribution_continuous_uniform(randseed, min, max, precision) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__distribution_continuous_uniform(randseed, min, max, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    (random() * ({{max}}-{{min}}) + {{min}})
    {%- if precision>=0 -%} ::numeric , {{precision}}) {%- endif -%}
{% endmacro %}

{% macro snowflake__distribution_continuous_uniform(randseed, min, max, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    UNIFORM({{min}}::float, {{max}}::float, RANDOM( {{randseed}} ))
    {%- if precision>=0 -%}  , {{precision}} ) {%- endif -%}
{% endmacro %}