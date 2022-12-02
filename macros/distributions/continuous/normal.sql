{% macro distribution_continuous_normal(mean=0, stddev=1, precision=-1) -%}
    {{ return(adapter.dispatch('distribution_continuous_normal')(get_randseed(), mean, stddev, precision)) }}
{%- endmacro %}

{% macro default__distribution_continuous_normal(randseed, mean, stddev, precision) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__distribution_continuous_normal(randseed, mean, stddev, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
    {%- if precision>=0 -%} ::numeric , {{precision}}) {%- endif -%}
{% endmacro %}

{% macro snowflake__distribution_continuous_normal(randseed, mean, stddev, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    NORMAL({{mean}}::float, {{stddev}}::float, RANDOM( {{randseed}} ))
    {%- if precision>=0 -%}  , {{precision}} ) {%- endif -%}
{% endmacro %}