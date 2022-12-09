{% macro synth_distribution_continuous_normal(mean=0, stddev=1, precision=-1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_normal')(mean, stddev, precision)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_normal(mean, stddev, precision) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_distribution_continuous_normal(mean, stddev, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
    {%- if precision>=0 -%} ::numeric , {{precision}}) {%- endif -%}
{% endmacro %}

{% macro snowflake__synth_distribution_continuous_normal(mean, stddev, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    NORMAL({{mean}}::float, {{stddev}}::float, RANDOM( synth_get_randseed() ))
    {%- if precision>=0 -%}  , {{precision}} ) {%- endif -%}
{% endmacro %}