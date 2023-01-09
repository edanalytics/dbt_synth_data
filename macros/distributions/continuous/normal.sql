{% macro synth_distribution_continuous_normal(mean=0, stddev=1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_normal')(mean, stddev)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_normal(mean, stddev) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( 1.0*{{stddev}} * sqrt(-2*log({{synth_sqlite_random()}}))*sin(2*pi()*{{synth_sqlite_random()}}) ) + 1.0*{{mean}} )
{% endmacro %}

{% macro postgres__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro snowflake__synth_distribution_continuous_normal(mean, stddev) %}
    NORMAL({{mean}}::float, {{stddev}}::float, RANDOM( {{ synth_get_randseed() }} ))
{% endmacro %}