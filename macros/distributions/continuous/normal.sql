{% macro synth_distribution_continuous_normal(mean=0, stddev=1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_normal', 'dbt_synth_data')(mean, stddev)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_normal(mean, stddev) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( 1.0*{{stddev}} * sqrt(-2*log({{ dbt_synth_data.synth_sqlite_random() }}))*sin(2*pi()*{{ dbt_synth_data.synth_sqlite_random() }}) ) + 1.0*{{mean}} )
{% endmacro %}

{% macro duckdb__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro postgres__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    ( ( {{stddev}}::float * sqrt(-2*log(random()))*sin(2*pi()*random()) ) + {{mean}}::float )
{% endmacro %}

{% macro snowflake__synth_distribution_continuous_normal(mean, stddev) %}
    NORMAL({{mean}}::float, {{stddev}}::float, RANDOM( {{ dbt_synth_data.synth_get_randseed() }} ))
{% endmacro %}

{% macro bigquery__synth_distribution_continuous_normal(mean, stddev) %}
    {#- formula below is based on https://mathworld.wolfram.com/Box-MullerTransformation.html -#}
    {#- BigQuery: LN() is natural log (LOG() is base-10), there is no pi() so use ACOS(-1) -#}
    ( ( CAST({{stddev}} AS FLOAT64) * sqrt(-2*LN(RAND()))*sin(2*ACOS(-1)*RAND()) ) + CAST({{mean}} AS FLOAT64) )
{% endmacro %}