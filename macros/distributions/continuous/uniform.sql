{% macro synth_distribution_continuous_uniform(min=0, max=1, precision=-1) -%}
    {{ return(adapter.dispatch('synth_distribution_continuous_uniform')(min, max, precision)) }}
{%- endmacro %}

{% macro default__synth_distribution_continuous_uniform(min, max, precision) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_distribution_continuous_uniform(min, max, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    (random() * ({{max}}-{{min}}) + {{min}})
    {%- if precision>=0 -%} ::numeric , {{precision}}) {%- endif -%}
{% endmacro %}

{% macro snowflake__synth_distribution_continuous_uniform(min, max, precision) %}
    {%- if precision>=0 -%}round( {%- endif -%}
    UNIFORM({{min}}::float, {{max}}::float, RANDOM( synth_get_randseed() ))
    {%- if precision>=0 -%}  , {{precision}} ) {%- endif -%}
{% endmacro %}