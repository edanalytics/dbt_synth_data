{% macro column_integer(name, min, max, distribution) -%}
    {{ dbt_synth.distribution_discrete_uniform(min=min, max=max) }} as {{name}}
{%- endmacro %}