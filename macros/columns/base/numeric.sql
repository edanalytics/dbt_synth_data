{% macro column_numeric(name, min, max, precision=5) -%}
    {{ dbt_synth.distribution_continuous_uniform(min=min, max=max, precision=precision) }} as {{name}}
{%- endmacro %}
