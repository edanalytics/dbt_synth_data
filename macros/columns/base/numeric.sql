{% macro synth_column_numeric(name, min, max, precision=5) -%}
    {{ synth_distribution_continuous_uniform(min=min, max=max, precision=precision) }} as {{name}}
{%- endmacro %}
