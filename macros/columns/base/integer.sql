{% macro synth_column_integer(name, min, max, distribution) -%}
    {{ synth_distribution_discrete_uniform(min=min, max=max) }} as {{name}}
{%- endmacro %}