{% macro synth_column_integer(name, distribution=None, min=None, max=None) -%}
    {% if distribution is none and min is none and max is none %}
        {{ exceptions.raise_compiler_error("intever column `" + name + "` mus specify either `min` and `max` (for a uniform distribution) or `distribution` (your own distribution of integers)") }}
    {% elif min is none and max is none %}
        {{ distribution }} as {{name}}
    {% elif distribution is none %}
        {{ synth_discretize_floor( distribution=synth_distribution_continuous_uniform(min=min, max=max+1) ) }} as {{name}}
    {% endif %}
{%- endmacro %}