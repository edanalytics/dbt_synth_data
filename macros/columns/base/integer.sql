{% macro synth_column_integer(name, distribution=None, min=None, max=None) -%}
    {% if distribution is none and min is none and max is none %}
        {{ exceptions.raise_compiler_error("integer column `" + name + "` must specify either `min` and `max` (for a uniform distribution) or `distribution` (your own distribution of integers)") }}
    
    {% elif min is none and max is none %}
        {{ distribution }}
    
    {% elif distribution is none %}
        {{ synth_distribution_discretize_floor(
            distribution=synth_distribution_continuous_uniform(min=min, max=max+1)
        ) }}
    
    {% endif %} as {{name}}
{%- endmacro %}