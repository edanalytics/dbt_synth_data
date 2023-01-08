{% macro synth_column_integer(name, min=None, max=None) -%}
    {% if min is none or max is none %}
        {{ exceptions.raise_compiler_error("integer column `" + name + "` must specify `min` and `max`") }}
    {% endif %}
    
    {% set base_field %}
        {{ synth_distribution_discretize_floor(
            distribution=synth_distribution_continuous_uniform(min=min, max=max+1)
        ) }} as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{%- endmacro %}