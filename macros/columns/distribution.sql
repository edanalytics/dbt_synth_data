{% macro synth_column_distribution(name, distribution) -%}
    {% set base_field %}
        {{distribution}} as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}
