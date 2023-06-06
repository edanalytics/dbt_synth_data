{% macro synth_column_boolean(name, pct_true=0.5) -%}
    {% set base_field %}
        CASE 
            WHEN {{ dbt_synth_data.synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{pct_true}} THEN TRUE
            ELSE FALSE
        END as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}
