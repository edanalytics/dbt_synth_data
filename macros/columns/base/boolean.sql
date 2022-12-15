{% macro synth_column_boolean(name, pct_true) -%}
    CASE 
        WHEN {{ synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{pct_true}} THEN TRUE
        ELSE FALSE
    END AS {{name}}
{%- endmacro %}
