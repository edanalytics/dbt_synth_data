{% macro column_boolean(name, pct_true) -%}
    CASE 
        WHEN {{ dbt_synth.distribution_continuous_uniform() }} < {{pct_true}} THEN TRUE
        ELSE FALSE
    END AS {{name}}
{%- endmacro %}
