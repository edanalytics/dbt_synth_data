{% macro synth_boolean(pct_true=0.5) -%}
    CASE 
        WHEN {{ synth_distribution_continuous_uniform(min=0.0, max=1.0) }} < {{pct_true}} THEN TRUE
        ELSE FALSE
    END
{%- endmacro %}
