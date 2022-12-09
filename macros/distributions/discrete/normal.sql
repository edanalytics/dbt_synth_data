{% macro synth_distribution_discrete_normal(mean=0, stddev=1, precision=0) -%}
    {{ synth_distribution_continuous_normal(mean=mean, stddev=stddev, precision=precision) }}
{%- endmacro %}