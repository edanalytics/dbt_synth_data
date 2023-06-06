{% macro synth_distribution_continuous_exponential(lambda=1.0) -%}
    {%- if lambda<=0 -%}
    {{ exceptions.raise_compiler_error("`lambda` for an exponential distribution must be greater than zero") }}
    {%- endif -%}

    -ln( abs ( {{ dbt_synth_data.synth_distribution_continuous_uniform() }} ) ) * (1/{{lambda}})
{%- endmacro %}

