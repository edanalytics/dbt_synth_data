{% macro distribution_continuous_exponential(lambda=1.0, precision=-1) -%}
    {%- if lambda<=0 -%}
    {{ exceptions.raise_compiler_error("`lambda` for an exponential distribution must be greater than zero") }}
    {%- endif -%}

    {%- if precision>=0 -%}round( {%- endif -%}
    -ln( abs ( {{ dbt_synth.distribution_continuous_uniform() }} ) ) * (1/{{lambda}})
    {%- if precision>=0 -%} ::numeric , {{precision}}) {%- endif -%}
{%- endmacro %}

