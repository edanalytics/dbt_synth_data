{% macro synth_distribution_discrete_binomial(n=10, p=0.5) -%}
    {%- if p<0 or p>1 -%}
    {{ exceptions.raise_compiler_error("`p` for a binomial distribution must be in the range [0,1]") }}
    {%- endif -%}

    {%- if n<1 -%}
    {{ exceptions.raise_compiler_error("`n` for a binomial distribution must be at least 1") }}
    
    {%- elif n==1 -%}
    {{ dbt_synth.distribution_discrete_bernoulli(p=p) }}
    
    {%- else -%}
    mod( abs( {{ synth_discretize_round(
        distribution=synth_distribution_continuous_normal(mean=n*p, stddev=(n*p*(1-p))**0.5),
        precision=0
    ) }} ) , {{n+1}} )

    {%- endif -%}
{%- endmacro %}