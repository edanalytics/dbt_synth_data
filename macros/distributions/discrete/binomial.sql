{% macro distribution_discrete_binomial(n=10, p=0.5) -%}
    {%- if p<0 or p>1 -%}
    {{ exceptions.raise_compiler_error("`p` for a binomial distribution must be in the range [0,1]") }}
    {%- endif -%}

    {%- if n<1 -%}
    {{ exceptions.raise_compiler_error("`n` for a binomial distribution must be at least 1") }}
    
    {%- elif n==1 -%}
    {{ dbt_synth.distribution_discrete_bernoulli(p=p) }}
    
    {%- else -%}
    {#-
        We can approximate a binomial distribution using a normal distribution (see
        https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation).

        Note that for very "wide" binomial distributions (large `n`) or "skew" binomial distributions (extreme `p`),
        normally-distributed values may be <0 or >n, which is impossible in a binomial distribution. These long-tail
        values are rare, so, while not completely correct, we use
        * abs() to eliminate those <0
        * mod(..., n+1) to eliminate those >n
        However, this trick does artificially increase small values slightly, and therefore does not produce a
        completely accurate binomial distribution. It's probably good enough, though.
    -#}
    mod( abs( {{ dbt_synth.distribution_continuous_normal(mean=n*p, stddev=n*p*(1-p), precision=0) }} ) , {{n+1}} )

    {%- endif -%}
{%- endmacro %}