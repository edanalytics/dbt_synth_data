{% macro distribution(name, class, type, min=0, max=1, mean=0, stddev=1, lambda=1.0, precision=-1, n=10, p=0.5, probabilities=None, values=None, weights=None, wrap="") %}
    {% if class=='discrete' %}
    {{ dbt_synth.distribution_discrete(type, min=min, max=max, mean=mean, stddev=stddev, n=n, p=p, probabilities=probabilities, wrap=wrap, values=values, weights=weights, precision=precision) }}
    {% elif class=='continuous' %}
    {{ dbt_synth.distribution_continuous(type, min=min, max=max, mean=mean, stddev=stddev, lambda=lambda) }}
    {% else %}
    {{ exceptions.raise_compiler_error("Invalid distribution class specified (discrete|continuous).") }}
    {% endif %}
{% endmacro %}

{#- This macro simply does routing, so you can call distribution_continuous('normal', ...) or distribution_continuous_normal(...) -#}
{% macro distribution_continuous(type, min=0, max=1, mean=0, stddev=1, lambda=1.0, precision=-1) -%}
    {%- if type=='uniform' -%}
    {{ dbt_synth.distribution_continuous_uniform(min=min, max=max) }}
    {%- elif type=='normal' -%}
    {{ dbt_synth.distribution_continuous_normal(mean=mean, stddev=stddev) }}
    {%- elif type=='exponential' -%}
    {{ dbt_synth.distribution_continuous_exponential(lambda=lambda) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid continuous distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}

{#- This macro simply does routing, so you can call distribution_discrete('normal', ...) or distribution_discrete_normal(...) -#}
{% macro distribution_discrete(type, min=0, max=1, mean=0, stddev=1, lambda=1.0, precision=0, n=10, p=0.5, probabilities=None, wrap="", values=None, weights=None) -%}
    {%- if type=='uniform' -%}
    {{ dbt_synth.distribution_discrete_uniform(min=min, max=max, precision=precision) }}
    {%- elif type=='normal' -%}
    {{ dbt_synth.distribution_discrete_normal(mean=mean, stddev=stddev, precision=precision) }}
    {%- elif type=='exponential' -%}
    {{ dbt_synth.distribution_discrete_exponential(lambda=lambda, precision=precision) }}
    {%- elif type=='bernoulli' -%}
    {{ dbt_synth.distribution_discrete_bernoulli(p=p) }}
    {%- elif type=='binomial' -%}
    {{ dbt_synth.distribution_discrete_binomial(n=n, p=p) }}
    {%- elif type=='probabilities' -%}
    {{ dbt_synth.distribution_discrete_probabilities(probabilities=probabilities, wrap=wrap) }}
    {%- elif type=='weights' -%}
    {{ dbt_synth.distribution_discrete_weights(values=values, weights=weights) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid discrete distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}