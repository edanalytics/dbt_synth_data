{% macro synth_distribution(name, class, type, min=0, max=1, mean=0, stddev=1, lambda=1.0, mu=0.0, b=1.0, x0=0.0, gamma=1.0, n=10, p=0.5, probabilities=None, values=None, weights=None) %}
    {% if class=='continuous' %}
    {{ synth_distribution_continuous(type, min=min, max=max, mean=mean, stddev=stddev, lambda=lambda, mu=mu, b=b, x0=x0, gamma=gamma) }}
    {% elif class=='discrete' %}
    {{ synth_distribution_discrete(type, n=n, p=p, probabilities=probabilities, values=values, weights=weights) }}
    {% else %}
    {{ exceptions.raise_compiler_error("Invalid distribution class " + class + " specified; should be `discrete` or `continuous`.") }}
    {% endif %}
{% endmacro %}

{#- This macro simply does routing, so you can call distribution_continuous('normal', ...) or distribution_continuous_normal(...) -#}
{% macro synth_distribution_continuous(type, min=0, max=1, mean=0, stddev=1, lambda=1.0, mu=0.0, b=1.0, x0=0.0, gamma=1.0) -%}
    {%- if type=='uniform' -%}
    {{ synth_distribution_continuous_uniform(min=min, max=max) }}
    {%- elif type=='normal' -%}
    {{ synth_distribution_continuous_normal(mean=mean, stddev=stddev) }}
    {%- elif type=='exponential' -%}
    {{ synth_distribution_continuous_exponential(lambda=lambda) }}
    {%- elif type=='laplace' -%}
    {{ synth_distribution_continuous_laplace(mu=mu, b=b) }}
    {%- elif type=='cauchy' -%}
    {{ synth_distribution_continuous_cauchy(x0=x0, gamma=gamma) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid continuous distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}

{#- This macro simply does routing, so you can call distribution_discrete('normal', ...) or distribution_discrete_normal(...) -#}
{% macro synth_distribution_discrete(type, n=10, p=0.5, probabilities=None, values=None, weights=None) -%}
    {%- if type=='bernoulli' -%}
    {{ synth_distribution_discrete_bernoulli(p=p) }}
    {%- elif type=='binomial' -%}
    {{ synth_distribution_discrete_binomial(n=n, p=p) }}
    {%- elif type=='probabilities' -%}
    {{ synth_distribution_discrete_probabilities(probabilities=probabilities) }}
    {%- elif type=='weights' -%}
    {{ synth_distribution_discrete_weights(values=values, weights=weights) }}
    {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid discrete distribution type specified.") }}
    {%- endif -%}
{%- endmacro %}