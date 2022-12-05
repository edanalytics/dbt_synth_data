{#-
    Creates a column with a `class` ("continuous" or "discrete") distribution of numbers - `type` determines
    the distribution ("uniform", "normal", "exponential", "bernoulli", "binomial", or "probabilities").
    Depending on the `type` chosen, other parameters may also be required.
-#}
{% macro column_distribution(name, class, type, min=0, max=1, mean=0, stddev=1, lambda=1.0, precision=-1, n=10, p=0.5, probabilities=None) %}
    {% if class=='discrete' %}
    {{ dbt_synth.distribution_discrete(type, min=min, max=max, mean=mean, stddev=stddev, n=n, p=p, probabilities=probabilities) }}
    {% elif class=='continuous' %}
    {{ dbt_synth.distribution_continuous(type, min=min, max=max, mean=mean, stddev=stddev, lambda=lambda) }}
    {% else %}
    {{ exceptions.raise_compiler_error("Invalid distribution class specified (discrete|continuous).") }}
    {% endif %} as {{name}}
{% endmacro %}