{% macro synth_distribution_discrete_probabilities(probabilities) %}
    {# Set up some variables: #}
    {%- set epsilon = 0.00001 -%}{# "close enough" to zero #}

    {# Build plain local lists for keys and values — namespace() can't hold lists
       reliably in dbt-fusion's Jinja runtime (Rust-based), so we use mutable
       local lists and keep namespace() only for scalar mutable state. #}
    {%- set prob_keys = [] -%}
    {%- set prob_vals = [] -%}

    {# Set up probability cutoff values and return keys: #}
    {%- if probabilities is mapping -%}{#- dict -#}
        {%- for k, v in probabilities.items() -%}
            {%- do prob_keys.append(k) -%}
            {%- do prob_vals.append(v) -%}
        {%- endfor -%}
    {%- elif probabilities is iterable -%}{#- list -#}
        {%- for v in probabilities -%}
            {%- do prob_keys.append(loop.index0) -%}
            {%- do prob_vals.append(v) -%}
        {%- endfor -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("`probabilities` must be a list or dict") }}
    {%- endif -%}

    {# Validate sum using explicit loop (|sum filter is unreliable on namespace-stored lists) #}
    {%- set prob_sum = namespace(val=0.0) -%}
    {%- for v in prob_vals -%}
        {%- set prob_sum.val = prob_sum.val + v -%}
    {%- endfor -%}
    {%- if (1.0 - prob_sum.val)|abs > epsilon -%}
        {{ exceptions.raise_compiler_error("`probabilities` must sum to 1.0, not " + prob_sum.val|string) }}
    {%- endif -%}

    {%- if prob_keys[0] is number -%}
        {% set wrap = "" %}
    {% elif prob_keys[0] is string %}
        {% set wrap = "'" %}
    {% else %}
        {{ exceptions.raise_compiler_error("`probabilities` keys must be strings or numbers") }}
    {% endif %}

    {# Find max number of digits in any specified probability: #}
    {%- set ns = namespace(max_prob_digits=1, curr_idx=0, curr_threshold=prob_vals[0]) -%}
    {%- for v in prob_vals -%}
        {%- if v|string|replace("0.","")|replace(".","")|length > ns.max_prob_digits -%}
            {%- set ns.max_prob_digits = v|string|replace("0.","")|replace(".","")|length -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if ns.max_prob_digits > 4 -%}
        {{ exceptions.raise_compiler_error("`probabilities` should not exceed 4 digits (for performance reasons, see docs)") }}
    {%- endif -%}

    {% if target.type == 'duckdb' %}
        {# DuckDB requires some gymnastics to prevent NULL values #}
        {% set value_list = [] %}
        {% for i in range(0, 10**ns.max_prob_digits) %}
            {%- if i >= ((10**ns.max_prob_digits)*ns.curr_threshold)|int and ns.curr_idx<prob_vals|length-1 -%}
                {%- set ns.curr_idx = ns.curr_idx + 1 -%}
                {%- set ns.curr_threshold = ns.curr_threshold + prob_vals[ns.curr_idx] -%}
            {%- endif -%}
            {% do value_list.append(prob_keys[ns.curr_idx]) %}
        {% endfor %}
        ifnull(
            ({{ value_list }})[
                {{ dbt_synth_data.synth_distribution_discretize_floor(
                    distribution=dbt_synth_data.synth_distribution_continuous_uniform(
                        min=0,
                        max=10**ns.max_prob_digits
                    )
                ) }}
            ],
            {{wrap}}{{value_list[value_list|length - 1]}}{{wrap}}
        )
    {% else %}
        {# Case statement on uniformly-distributed range: #}
        case {{ dbt_synth_data.synth_distribution_discretize_floor( distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=10**ns.max_prob_digits) ) }}
            {% for i in range(0, 10**ns.max_prob_digits + 1) %}
            {%- if i >= ((10**ns.max_prob_digits)*ns.curr_threshold)|int and ns.curr_idx<prob_vals|length-1 -%}
                {%- set ns.curr_idx = ns.curr_idx + 1 -%}
                {%- set ns.curr_threshold = ns.curr_threshold + prob_vals[ns.curr_idx] -%}
            {%- endif -%}
            when {{i}} then {{wrap}}{{prob_keys[ns.curr_idx]}}{{wrap}}
            {% endfor %}
        end
    {% endif %}
{% endmacro %}
