{% macro synth_distribution_discrete_probabilities(probabilities) %}
    {# Set up some variables: #}
    {%- set epsilon = 0.00001 -%}{# "close enough" to zero #}
    {%- set ns = namespace(max_prob_digits=1, keys=[], values=[], curr_idx=0, curr_threshold=0.0) -%}
    
    {# Set up probability cutoff values and return keys: #}
    {%- if probabilities is mapping -%}{#- dict -#}
        {%- set ns.keys = probabilities.keys()|list -%}
        {%- set ns.values = probabilities.values()|list -%}
    {%- elif probabilities is iterable -%}{#- list -#}
        {%- set ns.keys = range(probabilities|length) -%}
        {%- set ns.values = probabilities -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("`probabilities` must be a list or dict") }}
    {%- endif -%}

    {%- if (1.0 - ns.values|sum)|abs > epsilon -%}
        {{ exceptions.raise_compiler_error("`probabilities` must sum to 1.0, not " + ns.values|sum|string) }}
    {%- endif -%}

    {%- if ns.keys[0] is number -%}
        {% set wrap = "" %}
    {% elif ns.keys[0] is string %}
        {% set wrap = "'" %}
    {% else %}
        {{ exceptions.raise_compiler_error("`probabilities` keys must be strings or numbers") }}
    {% endif %}

    {%- set ns.curr_threshold = ns.values[0] -%}
    
    {# Find max number of digits in any specified probability: #}
    {%- for i in range(probabilities|length) -%}
        {%- if ns.values[i]|string|replace("0.","")|replace(".","")|length > ns.max_prob_digits -%}
        {%- set ns.max_prob_digits = ns.values[i]|string|replace("0.","")|replace(".","")|length -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if ns.max_prob_digits > 4 -%}
        {{ exceptions.raise_compiler_error("`probabilities` should not exceed 4 digits (for performance reasons, see docs)") }}
    {%- endif -%}
    
    {# Case statement on uniformly-distributed range: #}
    case {{ dbt_synth_data.synth_distribution_discretize_floor( distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=10**ns.max_prob_digits) ) }}
        {% for i in range(0, 10**ns.max_prob_digits) %}
        {%- if i >= ((10**ns.max_prob_digits)*ns.curr_threshold)|int and ns.curr_idx<probabilities|length-1 -%}
            {%- set ns.curr_idx = ns.curr_idx + 1 -%}
            {%- set ns.curr_threshold = ns.curr_threshold + ns.values[ns.curr_idx] -%}
        {%- endif -%}
        when {{i}} then {{wrap}}{{ns.keys[ns.curr_idx]}}{{wrap}}
        {% endfor %}
    end
{% endmacro %}