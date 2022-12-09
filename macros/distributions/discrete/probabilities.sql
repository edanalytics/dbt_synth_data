{% macro synth_distribution_discrete_probabilities(probabilities, wrap="") %}
    {# Set up some variables: #}
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
    {%- if ns.values|sum!=1.0 -%}
        {{ exceptions.raise_compiler_error("`probabilities` must sum to 1.0") }}
    {%- endif -%}
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
    case {{ synth_distribution_discrete_uniform(min=0, max=(10**ns.max_prob_digits - 1)) }}
        {% for i in range(0, 10**ns.max_prob_digits) %}
        {%- if i >= ((10**ns.max_prob_digits)*ns.curr_threshold)|int and ns.curr_idx<probabilities|length-1 -%}
            {%- set ns.curr_idx = ns.curr_idx + 1 -%}
            {%- set ns.curr_threshold = ns.curr_threshold + ns.values[ns.curr_idx] -%}
        {%- endif -%}
        when {{i}} then {{wrap}}{{ns.keys[ns.curr_idx]}}{{wrap}}
        {% endfor %}
    end
{% endmacro %}