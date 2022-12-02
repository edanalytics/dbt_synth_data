{#-
    `probabilities` may be
    * a list [0.05, 0.8, 0.15], in which case the (zero-based) indices will be returned with the specified probabilities
    * a dictionary with integer keys { "1": 0.05, "3": 0.8, "7": 0.15 }, in which case the keys will be returned with
      the specified probabilities
-#}
{#- TODO: not working!!! (...due to random() being inside a subquery...) -#}
{% macro distribution_discrete_probabilities(probabilities) %}
    {%- if probabilities is mapping -%}{#- dict -#}
    {%- set ns = namespace(threshold=0.0) -%}
    ( select m.k from (VALUES
        {%- for k,v in probabilities.items() -%}
        {%- set ns.threshold = ns.threshold + v -%}
        ({{k}}, {{ns.threshold}}){% if not loop.last %}, {% endif %}
        {%- endfor -%}
        ) AS m (k,v) inner join ( select {{ dbt_synth.distribution_continuous_uniform() }} as value) rand on rand.value<m.v limit 1 )

    {%- elif probabilities is iterable -%}{#- list -#}
    {%- set ns = namespace(threshold=0.0) -%}
    ( select m.k from (VALUES
        {%- for i in range(probabilities|length) -%}
        {%- set ns.threshold = ns.threshold + probabilities[i] -%}
        ({{i}}, {{ns.threshold}}){% if not loop.last %}, {% endif %}
        {%- endfor -%}
        ) AS m (k,v) inner join ( select {{ dbt_synth.distribution_continuous_uniform() }} as value) rand on rand.value<m.v limit 1 )

    {%- else -%}
    {{ exceptions.raise_compiler_error("`probabilities` must be a list or dict") }}
    {%- endif -%}
{% endmacro %}