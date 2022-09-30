{% macro get_randseed() %}
    {#
        Kinda hokey, but we want to pass a different - but repeatable - random seed to each column
        so they're consistent across runs. Here we add a `rand_seed` key to the `builtins` object and
        then  increment it every time another column is added.
    #}
    {%- if not builtins.get("rand_seed") -%}
    {%- do builtins.update({"rand_seed": 10000}) -%}
    {%- set next_rand_seed = 10000 -%}
    {%- else -%}
    {%- set next_rand_seed = builtins.get("rand_seed")|int + 1 -%}
    {%- do builtins.update({"rand_seed": next_rand_seed}) -%}
    {%- endif -%}
    {{ return(next_rand_seed) }}
{% endmacro%}

{%- macro get_post_hooks() -%}
    {% set posthooks %}
    {% if builtins.get("posthooks") %}
    {% for posthook in builtins.get("posthooks") %}
        {{ posthook }};
    {% endfor %}
    {% endif %}
    {% endset %}
    {{ return(posthooks) }}
{%- endmacro %}

{%- macro add_post_hook(posthook) -%}
    {%- set posthooks = builtins.get("posthooks") or [] -%}
    {{ posthooks.append(posthook) or "" }}
    {%- do builtins.update({"posthooks": posthooks}) -%}
{%- endmacro %}