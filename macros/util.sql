{#
    This macro is used in Snowflake to pass an incremented (different) random
    seed value to the RANDOM() function so we get different random values per
    row.

    The default behavior, if you pass no random seed or the same random seed
    to different RANDOM() calls, is you'll get the same random value, which is
    not what we want.

    This is implemented with sort of a hack, where we add a `rand_seed` key to
    the `builtins` object and then  increment it every time another column is
    added. The `builtins` object is one of few dbt objects that are *not*
    read-only, so we can store, read, and increment a key value on it.
#}
{% macro get_randseed() %}
    {%- if not builtins.get("rand_seed") -%}
    {%- do builtins.update({"rand_seed": 10000}) -%}
    {%- set next_rand_seed = 10000 -%}
    {%- else -%}
    {%- set next_rand_seed = builtins.get("rand_seed")|int + 1 -%}
    {%- do builtins.update({"rand_seed": next_rand_seed}) -%}
    {%- endif -%}
    {{ return(next_rand_seed) }}
{% endmacro%}

{#
    The next two functions, `add_update_hook()` and `add_cleanup_hook()`, both
    allow you to add "hook" queries that will run after the initial table
    creation. A typical use-case is
    (1) generate a random value in a column in your table
    (2) use `add_update_hook()` to create another column with some value based
        on the random value from step 1;
    (3) clean up by deleting the original random value column from step 1 using
        `add_cleanup_hook()`
    
    After building your table, you should (nearly) always finish with
        {{ config(post_hook=dbt_synth.get_post_hooks())}}
    which will first run the update hooks and then run the cleanup hooks.
#}
{%- macro add_update_hook(query) -%}
    {%- set updatehooks = builtins.get("updatehooks") or [] -%}
    {{ updatehooks.append(query) or "" }}
    {%- do builtins.update({"updatehooks": updatehooks}) -%}
{%- endmacro %}

{%- macro add_cleanup_hook(query) -%}
    {%- set cleanuphooks = builtins.get("cleanuphooks") or [] -%}
    {{ cleanuphooks.append(query) or "" }}
    {%- do builtins.update({"cleanuphooks": cleanuphooks}) -%}
{%- endmacro %}

{%- macro get_post_hooks() -%}
    {% set posthooks %}
    
    {% if builtins.get("updatehooks") %}
    {% for updatehook in builtins.get("updatehooks") | unique %}
        {{ updatehook }};
    {% endfor %}
    {% endif %}

    {% if builtins.get("cleanuphooks") %}
    {% for cleanuphook in builtins.get("cleanuphooks") | unique %}
        {{ cleanuphook }};
    {% endfor %}
    {% endif %}
    
    {% endset %}
    
    {{ return(posthooks) }}
{%- endmacro %}

{%- macro zip(list_a, list_b) -%}
    {% set dct = {} %}
    {% for i in range(list_a|length) %}
        {% do dct.update({list_a[i]: list_b[i]}) %}
    {% endfor %}
    {{ return(dct) }}
{%- endmacro %}
