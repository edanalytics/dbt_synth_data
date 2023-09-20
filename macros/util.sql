{#
    This macro is used in Snowflake to pass an incremented (different) random
    seed value to the RANDOM() function so we get different random values per
    row.

    The default behavior, if you pass no random seed or the same random seed
    to different RANDOM() calls, is you'll get the same random value, which is
    not what we want.

    This is implemented with sort of a hack, where we add a `rand_seed` key to
    the `target` object and then  increment it every time another column is
    added. The `target` object is one of few dbt objects that are *not*
    read-only, so we can store, read, and increment a key value on it.
#}
{% macro synth_get_randseed() %}
    {%- if not target.get("rand_seed") -%}
    {%- do synth_set_randseed(var('synth_randseed')) -%}
    {%- set next_rand_seed = var('synth_randseed') -%}
    {%- else -%}
    {%- set next_rand_seed = target.get("rand_seed")|int + 1 -%}
    {%- do target.update({"rand_seed": next_rand_seed}) -%}
    {%- endif -%}
    {{ return(next_rand_seed) }}
{% endmacro%}

{% macro synth_set_randseed(seed) %}
    {%- do target.update({"rand_seed": seed}) -%}
{% endmacro%}

{% macro synth_sqlite_random() %}
    {# convert SQLite RANDOM() int to 0.0-1.0 range #}
    {{ return('((RANDOM()+9223372036854775808)/2.0/9223372036854775808)') }}
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
{%- macro synth_add_update_hook(query) -%}
    {%- set updatehooks = target.get("updatehooks") or [] -%}
    {{ updatehooks.append(query) or "" }}
    {%- do target.update({"updatehooks": updatehooks}) -%}
{%- endmacro %}

{%- macro synth_add_cleanup_hook(query) -%}
    {%- set cleanuphooks = target.get("cleanuphooks") or [] -%}
    {{ cleanuphooks.append(query) or "" }}
    {%- do target.update({"cleanuphooks": cleanuphooks}) -%}
{%- endmacro %}

{%- macro synth_get_post_hooks() -%}
    {% set posthooks %}
    
    {% if target.get("updatehooks") %}
    {% for updatehook in target.get("updatehooks") | unique %}
        {{ updatehook }};
    {% endfor %}
    {% endif %}

    {% if target.get("cleanuphooks") %}
    {% for cleanuphook in target.get("cleanuphooks") | unique %}
        {{ cleanuphook }};
    {% endfor %}
    {% endif %}
    
    {% endset %}
    
    {{ return(posthooks) }}
{%- endmacro %}



{%- macro synth_set_table_name(name) -%}
    {{ dbt_synth_data.synth_store('synth_conf', 'table_name', name) }}
    {{ return("") }}
{%- endmacro %}

{%- macro synth_store(collection, key, value) -%}
    {%- set data = target.get(collection) or {} -%}
    {{ data.update({key: value}) or "" }}
    {%- do target.update({collection: data}) -%}
{%- endmacro %}

{%- macro synth_remove(collection, key) -%}
    {%- set data = target.get(collection) or {} -%}
    {{ data.pop(key) or "" }}
    {%- do target.update({collection: data}) -%}
    {{ return("") }}
{%- endmacro %}

{%- macro synth_retrieve(collection) -%}
    {{ return( target.get(collection) or {} ) }}
{%- endmacro %}



{# horizontally concatenates two CTEs with the same number of rows #}
{%- macro horizontal_union(cte_a, cte_b) -%}
    select *
    from (
        select *, row_number() over(order by 1) RowNumA
        from {{cte_a}}
    ) a
    join (
        select *, row_number() over(order by 1) RowNumB
        from {{cte_b}}
    ) b
    on a.RowNumA = b.RowNumB
{%- endmacro %}

{%- macro synth_zip(list_a, list_b) -%}
    {% set dct = {} %}
    {% for i in range(list_a|length) %}
        {% do dct.update({list_a[i]: list_b[i]}) %}
    {% endfor %}
    {{ return(dct) }}
{%- endmacro %}

{%- macro synth_initcap(string_expression) -%}
    {% set initcap_expression = "INITCAP(" + string_expression + ")" %}
    {% if target.type in ['sqlite', 'duckdb'] %}
        {% set initcap_expression = "UPPER(SUBSTR(" + string_expression + ", 1, 1)) || LOWER(SUBSTR(" + string_expression + ", 2))" %}
    {% endif %}
    {{ return(initcap_expression) }}
{%- endmacro %}