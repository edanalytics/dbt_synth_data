{% macro synth_table(rows=1000) -%}
    {{ return(adapter.dispatch('synth_table')(rows)) }}
{% endmacro %}

{% macro default__synth_table(rows=1000) -%}
    
    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    
    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {# {{ ctes.values() | list | join(",") }} #}
    {% for name, cte in ctes.items() %}
        {{table_name}}__{{name}} as (
            {{cte}}
        ) {% if ctes|length > 0 %} , {% endif %}
    {% endfor %}
    
    {{table_name}}__base as (
        select
            {{ adapter.dispatch('synth_table_rownum')() }} as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ),
    {{table_name}}__join0 as (
        select
            {{table_name}}__base.__row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ),
    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {{table_name}}__join{{counter}} as (
            select
                {{table_name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
        ),
    {% endfor %}
    {{table_name}} as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}
{%- endmacro %}

{% macro sqlite__synth_table(rows=1000) %}
    
    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = dbt_synth_data.synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    
    {% set ctes = dbt_synth_data.synth_retrieve('ctes') %}
    {% for name, cte in ctes.items() %}
        {% set query %}
        drop table if exists {{table_name}}__{{name}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__{{name}} as
            {{cte}}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {# {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %} #}
    
    {% set query %}
    drop table if exists {{table_name}}__base;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    create temp table {{table_name}}__base as
        select
            {{ adapter.dispatch('synth_table_rownum')() }} as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    drop table if exists {{table_name}}__join0;
    {% endset %}
    {% do run_query(query) %}

    {% set query %}
    create temp table {{table_name}}__join0 as
        select
            {{table_name}}__base.__row_number
            {% set base_fields = dbt_synth_data.synth_retrieve('base_fields') %}
            {% if base_fields.values() | length > 0 %},{% endif %}
            {{ base_fields.values() | list | join(",") }}
        from {{table_name}}__base
    ;
    {% endset %}
    {% do run_query(query) %}

    {% set joins = dbt_synth_data.synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {% set query %}
        drop table if exists {{table_name}}__join{{counter}};
        {% endset %}
        {% do run_query(query) %}

        {% set query %}
        create temp table {{table_name}}__join{{counter}} as
            select
                {{table_name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
            from {{table_name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", table_name+"__join"+(counter-1)|string) }}
        ;
        {% endset %}
        {% do run_query(query) %}
    {% endfor %}
    {{table_name}} as (
        select
            {% set final_fields = dbt_synth_data.synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{table_name}}__join{{joins|length}}
    )
    {{ config(post_hook=dbt_synth_data.synth_get_post_hooks())}}
{% endmacro %}


{% macro default__synth_table_generator(rows) -%}
    generate_series( 1, {{rows}}, 1 ) as s
{%- endmacro %}

{% macro postgres__synth_table_generator(rows) %}
    generate_series( 1, {{rows}} ) as s(idx)
{% endmacro %}

{% macro snowflake__synth_table_generator(rows) %}
    table(generator( rowcount => {{rows}} ))
{% endmacro %}



{% macro default__synth_table_rownum() -%}
    row_number() over (order by NULL)
{%- endmacro %}

{% macro postgres__synth_table_rownum() %}
    s.idx
{% endmacro %}
