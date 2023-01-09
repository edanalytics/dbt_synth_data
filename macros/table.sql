{% macro synth_table(rows=1000) -%}
    {% set ctes = synth_retrieve('ctes') %}
    {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %}
    
    base as (
        select
            row_number() over (order by 1) as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ),
    join0 as (
        select
            base.__row_number,
            {% set base_fields = synth_retrieve('base_fields') %}
            {{ base_fields.values() | list | join(",") }}
        from base
    ),
    {% set joins = synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        join{{counter}} as (
            select
                join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", "join"+(counter-1)|string) }}
            from join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", "join"+(counter-1)|string) }}
        ),
    {% endfor %}
    synth_table as (
        select
            {% set final_fields = synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from join{{joins|length}}
    )
    {{ config(post_hook=synth_get_post_hooks())}}
{%- endmacro %}


{% macro default__synth_table_generator(rows) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_table_generator(rows) %}
    generate_series( 1, {{rows}} ) as s
{% endmacro %}

{% macro postgres__synth_table_generator(rows) %}
    generate_series( 1, {{rows}} ) as s(idx)
{% endmacro %}

{% macro snowflake__synth_table_generator(rows) %}
    table(generator( rowcount => {{rows}} ))
{% endmacro %}