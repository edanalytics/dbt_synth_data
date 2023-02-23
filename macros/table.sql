{% macro synth_table(rows=1000) -%}
    {# Load CTE name (to support multiple synth CTEs in one model) #}
    {% set table_name = synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    
    {% set ctes = synth_retrieve('ctes') %}
    {{ ctes.values() | list | join(",") }}
    {% if ctes|length > 0%},{% endif %}
    
    {{name}}__base as (
        select
            {{ adapter.dispatch('synth_table_rownum')() }} as __row_number
        from {{ adapter.dispatch('synth_table_generator')(rows) }}
    ),
    {{name}}__join0 as (
        select
            base.__row_number,
            {% set base_fields = synth_retrieve('base_fields') %}
            {{ base_fields.values() | list | join(",") }}
        from base
    ),
    {% set joins = synth_retrieve('joins').values() | list %}
    {% for counter in range(1,joins|length+1) %}
        {{name}}__join{{counter}} as (
            select
                {{name}}__join{{counter-1}}.*
                {% if joins[counter-1]['fields']|length>0 %},{% endif %}
                {{ joins[counter-1]['fields'] | replace("___PREVIOUS_CTE___", name+"__join"+(counter-1)|string) }}
            from {{name}}__join{{counter-1}}
                {{ joins[counter-1]['clause'] | replace("___PREVIOUS_CTE___", anme+"__join"+(counter-1)|string) }}
        ),
    {% endfor %}
    {{name}} as (
        select
            {% set final_fields = synth_retrieve('final_fields').values() | list %}
            {% for final_field in final_fields %}
                {{ final_field | replace("___PREVIOUS_CTE___", "join"+joins|length|string) }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{name}}__join{{joins|length}}
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



{% macro default__synth_table_rownum() -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_table_rownum() %}
    row_number() over (order by 1)
{% endmacro %}

{% macro postgres__synth_table_rownum() %}
    s.idx
{% endmacro %}

{% macro snowflake__synth_table_rownum() %}
    seq8()
{% endmacro %}