{% macro synth_column_select(name, value_col, lookup_table, distribution="uniform", weight_col="", filter="", funcs=[]) -%}
    {% if distribution=='uniform' %}
        {{ synth_column_select_uniform(name, value_col, lookup_table, filter, funcs) }}
    
    {% elif distribution=='weighted' %}
        {% if weight_col=='' %}
            {{ exceptions.raise_compiler_error("`weight_col` is required when `distribution` for select column `" ~ name ~ "` is `weighted`.") }}
        {% endif %}
        {{ synth_column_select_weighted(name, value_col, lookup_table, weight_col, filter, funcs) }}
    
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `distribution` " ~ distribution ~ " for select column `" ~ name ~ "`: should be `uniform` (default) or `weighted`.") }}
    {% endif %}
{%- endmacro %}

{% macro synth_column_select_uniform(name, value_col, lookup_table, filter, funcs, final_expression="") %}
    {% set cte %}
        {{name}}__cte as (
            select
                {{value_col}},
                ( (row_number() over (order by {{value_col}} asc)) - 1 )::double precision / count(*) over () as from_val,
                ( (row_number() over (order by {{value_col}} asc))     )::double precision / count(*) over () as to_val
            from {{ref(lookup_table)}}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        )
    {% endset %}
    {{ synth_store("ctes", name+"__cte", cte) }}

    {% set base_field %}
      {{ synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
      {{name}}__cte.{{value_col}} as {{name}}
    {% endset %}
    {% set join_clause %}
      left join {{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{name}}__cte.from_val and {{name}}__cte.to_val
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
      {{name}}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{% endmacro %}

{% macro synth_column_select_weighted(name, value_col, lookup_table, weight_col, filter, funcs) %}
    {% set cte %}
        {{name}}__cte as (
            select
                {% for f in funcs %}{{f}}({% endfor %}{{value_col}}{% for f in funcs %}){% endfor %} as {{value_col}},
                {{weight_col}},
                ( (sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc)) - {{weight_col}}::double precision) / sum({{weight_col}}::double precision) over () as from_val,
                ( (sum({{weight_col}}::double precision) over (order by {{weight_col}} desc, {{value_col}} asc))                                   ) / sum({{weight_col}}::double precision) over () as to_val
            from {{ this.database }}.{{ this.schema }}.{{lookup_table}}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        )
    {% endset %}
    {{ synth_store("ctes", name+"__cte", cte) }}

    {% set base_field %}
      {{ synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
      {{name}}__cte.{{value_col}} as {{name}}
    {% endset %}
    {% set join_clause %}
      left join {{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{name}}__cte.from_val and {{name}}__cte.to_val
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
      {{name}}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{% endmacro %}