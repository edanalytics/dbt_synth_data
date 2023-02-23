{% macro synth_column_select(name, model_name, value_cols=[], distribution="uniform", weight_col="", filter="", do_ref=True) -%}
    {# Allow for `value_cols` to be a single (string) column name: #}
    {% if value_cols is string %}{% set value_cols = [value_cols] %}{% endif %}
    
    {% if distribution=='uniform' %}
        {{ synth_column_select_uniform(name, model_name, value_cols, filter, do_ref) }}
    
    {% elif distribution=='weighted' %}
        {{ synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref) }}
    
    {% else %}
        {{ exceptions.raise_compiler_error("Invalid `distribution` " ~ distribution ~ " for select column `" ~ name ~ "`: should be `uniform` (default) or `weighted`.") }}
    {% endif %}
{%- endmacro %}

{% macro synth_column_select_uniform(name, model_name, value_cols, filter, do_ref) %}
    {% set table_name = synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    {% set cte %}
        {{table_name}}__{{name}}__cte as (
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                1.0*( (row_number() over (order by {{value_cols[0]}} asc)) - 1 ) / count(*) over () as from_val,
                1.0*( (row_number() over (order by {{value_cols[0]}} asc))     ) / count(*) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        )
    {% endset %}
    {{ synth_store("ctes", table_name+"__"+name+"__cte", cte) }}

    {% set base_field %}
        {{ synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_name}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_name}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{name}}__cte.from_val and {{name}}__cte.to_val
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{% endmacro %}

{% macro synth_column_select_weighted(name, model_name, value_cols, weight_col, filter, do_ref) %}
    {% set table_name = synth_retrieve('synth_conf')['table_name'] or "synth_table" %}
    {% if not weight_col %}
        {{ exceptions.raise_compiler_error("`weight_col` is required when `distribution` for select column `" ~ name ~ "` is `weighted`.") }}
    {% endif %}
    
    {% set cte %}
        {{table_name}}__{{name}}__cte as (
            select
                {% for value_col in value_cols %}
                {{value_col}},
                {% endfor %}
                {{weight_col}},
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc) - {{weight_col}}) / sum({{weight_col}}) over () as from_val,
                ( sum({{weight_col}}) over (order by {{weight_col}} desc, {{value_cols[0]}} asc)                 ) / sum({{weight_col}}) over () as to_val
            from {% if do_ref %}{{ref(model_name)}}{% else %}{{model_name}}{% endif %}
            {% if filter|trim|length %}
            where {{filter}}
            {% endif %}
            order by from_val asc, to_val asc
        )
    {% endset %}
    {{ synth_store("ctes", table_name+"__"+name+"__cte", cte) }}

    {% set base_field %}
      {{ synth_distribution_continuous_uniform(min=0, max=1) }} as {{name}}__rand
    {% endset %}
    {{ synth_store("base_fields", name+"__rand", base_field) }}

    {% set join_fields %}
        {% if value_cols|length==1 %}
            {{table_name}}__{{name}}__cte.{{value_cols[0]}} as {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{table_name}}__{{name}}__cte.{{value_col}} as {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor%}
        {% endif %}
    {% endset %}
    {% set join_clause %}
        left join {{table_name}}__{{name}}__cte on ___PREVIOUS_CTE___.{{name}}__rand between {{name}}__cte.from_val and {{name}}__cte.to_val
    {% endset %}
    {{ synth_store("joins", name+"__cte", {"fields": join_fields, "clause": join_clause} ) }}
    
    {% set final_field %}
        {% if value_cols|length==1 %}
            {{name}}
        {% else %}
            {% for value_col in value_cols %}
            {{name}}__{{value_col}}
            {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    {% endset %}
    {{ synth_store("final_fields", name, final_field) }}
{% endmacro %}