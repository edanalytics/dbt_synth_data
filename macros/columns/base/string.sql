{% macro synth_column_string(name, min_length=1, max_length=32) -%}
    {{ return(adapter.dispatch('synth_column_string_base')(name, min_length, max_length)) }}
{%- endmacro %}

{% macro default__synth_column_string_base(name, min_length, max_length) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_column_string_base(name, min_length, max_length) %}
    {% set cte %}
        {{name}}__cte(n, v, chars) as (
            select {{ synth_distribution_discretize_floor(
                    distribution=synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
                ) }}, '', '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
            UNION ALL
            SELECT n-1, v||SUBSTR(chars, {{ synth_distribution_discretize_floor(
                distribution=synth_distribution_continuous_uniform(min=0, max='LENGTH(chars)')
            ) }} + 1, 1), chars
            FROM {{name}}__cte
            WHERE n > 0
        )
    {% endset %}
    {{ synth_store("ctes", name+"__cte", cte) }}

    {% set base_field %}
        (SELECT v FROM {{name}}__cte WHERE n = 0) as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{% endmacro %}

{% macro postgres__synth_column_string_base(name, min_length, max_length) %}
    {% set base_field %}
    substr(
        translate(
            encode(
                decode(md5(random()::text), 'hex')
                || decode(md5(random()::text), 'hex')
                || decode(md5(random()::text), 'hex')
                || decode(md5(random()::text), 'hex'),
            'base64'),
        '/+=', ''),
        0,
        floor(RANDOM() * ({{max_length}}-{{min_length}}) + {{min_length}})::int
    ) as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{% endmacro %}

{% macro snowflake__synth_column_string_base(name, min_length, max_length) %}
    {% set base_field %}
    randstr(
        uniform({{min_length}}, {{max_length}}, RANDOM({{synth_get_randseed()}}) ),
        uniform(1, 1000000000, RANDOM({{synth_get_randseed()}}) )
    )::varchar({{max_length}}) as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{% endmacro%}