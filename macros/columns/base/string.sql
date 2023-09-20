{% macro synth_column_string(name, min_length=1, max_length=32) -%}
    {{ return(adapter.dispatch('synth_column_string_base')(name, min_length, max_length)) }}
{%- endmacro %}

{% macro default__synth_column_string_base(name, min_length, max_length) -%}
    {% set allowed_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' %}
    {% set base_field %}
        (
            substr(
                {% for i in range(0,max_length) %}
                    substr(
                        '{{allowed_chars}}',
                        cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=allowed_chars|length)
                        ) }} as int),
                        1
                    )
                    {% if not loop.last %} || {% endif %}
                {% endfor %}
            , 0, cast( {{ dbt_synth_data.synth_distribution_discretize_floor(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
            ) }} as int) )
        ) as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{# {% macro sqlite__synth_column_string_base(name, min_length, max_length) %}
    {% set allowed_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' %}
    {% set base_field %}
        (
            substr(
                {% for i in range(0,{{max_length}}) %}
                    substr(
                        '{{allowed_chars}}',
                        {{ dbt_synth_data.synth_distribution_discretize_floor(
                            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=allowed_chars|length)
                        ) }},
                        1
                    )
                    {% if not loop.last %} || {% endif %}
                {% endfor %}
            , 0, {{ dbt_synth_data.synth_distribution_discretize_floor(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
            ) }})
        ) as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{% endmacro %}

{% macro duckdb__synth_column_string_base(name, min_length, max_length) %}
    {% set allowed_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz' %}
    {% set base_field %}
        (
            substr(
                {% for i in range(0,{{max_length}}) %}
                    substr(
                        '{{allowed_chars}}',
                        {{ dbt_synth_data.synth_distribution_discretize_floor(
                            distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=0, max=allowed_chars|length)
                        ) }},
                        1
                    )
                    {% if not loop.last %} || {% endif %}
                {% endfor %}
            , 0, {{ dbt_synth_data.synth_distribution_discretize_floor(
                distribution=dbt_synth_data.synth_distribution_continuous_uniform(min=min_length, max=max_length+1)
            ) }})
        ) as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
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
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{% endmacro %} #}

{% macro snowflake__synth_column_string_base(name, min_length, max_length) %}
    {% set base_field %}
    randstr(
        uniform({{min_length}}, {{max_length}}, RANDOM({{ dbt_synth_data.synth_get_randseed() }}) ),
        uniform(1, 1000000000, RANDOM({{ dbt_synth_data.synth_get_randseed() }}) )
    )::varchar({{max_length}}) as {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{% endmacro%}