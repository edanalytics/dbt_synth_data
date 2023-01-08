{% macro synth_column_string(name, min_length=1, max_length=32) -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_string_base')(min_length, max_length) }} as {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_string_base(min_length, max_length) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_string_base(min_length, max_length) %}
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
    )
{% endmacro %}

{% macro snowflake__synth_column_string_base(min_length, max_length) %}
    randstr(
        uniform({{min_length}}, {{max_length}}, RANDOM({{synth_get_randseed()}}) ),
        uniform(1, 1000000000, RANDOM({{synth_get_randseed()}}) )
    )::varchar({{max_length}})
{% endmacro%}