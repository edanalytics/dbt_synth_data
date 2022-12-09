{% macro synth_column_string(name, min_length=1, max_length=32) -%}
    {{ return(adapter.dispatch('synth_column_string')(name, min_length, max_length)) }}
{%- endmacro %}

{% macro default__synth_column_string(name, min_length, max_length) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgres__synth_column_string(name, min_length, max_length) %}
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
{% endmacro %}

{% macro snowflake__synth_column_string(name, min_length, max_length) %}
    randstr(
        uniform({{min_length}}, {{max_length}}, RANDOM({{synth_get_randseed()}}) ),
        uniform(1, 1000000000, RANDOM({{synth_get_randseed()}}) )
    )::varchar({{max_length}}) as {{name}}
{% endmacro%}