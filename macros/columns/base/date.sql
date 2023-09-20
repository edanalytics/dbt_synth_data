{% macro synth_column_date(name, min='1990-01-01', max='', distribution='uniform') -%}
    {% set base_field %}
        {{ adapter.dispatch('synth_column_date_base')(min, max, distribution) }} AS {{name}}
    {% endset %}
    {{ synth_store('base_fields', name, base_field) }}

    {% set final_field %}
        {{name}}
    {% endset %}
    {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}
{%- endmacro %}

{% macro default__synth_column_date_base(min, max, distribution) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro sqlite__synth_column_date_base(min, max, distribution) %}
    date('{{min}}',
        '+' ||
        ROUND({{dbt_synth_data.synth_sqlite_random()}} * ({% if max|length > 0 %}JULIANDAY('{{max}}'){% else %}JULIANDAY(DATE()){% endif %} - JULIANDAY('{{min}}'))) ||
        ' days')
{% endmacro %}

{% macro duckdb__synth_column_date_base(min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

{% macro postgres__synth_column_date_base(min, max, distribution) %}
    date '{{min}}' + ROUND(RANDOM() * ({% if max|length > 0 %}date '{{max}}'{% else %}CURRENT_DATE{% endif %} - date '{{min}}'))::int
{% endmacro %}

{% macro snowflake__synth_column_date_base(min, max, distribution) %}
    dateadd(
        day,
        UNIFORM(
            0,
            datediff(day, '{{min}}'::date, '{{max}}'::date),
            RANDOM( {{ dbt_synth_data.synth_get_randseed() }} )),
        '{{min}}'::date
    )
{% endmacro%}