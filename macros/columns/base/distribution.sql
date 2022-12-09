{% macro column_distribution(name, distribution) %}
    {{distribution}} as {{name}}
{% endmacro %}