{% macro synth_column_distribution(name, distribution) %}
    {{distribution}} as {{name}}
{% endmacro %}