{% macro synth_distribution_average(d0, d1, d2=None, d3=None, d4=None, d5=None, d6=None, d7=None, d8=None, d9=None, weights=None) -%}
    {# Assume uniform/equal weights if weights are not specified:  #}
    {% if weights is none %}
        {% if d9 %}{% set weights=[1]*10 %}
        {% elif d8 %}{% set weights=[1]*9 %}
        {% elif d7 %}{% set weights=[1]*8 %}
        {% elif d6 %}{% set weights=[1]*7 %}
        {% elif d5 %}{% set weights=[1]*6 %}
        {% elif d4 %}{% set weights=[1]*5 %}
        {% elif d3 %}{% set weights=[1]*4 %}
        {% elif d2 %}{% set weights=[1]*3 %}
        {% else %}{% set weights=[1]*2 %}
        {% endif %}
    {% endif %}
    (
        ( {{weights[0]}} * ({{d0}}) )
        + ( {{weights[1]}} * ({{d1}}) )
        {% if d2 %}+ ( {{weights[2]}} * ({{d2}}) ){% endif %}
        {% if d3 %}+ ( {{weights[3]}} * ({{d3}}) ){% endif %}
        {% if d4 %}+ ( {{weights[4]}} * ({{d4}}) ){% endif %}
        {% if d5 %}+ ( {{weights[5]}} * ({{d5}}) ){% endif %}
        {% if d6 %}+ ( {{weights[6]}} * ({{d6}}) ){% endif %}
        {% if d7 %}+ ( {{weights[7]}} * ({{d7}}) ){% endif %}
        {% if d8 %}+ ( {{weights[8]}} * ({{d8}}) ){% endif %}
        {% if d9 %}+ ( {{weights[9]}} * ({{d9}}) ){% endif %}
    ) / ( {{weights|sum}} )
{%- endmacro %}