{% macro distribution_union(d0, d1, d2=None, d3=None, d4=None, d5=None, d6=None, d7=None, d8=None, d9=None, weights=None) -%}
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
    case {{ dbt_synth.distribution_discrete_uniform(min=0, max=weights|sum - 1) }}
    {% for d in range(0, weights|length) %}
        {% for w in range(0, weights[d]) %}
        when {{ weights[:d]|sum + w }} then (
            {%- if d==0 -%}{{d0}}{%- endif -%}
            {%- if d==1 -%}{{d1}}{%- endif -%}
            {%- if d==2 -%}{{d2}}{%- endif -%}
            {%- if d==3 -%}{{d3}}{%- endif -%}
            {%- if d==4 -%}{{d4}}{%- endif -%}
            {%- if d==5 -%}{{d5}}{%- endif -%}
            {%- if d==6 -%}{{d6}}{%- endif -%}
            {%- if d==7 -%}{{d7}}{%- endif -%}
            {%- if d==8 -%}{{d8}}{%- endif -%}
            {%- if d==9 -%}{{d9}}{%- endif -%}
        )
        {% endfor %}
    {% endfor %}
    end
{%- endmacro %}