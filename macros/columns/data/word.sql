{% macro synth_column_word(name, language="English", language_code="en", distribution="weighted", pos=[], filter="") -%}
    {% set all_filters %}
        {% if filter and filter|length>1 %}
        ( {{filter}} ) and 
        {% endif %}
        {% if pos and pos|length>1 %}
        (
            {% for p in pos %}
                part_of_speech='{{p}}'
                {%- if not loop.last %}OR {% endif %}
            {% endfor -%}
        ) and 
        {% endif %}
        (
            {%- if language %}
                language='{{language}}'
            {%- elif language_code %}
                language_code='{{language_code}}'
            {%- else %}
                {{ exceptions.raise_compiler_error("Word column `" ~ name ~ "` must specify either `language` or `language_code`.") }}
            {% endif -%}
        )
    {% endset %}
    {{ dbt_synth_data.synth_column_select(
        name=name,
        model_name="synth_words",
        value_cols="word",
        distribution=distribution,
        weight_col="frequency",
        filter=all_filters
    ) }}
    {{ return("") }}
{%- endmacro %}