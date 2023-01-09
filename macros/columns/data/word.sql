{% macro synth_column_word(name, language="English", language_code="en", distribution="weighted", pos=[]) -%}
    {% set filter %}
        (
            {% for p in pos %}
                part_of_speech='{{p}}'
                {%- if not loop.last %}OR {% endif %}
            {% endfor -%}
        ) and (
            {%- if language %}
                language='{{language}}'
            {%- elif language_code %}
                language_code='{{language_code}}'
            {%- else %}
                {{ exceptions.raise_compiler_error("Word column `" ~ name ~ "` must specify either `language` or `language_code`.") }}
            {% endif -%}
        )
    {% endset %}
    {{ synth_column_select(
        name=name,
        model_name="synth_words",
        value_cols="word",
        distribution=distribution,
        weight_col="frequency",
        filter=filter
    ) }}
    {{ return("") }}
{%- endmacro %}