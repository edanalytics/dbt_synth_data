{% macro synth_word(name, language="English", language_code="en", distribution="weighted", pos=[]) -%}
    {% set filter %}
        (
            {% for p in pos %}
                part_of_speech='{{p}}'
                {% if not loop.last %}OR {% endif %}
            {% endfor %}
        ) and (
            {% if language %}
                language='{{language}}'
            {% elif language_code %}
                language_code='{{language_code}}'
            {% else %}
                {{ exceptions.raise_compiler_error("Word column `" ~ name ~ "` must specify either `language` or `language_code`.") }}
            {% endif %}
        )
    {% endset %}
    {{ return(
        synth_select(
            name=name,
            value_col="word",
            lookup_table="synth_words",
            distribution=distribution,
            weight_col="frequency",
            filter=filter,
            funcs=["INITCAP"]
        )
    ) }}
{%- endmacro %}