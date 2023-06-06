{% macro synth_column_words(name, language="", language_code="", distribution="weighted", n=3, format_strings=[]) -%}

    {% if not language and not language_code %}
        {{ exceptions.raise_compiler_error("Words column `" ~ name ~ "` must specify either `language` or `language_code`.") }}
    {% endif %}

    {% if format_strings|length %}

        {# tokenize each format_string #}
        {% set tokenized_format_strings = [] %}
        {% for format_string in format_strings %}
            {% set tokens, expression = dbt_synth_data.synth_column_words_tokenize_format_string(name, format_string) %}
            {{ tokenized_format_strings.append({
                "format_string": format_string,
                "tokens": tokens,
                "expression": expression
            }) or "" }}
        {% endfor %}

        {# get minimal set of word parts to generate #}
        {% set token_set = {} %}
        {% for obj in tokenized_format_strings %}
            {% for col_name,pos in obj["tokens"].items() %}
                {% if col_name not in token_set.keys() %}
                    {{ token_set.update({col_name: pos}) or ""}}
                {% endif %}
            {% endfor %}
        {% endfor %}
        
        {% set words_expression %}
        (CASE
        {% for i in range(tokenized_format_strings|length) %}
            WHEN ___PREVIOUS_CTE___.{{name}}__format_idx={{i+1}} THEN {{tokenized_format_strings[i]["expression"]}}
        {% endfor %}
        END)
        {% endset %}

        {% for col_name,pos in token_set.items() %}
        {{ dbt_synth_data.synth_column_word(name=col_name, language=language, language_code=language_code, pos=[pos], distribution=distribution) }}
        {{ dbt_synth_data.synth_remove("final_fields", col_name) }}
        {% endfor %}
        
        {% set base_field %}
            {{ dbt_synth_data.synth_distribution_discretize_floor(dbt_synth_data.synth_distribution_continuous_uniform(min=1, max=format_strings|length+1)) }} as {{name}}__format_idx
        {% endset %}
        {{ dbt_synth_data.synth_store('base_fields', name+"__format_idx", base_field) }}

        {% set join_fields %}
            {{ words_expression }} as {{name}}
        {% endset %}
        {{ dbt_synth_data.synth_store("joins", name+"__cte", {"fields": join_fields, "clause": ""} ) }}
        
        {% set final_field %}
            {{name}}
        {% endset %}
        {{ dbt_synth_data.synth_store('final_fields', name, final_field) }}

    {% elif n|int>0 %}

        {% set words_expression %}
        {% for i in range(n) %}{{name}}_word{{i}} {% if not loop.last %} || ' ' || {% endif %}{% endfor %}
        {% endset %}

        {% set query %}
        {% for i in range(n) %}
        {{ dbt_synth_data.synth_column_word(name=name+'_word'+i|string, language=language, language_code=language_code, distribution=distribution) }}
        {{ dbt_synth_data.synth_remove("final_fields", name+'_word'+i|string) }}
        {% endfor %}
        {{ dbt_synth_data.synth_column_expression(name=name, expression=words_expression) }}
        {% endset %}

    {% else %}
        {{ exceptions.raise_compiler_error("Words column `" ~ name ~ "` must specify either `n`>0 or a `format_string`.") }}
    {% endif %}

    {{ return("") }}
{%- endmacro %}


{% macro synth_column_words_tokenize_format_string(name, format_string) %}
    {% set poss = [] %}
    {% set pieces1 = format_string.split('{') %}
    {% set pieces = [] %}
    {% for piece in pieces1 %}
        {% if '}' in piece %}
            {% set subpieces = piece.split('}') %}
            {{ pieces.append('{' + subpieces[0] + '}') or "" }}
            {{ poss.append(subpieces[0]) or "" }}
            {% if subpieces[1]|length>0 %}
                {{ pieces.append(subpieces[1]) or "" }}
            {% endif %}
        {% elif piece|length>0 %}
            {{ pieces.append(piece) or "" }}
        {% endif %}
    {% endfor %}

    {% set type_counts = {} %}
    {% set tokens = {} %}
    {% for pos in poss %}
        {% if pos in type_counts %}
        {% do type_counts.update({pos: type_counts[pos] + 1}) %}
        {{ tokens.update({ name + "__" + pos|replace(" ", "_") + type_counts[pos]|string : pos }) or "" }}
        {% else %}
        {% do type_counts.update({pos: 1}) %}
        {{ tokens.update({ name + "__" + pos|replace(" ", "_") + type_counts[pos]|string : pos }) or "" }}
        {% endif %}
    {% endfor %}

    {% set expression = {"value": "'" + format_string + "'"} %}
    {% for col_name,pos in tokens.items() %}
        {% do expression.update({ "value": expression["value"].replace('{'+pos+'}', "' || ___PREVIOUS_CTE___."+col_name+" || '", 1) }) %}
    {% endfor %}
    {% if expression["value"][:6]=="'' || " %}
    {% do expression.update({ "value": expression["value"][6:] }) %}
    {% endif %}
    {% if expression["value"][-6:]==" || ''" %}
    {% do expression.update({ "value": expression["value"][:-6] }) %}
    {% endif %}
    
    {{ return( (tokens, expression["value"]) )}}
{% endmacro %}