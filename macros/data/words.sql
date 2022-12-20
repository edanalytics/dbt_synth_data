{% macro synth_words(name, language="", language_code="", distribution="weighted", n=3, format_strings=[], funcs=[]) -%}

    {% if not language and not language_code %}
        {{ exceptions.raise_compiler_error("Words column `" ~ name ~ "` must specify either `language` or `language_code`.") }}
    {% endif %}

    {% if format_strings|length %}

        {# tokenize each format_string #}
        {% set tokenized_format_strings = [] %}
        {% for format_string in format_strings %}
            {% set tokens, expression = synth_words_tokenize_format_string(name, format_string) %}
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
        {% for f in funcs %}{{f}}({% endfor %}
        (CASE
        {% for i in range(tokenized_format_strings|length) %}
            WHEN {{name}}_format_idx={{i+1}} THEN {{tokenized_format_strings[i]["expression"]}}
        {% endfor %}
        END)
        {% for f in funcs %}){% endfor %}
        {% endset %}

        {% set query %}
        {{ synth_integer(min=1, max=format_strings|length, distribution='uniform') }} as {{name}}_format_idx,
        {% for col_name,pos in token_set.items() %}
        {{ synth_word(language=language, language_code=language_code, pos=[pos], distribution=distribution) }} as {{col_name}},
        {% endfor %}
        {{ synth_expression(name=name, expression=words_expression, type='varchar') }}
        {% endset %}

        {% set cleanup_cols = [] %}
        {% for col_name in token_set.keys() %}{{ cleanup_cols.append(col_name) or "" }}{% endfor %}
        {{ cleanup_cols.append(name + "_format_idx") or "" }}
        {% for col in cleanup_cols %}
        {{ synth_add_cleanup_hook(synth_words_cleanup(col)) or "" }}
        {% endfor %}

    {% elif n|int>0 %}

        {% set words_expression %}
        {% for i in range(n) %}{{name}}_word{{i}} {% if not loop.last %} || ' ' || {% endif %}{% endfor %}
        {% endset %}

        {% set cleanup_cols = [] %}
        {% for i in range(n) %}{{ cleanup_cols.append(name + "_word" + i|string) or "" }}{% endfor %}

        {% set query %}
        {% for i in range(n) %}
        {{ synth_word(language=language, language_code=language_code, distribution=distribution) }} as {{name}}_word{{i}},
        {% endfor %}
        {{ synth_expression(name=name, expression=words_expression, type='varchar') }}
        {% for col in cleanup_cols %}
        {{ synth_add_cleanup_hook(synth_words_cleanup(col)) or "" }}
        {% endfor %}
        {% endset %}

    {% else %}
        {{ exceptions.raise_compiler_error("Words column `" ~ name ~ "` must specify either `n`>0 or a `format_string`.") }}
    {% endif %}

    {{ return(query) }}
{%- endmacro %}

{% macro synth_words_cleanup(col) %}
alter table {{ this }} drop column {{col}}
{% endmacro %}


{% macro synth_words_tokenize_format_string(name, format_string) %}
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
        {{ tokens.update({ name + "_" + pos|replace(" ", "_") + type_counts[pos]|string : pos }) or "" }}
        {% else %}
        {% do type_counts.update({pos: 1}) %}
        {{ tokens.update({ name + "_" + pos|replace(" ", "_") + type_counts[pos]|string : pos }) or "" }}
        {% endif %}
    {% endfor %}

    {% set expression = {"value": "'" + format_string + "'"} %}
    {% for col_name,pos in tokens.items() %}
        {% do expression.update({ "value": expression["value"].replace('{'+pos+'}', "' || "+col_name+" || '", 1) }) %}
    {% endfor %}
    {% if expression["value"][:6]=="'' || " %}
    {% do expression.update({ "value": expression["value"][6:] }) %}
    {% endif %}
    {% if expression["value"][-6:]==" || ''" %}
    {% do expression.update({ "value": expression["value"][:-6] }) %}
    {% endif %}
    
    {{ return( (tokens, expression["value"]) )}}
{% endmacro %}