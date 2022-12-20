{% macro synth_column_address(
    name,
    address_types=['house','apartment','pobox'],
    street_types=['St.', 'Rd.', 'Dr.', 'Ln.', 'Ave.', 'Pl.', 'Blvd.', 'Ct.', 'Trl.', 'Pkwy.'],
    cities=[],
    geo_regions=[],
    geo_region_abbrs=[],
    countries=['United States'],
    postal_code_min=10,
    postal_code_max=99999,
    parts=['street_address', 'city', 'geo_region', 'postal_code'],
    distribution="weighted"
) -%}

    {% set address_expression %}
        {% if 'street_address' in parts %}
            {{name}}__street_address
        {% endif %}

        {% if 'city' in parts %}
            {% if parts.index('city') > 0 %} || ' ' || {% endif %}
            {{name}}__city
        {% endif %}

        {% if 'geo_region' in parts %}
            {% if parts.index('geo_region') > 0 %} || ', ' || {% endif %}
            {{name}}__geo_region
        {% elif 'geo_region_abbr' in parts %}
            {% if parts.index('geo_region_abbr') > 0 %} || ', ' || {% endif %}
            {{name}}__geo_region_abbr
        {% endif %}

        {% if 'postal_code' in parts %}
            {% if parts.index('postal_code') > 0 %} || ' ' || {% endif %}
            {{name}}__postal_code
        {% endif %}

        {% if 'country' in parts %}
            {% if parts.index('country') > 0 %} || ' ' ||{% endif %}
            {{name}}__country
        {% endif %}
    {% endset %}

    {% if 'street_address' in parts %}
        {% set street_address_expression %}
            case {{name}}__address_type
                {% for i in range(address_types|length) %}

                when {{i+1}} then
                {% if address_types[i]=='house' %}

                    {{name}}__number1
                    || ' '
                    || {{name}}__street_name
                    || ' '
                    || (case {{name}}__street_type
                        {% for i in range(street_types|length) %}
                        when {{i+1}} THEN '{{street_types[i]}}'
                        {% endfor %}
                    end)
                
                
                {% elif address_types[i]=='apartment' %}
                
                    {{name}}__number1
                    || ' '
                    || {{name}}__street_name
                    || ' '
                    || (case {{name}}__street_type
                        {% for i in range(street_types|length) %}
                        when {{i+1}} THEN '{{street_types[i]}}'
                        {% endfor %}
                    end)
                    || ' ' {# unit #}
                    || (case {{name}}__unit_type
                        when 1 then 'No. '
                        else '#'
                    end)
                    || {{name}}__number2
                
                {% elif address_types[i]=='pobox' %}
                
                    (case {{name}}__unit_type
                        when 1 then 'PO'
                        else 'P.O. '
                    end)
                    || ' Box '
                    || {{name}}__number2
                
                {% endif %}

                {% endfor %}
            end
        {% endset %}

        {{ synth_column_integer(name=name+'__address_type', min=1, max=address_types|length, distribution='uniform') }},
        {{ synth_column_integer(name=name+'__number1', min=10, max=9999, distribution='uniform') }},
        {{ synth_column_words(name=name+'__street_name', language_code='en', distribution="uniform", format_strings=[
            "{NOUN}",
            "{ADJ} {NOUN}"
            ], funcs=["INITCAP"]) }},
        {{ synth_column_integer(name=name+'__street_type', min=1, max=street_types|length, distribution='uniform') }},
        {{ synth_column_integer(name=name+'__unit_type', min=1, max=2, distribution='uniform') }},
        {{ synth_column_integer(name=name+'__number2', min=1, max=999, distribution='uniform') }},
        {{ synth_column_expression(name=name+'__street_address', expression=street_address_expression) }}
        
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'address_type')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'number1')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'street_name')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'street_type')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'unit_type')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'number2')) or "" }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'street_address')) or "" }}
    {% endif %}

    {% if 'city' in parts %}
        {% set filter_pieces = [] %}
        {% if countries|length %}
            {% do filter_pieces.append("country_name in ('"+("','".join(countries))+"')") %}
        {% endif %}
        {% if geo_region_abbrs|length %}
            {% do filter_pieces.append("geo_region_abbr in ('"+("','".join(geo_region_abbrs))+"')") %}
        {% endif %}
        {% if cities|length %}
            {% do filter_pieces.append("name_ascii in ('"+("','".join(cities))+"')") %}
        {% endif %}
        {% if filter_pieces|length %}
            {% set filter = " and ".join(filter_pieces) %}
        {% else %}
            {% set filter = "" %}
        {% endif %}
        {{ synth_column_city(name=name+'__city', distribution=distribution, weight_col="population", filter=filter) }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'city')) or "" }}
    {% endif %}

    {% if 'geo_region' in parts %}
        {% set filter_pieces = [] %}
        {% if countries|length %}
            {% do filter_pieces.append("country_name in ('"+("','".join(countries))+"')") %}
        {% endif %}
        {% if geo_regions|length %}
            {% do filter_pieces.append("name in ('"+("','".join(geo_regions))+"')") %}
        {% endif %}
        {% if geo_region_abbrs|length %}
            {% do filter_pieces.append("abbr in ('"+("','".join(geo_region_abbrs))+"')") %}
        {% endif %}
        {% if filter_pieces|length %}
            {% set filter = " and ".join(filter_pieces) %}
        {% else %}
            {% set filter = "" %}
        {% endif %}
        {{ synth_column_geo_region(name=name+'__geo_region', distribution=distribution, weight_col="population", filter=filter) }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'geo_region')) or "" }}
    {% elif 'geo_region_abbr' in parts %}
        {% set filter_pieces = [] %}
        {% if countries|length %}
            {% do filter_pieces.append("country_name in ('"+("','".join(countries))+"')") %}
        {% endif %}
        {% if geo_regions|length %}
            {% do filter_pieces.append("abbr in ('"+("','".join(geo_regions))+"')") %}
        {% endif %}
        {% if geo_region_abbrs|length %}
            {% do filter_pieces.append("abbr in ('"+("','".join(geo_region_abbrs))+"')") %}
        {% endif %}
        {% if filter_pieces|length %}
            {% set filter = " and ".join(filter_pieces) %}
        {% else %}
            {% set filter = "" %}
        {% endif %}
        {{ synth_column_geo_region(name=name+'__geo_region_abbr', distribution=distribution, weight_col="population", filter=filter) }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'geo_region_abbr')) or "" }}
    {% endif %}

    {% if 'postal_code' in parts %}
        {{ synth_column_integer(name=name+'__postal_code', min=postal_code_min, max=postal_code_max, distribution='uniform') }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'postal_code')) or "" }}
    {% endif %}

    {% if 'country' in parts %}
        {% if countries|length %}
            {% set filter = "name_ascii in ('"+("','".join(countries))+"')" %}
        {% else %}
            {% set filter = "" %}
        {% endif %}
        {{ synth_column_country(name=name+'__country', distribution=distribution, weight_col="population", filter=filter) }}
        {{ synth_add_cleanup_hook(synth_address_cleanup(name, 'country')) or "" }}
    {% endif %}

    {% if parts|length > 0 %}
        , {{ synth_column_expression(name=name, expression=address_expression) }}
    {% endif %}
{%- endmacro %}

{% macro synth_address_cleanup(name, col) %}
alter table {{ this }} drop column {{name}}__{{col}}
{% endmacro %}

