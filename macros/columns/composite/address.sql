{% macro column_address(
    name,
    address_types=['house','apartment','pobox'],
    street_types=['St.', 'Rd.', 'Dr.', 'Ln.', 'Ave.', 'Pl.', 'Blvd.', 'Ct.', 'Trl.', 'Pkwy.'],
    cities=[],
    geo_regions=[],
    countries=['United States'],
    postal_code_min=10,
    postal_code_max=99999,
    parts=['street_address', 'city', 'geo_region', 'postal_code']
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

        {{ dbt_synth.column_integer(name=name+'__address_type', min=1, max=address_types|length, distribution='uniform') }},
        {{ dbt_synth.column_integer(name=name+'__number1', min=10, max=9999, distribution='uniform') }},
        {{ dbt_synth.column_words(name=name+'__street_name', distribution="uniform", format_strings=[
            "{noun}",
            "{adjective} {noun}"
            ], funcs=["INITCAP"]) }},
        {{ dbt_synth.column_integer(name=name+'__street_type', min=1, max=street_types|length, distribution='uniform') }},
        {{ dbt_synth.column_integer(name=name+'__unit_type', min=1, max=2, distribution='uniform') }},
        {{ dbt_synth.column_integer(name=name+'__number2', min=1, max=999, distribution='uniform') }},
        {{ dbt_synth.column_expression(name=name+'__street_address', expression=street_address_expression) }}
        
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'address_type')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'number1')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'street_name')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'street_type')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'unit_type')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'number2')) or "" }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'street_address')) or "" }}
    {% endif %}

    {% if 'city' in parts %}
        {% if cities|length>0 %}
            {% set city_filter = "country_name in ('"+("','".join(countries))+"')" %}
        {% else %}
            {% set city_filter = "" %}
        {% endif %}
        {{ dbt_synth.column_city(name=name+'__city', distribution="weighted", weight_col="population", filter=city_filter) }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'city')) or "" }}
    {% endif %}

    {% if 'geo_region' in parts %}
        {% if geo_regions|length>0 %}
            {% set geo_region_filter = "country_name in ('"+("','".join(countries))+"')" %}
        {% else %}
            {% set geo_region_filter = "" %}
        {% endif %}
        {{ dbt_synth.column_geo_region(name=name+'__geo_region', distribution="weighted", weight_col="population", filter=geo_region_filter) }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'geo_region')) or "" }}
    {% endif %}

    {% if 'postal_code' in parts %}
        {{ dbt_synth.column_integer(name=name+'__postal_code', min=postal_code_min, max=postal_code_max, distribution='uniform') }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'postal_code')) or "" }}
    {% endif %}

    {% if 'country' in parts %}
        {% if countries|length>0 %}
            {% set country_filter = "name in ('"+("','".join(countries))+"')" %}
        {% else %}
            {% set country_filter = "" %}
        {% endif %}
        {{ dbt_synth.column_country(name=name+'__country', distribution="weighted", weight_col="population", filter=country_filter) }}
        {{ dbt_synth.add_cleanup_hook(address_cleanup(name, 'country')) or "" }}
    {% endif %}

    {% if parts|length > 0 %}
        , {{ dbt_synth.column_expression(name=name, expression=address_expression) }}
    {% endif %}
{%- endmacro %}

{% macro address_cleanup(name, col) %}
alter table {{ this }} drop column {{name}}__{{col}}
{% endmacro %}

