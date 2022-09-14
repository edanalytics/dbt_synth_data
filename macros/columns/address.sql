{% macro column_address(
    name,
    address_types=['house','apartment','pobox'],
    street_types=['St.', 'Rd.', 'Dr.', 'Ln.', 'Ave.', 'Pl.', 'Blvd.', 'Ct.', 'Trl.', 'Pkwy.'],
    cities=[],
    geo_regions=[],
    countries=['United States of America'],
    postal_code_min=10,
    postal_code_max=99999
) -%}
    {{ return(adapter.dispatch('column_address')(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max)) }}
{%- endmacro %}

{% macro default__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max) -%}
    {# NOT YET IMPLEMENTED #}
{%- endmacro %}

{% macro postgresql__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max) %}
    {# NOT YET IMPLEMENTED #}
{% endmacro %}

{% macro snowflake__column_address(name, address_types, street_types, cities, geo_regions, countries, postal_code_min, postal_code_max) %}
    {{ dbt_synth.add_post_hook(address_update_street_noun(name)) or "" }}
    {{ dbt_synth.add_post_hook(address_update_street_adjective(name)) or "" }}
    {{ dbt_synth.add_post_hook(address_update_city(name, cities)) or "" }}
    {{ dbt_synth.add_post_hook(address_update_geo_region(name, geo_regions)) or "" }}
    {{ dbt_synth.add_post_hook(address_update_country(name, countries)) or "" }}
    {{ dbt_synth.add_post_hook(address_update(name, address_types, street_types)) or "" }}
    {% for col in ['address_type', 'number1', 'street_num_words', 'street_noun_idx', 'street_noun', 'street_adjective_idx', 'street_adjective', 'street_type', 'unit_type', 'number2', 'city_idx', 'city', 'geo_region_idx', 'geo_region', 'country_idx', 'country', 'postal_code'] %}
    {{ dbt_synth.add_post_hook(address_cleanup(name, col)) or "" }}
    {% endfor %}

    UNIFORM( 1, {{address_types|length}}, RANDOM({{get_randseed()}}))::int as {{name}}_address_type,
    UNIFORM(10, 9999, RANDOM({{get_randseed()}}))::varchar as {{name}}_number1, {# 2-4 digit number #}
    UNIFORM( 1,    2, RANDOM({{get_randseed()}}))::int as {{name}}_street_num_words,
    {# 1641 number comes from (select count(*) from synth_words where types like '%noun%'): #}
    UNIFORM( 1, 1641, RANDOM({{get_randseed()}}))::int as {{name}}_street_noun_idx,
    ''::varchar as {{name}}_street_noun,
    {# 610 number comes from (select count(*) from synth_words where types like '%adjective%''): #}
    UNIFORM( 1,  610, RANDOM({{get_randseed()}}))::int as {{name}}_street_adjective_idx,
    ''::varchar as {{name}}_street_adjective,
    UNIFORM( 1, {{street_types|length}}, RANDOM({{get_randseed()}}))::int as {{name}}_street_type,
    UNIFORM( 1,    2, RANDOM({{get_randseed()}}))::int as {{name}}_unit_type,
    UNIFORM( 1,  999, RANDOM({{get_randseed()}}))::varchar as {{name}}_number2, {# 1-3 digit number #}
    {# 331 number comes from (select count(*) from synth_cities): #}
    UNIFORM( 1,  {% if cities|length>0 %}{{cities|length}}{% else %}331{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}_city_idx,
    ''::varchar as {{name}}_city,
    {# 71 number comes from (select count(*) from synth_geo_regions): #}
    UNIFORM( 1,   {% if geo_regions|length>0 %}{{geo_regions|length}}{% else %}71{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}_geo_region_idx,
    ''::varchar as {{name}}_geo_region,
    {# 195 number comes from (select count(*) from synth_countries): #}
    UNIFORM( 1,  {% if countries|length>0 %}{{countries|length}}{% else %}195{% endif %}, RANDOM({{get_randseed()}}))::int as {{name}}_country_idx,
    ''::varchar as {{name}}_country,
    LPAD(UNIFORM({{postal_code_min}}, {{postal_code_max}}, RANDOM({{get_randseed()}}) )::varchar, 5, '0') as {{name}}_postal_code,
    ''::varchar AS {{name}}
{% endmacro%}

{% macro address_update_street_noun(name) %}
    update {{this}} x set x.{{name}}_street_noun=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%noun%'
    ) as y where y.row_num=x.{{name}}_street_noun_idx
{% endmacro%}

{% macro address_update_street_adjective(name) %}
    update {{this}} x set x.{{name}}_street_adjective=y.word from (
        select INITCAP(word) as word, row_number() over (order by word asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_words
        where types like '%adjective%'
    ) as y where y.row_num=x.{{name}}_street_adjective_idx
{% endmacro%}

{% macro address_update_city(name, cities) %}
    update {{this}} x set x.{{name}}_city=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_cities
        {% if cities|length > 0 %}where name in ('{{cities|join("','")}}'){% endif %}
    ) as y where y.row_num=x.{{name}}_city_idx
{% endmacro%}

{% macro address_update_geo_region(name, geo_regions) %}
    update {{this}} x set x.{{name}}_geo_region=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_geo_regions
        {% if geo_regions|length > 0 %}where name in ('{{geo_regions|join("','")}}'){% endif %}
    ) as y where y.row_num=x.{{name}}_geo_region_idx
{% endmacro%}

{% macro address_update_country(name, countries) %}
    update {{this}} x set x.{{name}}_country=y.name from (
        select name, row_number() over (order by name asc) as row_num
        from {{this.database}}.{{this.schema}}.synth_countries
        {% if countries|length > 0 %}where name in ('{{countries|join("','")}}'){% endif %}
    ) as y where y.row_num=x.{{name}}_country_idx
{% endmacro%}

{% macro address_update(name, address_types, street_types) %}
    update {{this}} x set x.{{name}}=(
        case x.{{name}}_address_type
            {% for i in range(address_types|length) %}
            when {{i+1}} then {{ build_address(name, address_types[i], street_types) }}
            {% endfor %}
        end
    )
{% endmacro %}

{% macro address_cleanup(name, col) %}
alter table {{ this }} drop column {{name}}_{{col}}
{% endmacro %}


{% macro build_address(name, address_type, street_types) %}

    {% if address_type=='house' %}

        x.{{name}}_number1 || ' '
        || (case x.{{name}}_street_num_words
            when 1 then x.{{name}}_street_noun
            else x.{{name}}_street_adjective || ' ' || x.{{name}}_street_noun
        end)
        || ' '
        || (case x.{{name}}_street_type
            {% for i in range(street_types|length) %}
            when {{i+1}} THEN '{{street_types[i]}}'
            {% endfor %}
        end)
        || ', ' || x.{{name}}_city || ', ' || x.{{name}}_geo_region
        || ' ' || x.{{name}}_postal_code || ' ' || x.{{name}}_country
    
    {% elif address_type=='apartment' %}
    
        x.{{name}}_number1 || ' '
        || (case x.{{name}}_street_num_words
            when 1 then x.{{name}}_street_noun
            else x.{{name}}_street_adjective || ' ' || x.{{name}}_street_noun
        end)
        || ' '
        || (case x.{{name}}_street_type
            {% for i in range(street_types|length) %}
            when {{i+1}} THEN '{{street_types[i]}}'
            {% endfor %}
        end)
        || ' ' {# unit #}
        || (case x.{{name}}_unit_type
            when 1 then 'No. '
            else '#'
        end)
        || x.{{name}}_number2
        || ', ' || x.{{name}}_city || ', ' || x.{{name}}_geo_region
        || ' ' || x.{{name}}_postal_code || ' ' || x.{{name}}_country
    
    {% elif address_type=='pobox' %}
    
       (case x.{{name}}_unit_type
            when 1 then 'PO'
            else 'P.O. '
        end)
        || ' Box '
        || x.{{name}}_number2
        || ', ' || x.{{name}}_city || ', ' || x.{{name}}_geo_region
        || ' ' || x.{{name}}_postal_code || ' ' || x.{{name}}_country
    
    {% endif %}
{% endmacro %}
