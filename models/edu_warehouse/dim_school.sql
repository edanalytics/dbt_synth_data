-- depends_on: {{ ref('dim_lea') }}
-- depends_on: {{ ref('synth_words') }}
-- depends_on: {{ ref('synth_cities') }}

{{ config(materialized='table') }}
{{ synth_table(
    rows = var('num_schools'),
    columns = [
        synth_column_primary_key(name='k_school'),
        synth_column_foreign_key(name='k_lea', table='dim_lea', column='k_lea'),
        synth_column_lookup(name='tenant_code', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='tenant_code'),
        synth_column_integer_sequence(name='school_id', step=1, start=1000),
        synth_column_value(name='school_name', value='SchoolNameComingSoon'),
        synth_column_value(name='school_short_name', value='SchoolShortNameComingSoon'),
        synth_column_lookup(name='lea_name', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_name'),
        synth_column_lookup(name='lea_id', value_col='k_lea', lookup_table='dim_lea', from_col='k_lea', to_col='lea_id'),
        synth_column_value(name='school_category', value=None),
        synth_column_value(name='school_type', value='Regular'),
        synth_column_value(name='operational_status', value=None),
        synth_column_value(name='administrative_funding_control', value=None),
        synth_column_value(name='internet_access', value=None),
        synth_column_value(name='title_i_part_a_school_designation', value=None),
        synth_column_value(name='charter_status', value=None),
        synth_column_value(name='charter_approval_agency', value=None),
        synth_column_value(name='magnet_type', value=None),
        synth_column_value(name='website', value=None),
        synth_column_value(name='address_type', value='Physical'),
        synth_column_address(name='street_address', countries=['United States'], geo_region_abbrs=[var('state_code')], address_types=['house'], parts=['street_address']),
        synth_column_address(name='city', countries=['United States'], geo_region_abbrs=[var('state_code')], address_types=['house'], parts=['city'], distribution="uniform"),
        synth_column_value(name='name_of_county', value=None),
        synth_column_address(name='state_code', countries=['United States'], geo_region_abbrs=[var('state_code')], address_types=['house'], parts=['geo_region_abbr'], distribution="uniform"),
        synth_column_address(name='postal_code', countries=['United States'], geo_region_abbrs=[var('state_code')], address_types=['house'], parts=['postal_code']),
        synth_column_value(name='building_site_number', value=None),
        synth_column_value(name='locale', value=None),
        synth_column_value(name='congressional_district', value=None),
        synth_column_value(name='county_fips_code', value=None),
        synth_column_value(name='latitude', value=None),
        synth_column_value(name='longitude', value=None),
    ]
) }}
{{ config(post_hook=synth_get_post_hooks())}}
