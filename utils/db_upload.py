"""
Add local data files to BICEP database

* x-stock baseline metadata
* tech meta and adoption forecasts
* upgrades and associated costs

"""

import numpy as np
import pandas as pd
from loguru import logger

from utils.db_models import engines

OEDI_BASE = 'https://oedi-data-lake.s3.amazonaws.com'
EUSS_BASE = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock'
RES_STOCK_BASELINE_META = '2022/resstock_amy2018_release_1.1/metadata/baseline.parquet'
COM_STOCK_BASELINE_META = '2023/comstock_amy2018_release_2/metadata/baseline.parquet'
RES_META_FILE = f'{OEDI_BASE}/{EUSS_BASE}/{RES_STOCK_BASELINE_META}'
COM_META_FILE = f'{OEDI_BASE}/{EUSS_BASE}/{COM_STOCK_BASELINE_META}'

res_cols = ['metadata_index',
            'weight',
            'in.heating_fuel',
            'in.hvac_cooling_type',
            'in.hvac_heating_type_and_fuel',
            'in.water_heater_fuel',
            'in.water_heater_efficiency',
            'in.geometry_building_type_recs',
            'in.vintage',
            # 'year_built',
            'in.sqft',
            'in.income',
            'in.census_division',
            'in.census_region',
            'in.iso_rto_region',
            'in.county',
            'in.puma',
            # 'nhgis_tract',
            'in.reeds_balancing_area',
            'in.state',
            'in.ashrae_iecc_climate_zone_2004',
            'in.geometry_building_number_units_mf',
            'in.geometry_building_number_units_sfa',]

com_cols = ['metadata_index',
            'weight',
            'in.heating_fuel',
            'in.hvac_cool_type',
            'in.hvac_heat_type',
            'in.service_water_heating_fuel',
            # 'in.water_heater_efficiency',
            'in.comstock_building_type',
            'in.vintage',
            'in.year_built',
            'in.sqft',
            # 'in.income',
            'in.census_division_name',
            'in.census_region_name',
            'in.iso_rto_region',
            'in.nhgis_county_gisjoin',
            'in.nhgis_puma_gisjoin',
            'in.nhgis_tract_gisjoin',
            'in.reeds_balancing_area',
            'in.state',
            'in.ashrae_iecc_climate_zone_2006',
            'out.params.occupant_density_ppl_per_m_2..people_per_m2']

res_col_mapping = {'metadata_index': 'metadata_index',
                   'bldg_id': 'building_id',
                   'weight': 'weight',
                   'in.heating_fuel': 'heating_fuel',
                   'in.hvac_cooling_type': 'hvac_cool_type',
                   'in.hvac_heating_type_and_fuel': 'hvac_heat_type',
                   'in.water_heater_fuel': 'water_heating_fuel',
                   'in.water_heater_efficiency': 'water_heating_type',
                   'in.geometry_building_type_recs': 'building_type',
                   'in.vintage': 'vintage',
                   'in.sqft': 'sqft',
                   'in.income': 'income',
                   'in.census_division': 'census_division',
                   'in.census_region': 'census_region',
                   'in.iso_rto_region': 'iso_rto_region',
                   'in.county': 'nhgis_county',
                   'in.puma': 'nhgis_puma',
                   'in.reeds_balancing_area': 'reeds_balancing_area',
                   'in.state': 'state',
                   'in.ashrae_iecc_climate_zone_2004': 'ashrae_iecc_climate_zone', }

com_col_mapping = {'metadata_index': 'metadata_index',
                   'bldg_id': 'building_id',
                   'weight': 'weight',
                   'in.heating_fuel': 'heating_fuel',
                   'in.hvac_cool_type': 'hvac_cool_type',
                   'in.hvac_heat_type': 'hvac_heat_type',
                   'in.service_water_heating_fuel': 'water_heating_fuel',
                   'in.comstock_building_type': 'building_type',
                   'in.vintage': 'vintage',
                   'in.year_built': 'year_built',
                   'in.sqft': 'sqft',
                   'in.census_division_name': 'census_division',
                   'in.census_region_name': 'census_region',
                   'in.iso_rto_region': 'iso_rto_region',
                   'in.nhgis_county_gisjoin': 'nhgis_county',
                   'in.nhgis_puma_gisjoin': 'nhgis_puma',
                   'in.nhgis_tract_gisjoin': 'nhgis_tract',
                   'in.reeds_balancing_area': 'reeds_balancing_area',
                   'in.state': 'state',
                   'in.ashrae_iecc_climate_zone_2006': 'ashrae_iecc_climate_zone',
                   'out.params.occupant_density_ppl_per_m_2..people_per_m2': 'occupant_density_m_2'}


def upload_stock_meta(residential=True):
    if residential:
        col_mapping = res_col_mapping
        cols = res_cols
        res = 1
        file = RES_META_FILE
    else:
        col_mapping = com_col_mapping
        cols = com_cols
        res = 0
        file = COM_META_FILE

    logger.info(f'Retrieving baseline meta file for residential={residential}')
    data = pd.read_parquet(file, columns=cols)
    data.reset_index(inplace=True)  # reset index so bldg_id is one of the columns
    data.rename(columns=col_mapping, inplace=True)
    data['residential'] = res

    if not residential:
        data['water_heating_type'] = data['water_heating_fuel']
        data['iso_rto_region'].fillna(value='None', inplace=True)
        data['reeds_balancing_area'].fillna(value=-1, inplace=True)

    else:
        # calculate the total number of units in each building
        mf_col = 'in.geometry_building_number_units_mf'
        sfa_col = 'in.geometry_building_number_units_sfa'
        # single-family have 0 units in these columns
        data[mf_col] = data[mf_col].replace('None', np.nan).astype('Int64').fillna(0)
        data[sfa_col] = data[sfa_col].replace('None', np.nan).astype('Int64').fillna(0)

        # calculate total number of units for single-family attached and multifamily
        data['total_units'] = data[sfa_col] + data[mf_col]
        # replace sf 0's with 1
        data['total_units'].replace(0, 1, inplace=True)
        # remove intermediate cols
        data.drop(columns=[mf_col, sfa_col], inplace=True)

    logger.info('Uploading data to database')
    data.to_sql(name='stock-meta', con=engines['x-stock'],
                if_exists='append', index=False, chunksize=1000)
    logger.info(f'Baseline metadata successfully added for residential={residential}')


def upload_adoption_forecasts():
    """
    One-time upload of complete adoption forecasts to Azure database.
    Only drops specific CA records that already exist (BAU/HIGH 2020/2050), 
    uploads all other data including missing CA years and CA MID scenario.
    """
    logger.info('Loading processed adoption forecasts')
    data = pd.read_parquet('data/parsed_inputs/adoption_forecasts.parquet')
    
    logger.info(f'Original data: {len(data):,} records')
    logger.info(f'States in data: {sorted(data["state"].unique())}')
    
    # Remove only specific CA records that already exist in Azure (avoid duplicates)
    # Azure has CA data for: BAU 2020/2050, HIGH 2020/2050 (but not MID or other years)
    existing_ca_conditions = (
        (data['state'] == 'CA') & 
        (data['scenario'].isin(['bau', 'high'])) & 
        (data['year'].isin([2020, 2050]))
    )
    
    data_filtered = data[~existing_ca_conditions].copy()
    
    removed_count = len(data) - len(data_filtered)
    logger.info(f'Removed {removed_count} existing CA records (BAU/HIGH 2020/2050)')
    logger.info(f'Remaining records: {len(data_filtered):,}')
    
    # Show what CA data we're keeping
    remaining_ca = data_filtered[data_filtered['state'] == 'CA']
    if len(remaining_ca) > 0:
        ca_scenarios = remaining_ca['scenario'].unique()
        ca_years = sorted(remaining_ca['year'].unique())
        logger.info(f'Keeping CA data for scenarios: {list(ca_scenarios)}')
        logger.info(f'Keeping CA data for years: {ca_years[:5]}...{ca_years[-5:]} ({len(ca_years)} total)')
    else:
        logger.info('No CA data remaining after filtering')
    
    # Verify schema matches Azure exactly
    required_columns = ['tech_id', 'tech_name', 'sector', 'year', 'scenario', 'state', 'stock_projection', 'projection_units']
    missing_columns = set(required_columns) - set(data_filtered.columns)
    extra_columns = set(data_filtered.columns) - set(required_columns)
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    if extra_columns:
        logger.warning(f"Dropping extra columns: {extra_columns}")
        data_filtered = data_filtered[required_columns]
    
    logger.info('Schema validation passed - uploading to Azure database')
    logger.info(f'Uploading {len(data_filtered):,} records to adoption-forecasts table')
    
    # Ensure engine is created before use (lazy initialization)
    from utils.db_models import create_engine, validate_database
    validate_database('x-stock')
    if 'x-stock' not in engines:
        engines['x-stock'] = create_engine('x-stock')
    
    # Upload to Azure database (append to existing data)
    data_filtered.to_sql(
        name='adoption-forecasts', 
        con=engines['x-stock'],
        if_exists='append',  # Keep existing CA records, add new ones
        index=False, 
        chunksize=1000
    )
    
    logger.info('Adoption forecasts successfully uploaded to Azure database')


if __name__ == '__main__':
    """
    Example usage of db_upload.py:
    
    # Upload residential stock metadata
    upload_stock_meta(residential=True)
    
    # Upload commercial stock metadata
    upload_stock_meta(residential=False)
    
    # Upload adoption forecasts (one-time operation)
    upload_adoption_forecasts()
    """
    pass
