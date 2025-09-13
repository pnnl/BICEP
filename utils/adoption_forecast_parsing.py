"""
Module to parse, align, and combined the multi-sector technology adoption forecasts.
"""

from io import BytesIO
import json
import os

import pandas as pd
import numpy as np

from azure.storage.blob import BlobServiceClient

from utils.sensitive_config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

container_name = 'bicep'
bau_building_blob_name = 'scout-outputs/uec_sdshr_gcam_AEO2023Ref.json'
high_building_blob_name = 'scout-outputs/uec_sdshr_gcam_alt-High.json'

BLOB_URL = account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"

service_client = BlobServiceClient(BLOB_URL, credential=AZURE_STORAGE_KEY)


def scout_forecast(forecast_blob_name, scenario, metric='stock',):
    blob_client = service_client.get_blob_client(container=container_name,
                                                 blob=forecast_blob_name)

    # Download the blob's content as a stream
    with BytesIO() as input_blob:
        blob_client.download_blob().readinto(input_blob)
        input_blob.seek(0)  # Seek to the start of the stream
        json_data = json.load(input_blob)

    building_forecast = pd.DataFrame()
    for state in json_data.keys():
        if len(json_data[state]) == 0:
            continue
        for sector in ['resid', 'comm']:
            for fuel in ['electricity', 'gas', 'refined liquids', 'biomass']:
                try:
                    for end_use in ['heating', 'hot water']:
                        for tech in json_data[state][sector][fuel][end_use].keys():
                            stock_projections = pd.DataFrame(
                                list(json_data[state][sector][fuel][end_use][tech]['stock'].items()),
                                columns=['year', 'stock']
                            )
                            stock_projections['sector'] = sector
                            stock_projections['fuel'] = fuel
                            stock_projections['end_use'] = end_use
                            stock_projections['technology'] = tech
                            stock_projections['year'] = stock_projections['year'].astype(int)
                            stock_projections['state'] = state
                            stock_projections['metric'] = metric
                            stock_projections['scenario'] = scenario

                            building_forecast = pd.concat([building_forecast, stock_projections])
                except KeyError:
                    pass

    return building_forecast


def eda(file):
    """

    I think it would help to list the specific variables you need, I'm not sure I'm tracking.
    The raw data I'm sharing here have stock, energy, and energy cost outputs for each measure run in each scenario –
    aggregated totals and broken out by state, building type (res/com and new/exist), and end use.

    I believe the output we don't have in the raw data that you need is stock costs (or capital technology investment costs)
    broken out by those dimensions. We only have the aggregated investment costs by measure here.
    But I think you can get a rough version of that by doing the following in these data, for each measure
    in the dictionary file and a given projection year:


    #1 Pull the total stock
        ("Markets and Savings (Overall)" ->
        "Max adoption potential" ->
        "Measure Stock [units vary]" ->
        [insert projection year])

    #2 Pull the total stock costs
        ("Markets and Savings (Overall)" ->
        "Max adoption potential" ->
        "Total Measure Stock Cost (2024$)" ->
        [insert projection year])

    #3 Pull the total stock for a given region/building/end use/fuel breakout
        ("Markets and Savings (by Category)" ->
        "Max adoption potential" ->
        "Measure Stock [units vary]" ->
        [insert state] ->
        [insert building type/vintage] ->
        [insert end use] ->
        [insert fuel type] ->
        [insert projection year])

    Divide #3 by #1, and multiply the resulting fraction by #2 to get the measure stock cost allocation for that breakout.
    It's not perfect since the allocation of stock costs is not necessarily 1:1 with the allocation of stock across those
    dimensions (e.g., costs in existing building types will be disproportionately higher), but it gives you a starting point
    for the near-term. In the longer-term (by end of FY), we will seek to add the stock cost breakouts
    directly to support your work.

    Hope this helps!

    Jared


    Additional guidance about files:
    Each file represents one of the five scenarios and includes a list of dictionaries, where each dictionary corresponds
    to a single measure in the analysis.

    Detailed breakouts of the type I believe you are interested in are available under the key "Markets and Savings
     (by Category)" and are nested as follows:

    "Markets and Savings (by Category)" -> "Max adoption potential" ->
    [insert output variable – e.g., "Baseline Energy Cost (USD)" or
    "Efficient Energy Cost (USD)" or
    "Energy Cost Savings (USD)" for energy costs] ->
    [insert state] ->
    [insert building type] ->
    [insert end use] ->
    [insert fuel type] -[insert projection year]. See below regarding the available keys for those breakout categories.

    Note that when fuel type is not applicable to a given measure, no key will be reported for it in the nested dictionary.
    states: [
                        'AL', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
                        'GA', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
                        'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
                        'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
                        'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI',
                        'WY']

    building types/vintages: ['Residential (New)', 'Residential (Existing)', 'Commercial (New)', 'Commercial (Existing)']

    end uses: ['Heating (Equip.)', 'Cooling (Equip.)',  'Ventilation', 'Lighting', 'Water Heating', 'Refrigeration',
    'Cooking', 'Computers and Electronics', 'Other']

    fuel types: ['Electric', 'Natural Gas', 'Propane', 'Distillate/Other', 'Biomass']

    """

    output_file = 'high.json'

    full_scenario = json.load(open(output_file))
    measure = '(C) Ref. Case RTU, NG Heat'

    measure_data = full_scenario[measure]
    region = 'ME'
    building_class = 'Commercial (Existing)'
    end_use = 'Heating (Equip.)'
    fuel = 'Natural Gas'

    market_saving = measure_data['Markets and Savings (by Category)']
    max_adopt_potential = market_saving['Max adoption potential']
    baseline_stock = max_adopt_potential['Baseline Stock (TBtu heating served)']

    stock_by_region = baseline_stock[region]
    stock_by_region_class = stock_by_region[building_class]
    stock_by_region_class_enduse = stock_by_region_class[end_use]
    stock_by_region_class_enduse_fuel = stock_by_region_class_enduse[fuel]

    first_file = '/Users/faye994/code/BICEP/stated.json'
    first_scenario = json.load(open(first_file))

    for year in range(2024, 2050):
        cost = \
            full_scenario['(R) ESTAR HP TS (Resist. Heat, No Cool)']['Markets and Savings (Overall)'][
                'Max adoption potential'][
                'Total Measure Stock Cost (2024$)'][f'{year}']
        stock = \
            full_scenario['(R) ESTAR HP TS (Resist. Heat, No Cool)']['Markets and Savings (Overall)'][
                'Max adoption potential'][
                'Measure Stock (units equipment)'][f'{year}']
        existing_stock_per_region = \
            full_scenario['(R) ESTAR HP TS (Resist. Heat, No Cool)']['Markets and Savings (by Category)'][
                'Max adoption potential']['Measure Stock (units equipment)']['AL']['Residential (Existing)'][
                'Heating (Equip.)']['Electric'][f'{year}']
        new_stock_per_region = \
            full_scenario['(R) ESTAR HP TS (Resist. Heat, No Cool)']['Markets and Savings (by Category)'][
                'Max adoption potential']['Measure Stock (units equipment)']['AL']['Residential (New)'][
                'Heating (Equip.)'][
                'Electric'][f'{year}']
        print(
            f'AL {year} stock_per_region / total stock * total cost = ${(new_stock_per_region + existing_stock_per_region) / stock * cost:,.2f}')
        print(
            f'AL {year} total cost / total stock = ${cost / stock:,.2f}')
        


# =============================================================================
# LOCAL DATA PROCESSING FUNCTIONS (NEW)
# Process Scout, EV, and PV data locally from input files
# =============================================================================

def scout_forecast_local(scenario='bau'):
    """
    Process Scout data locally from input files.
    
    Args:
        scenario (str): 'bau' or 'high' scenario
        
    Returns:
        pd.DataFrame: Formatted data ready for _get_tech_projections()
    """
    # Define file paths
    base_path = '/Users/faye994/code/BICEP/input_folder'
    if scenario == 'bau':
        file_path = os.path.join(base_path, 'uec_sdshr_gcam_AEO2023Ref.json')
    elif scenario == 'high':
        file_path = os.path.join(base_path, 'uec_sdshr_gcam_alt-High.json')
    else:
        raise ValueError("Scenario must be 'bau' or 'high'")
    
    # Load JSON data
    with open(file_path, 'r') as f:
        json_data = json.load(f)
    
    # Tech ID mappings for Scout data  
    # Maps JSON tech names to (tech_id, database_tech_name) tuples
    tech_mapping = {
        'heat pump': (1, 'heat pump'),
        'electric heat pump': (1, 'heat pump'),  # Alternative JSON name
        'electric furnace': (2, 'electric furnace'),
        'fuel furnace': (3, 'fuel furnace'),
        'gas furnace': (4, 'gas furnace'),
        'wood furnace': (5, 'wood furnace'),
        'heat pump water heater': (6, 'hpwh'),
        'electric heat pump water heater': (6, 'hpwh'),  # Full JSON name
        'electric resistance water heater': (7, 'electric wh'),  # Standard electric water heater
        'electric water heater': (7, 'electric wh'),
        'fuel water heater': (8, 'fuel wh'),
        'gas water heater': (9, 'gas wh')
    }
    
    building_forecast = pd.DataFrame()
    
    for state in json_data.keys():
        if len(json_data[state]) == 0:
            continue
        for sector in ['resid', 'comm']:
            for fuel in ['electricity', 'gas', 'refined liquids', 'biomass']:
                try:
                    for end_use in ['heating', 'hot water']:
                        for tech in json_data[state][sector][fuel][end_use].keys():
                            # Get tech_id and database tech name from mapping
                            tech_lower = tech.lower()
                            tech_id = None
                            db_tech_name = None
                            
                            # Try exact match first
                            if tech_lower in tech_mapping:
                                tech_id, db_tech_name = tech_mapping[tech_lower]
                            else:
                                # Try partial match
                                for mapped_name, (t_id, db_name) in tech_mapping.items():
                                    if mapped_name in tech_lower:
                                        tech_id, db_tech_name = t_id, db_name
                                        break
                            
                            if tech_id is None:
                                continue  # Skip unmapped technologies
                            
                            stock_projections = pd.DataFrame(
                                list(json_data[state][sector][fuel][end_use][tech]['stock'].items()),
                                columns=['year', 'stock_projection']
                            )
                            stock_projections['id'] = range(len(stock_projections))
                            stock_projections['tech_id'] = tech_id
                            stock_projections['tech_name'] = db_tech_name  # Use database tech name instead of JSON name
                            stock_projections['sector'] = sector
                            stock_projections['fuel'] = fuel
                            stock_projections['end_use'] = end_use
                            stock_projections['year'] = stock_projections['year'].astype(int)
                            stock_projections['state'] = state
                            stock_projections['scenario'] = scenario
                            stock_projections['projection_units'] = 'units'

                            building_forecast = pd.concat([building_forecast, stock_projections], ignore_index=True)
                except KeyError:
                    pass

    return building_forecast


def ev_forecast_local(scenario='bau'):
    """
    Process EV data locally from CSV files.
    
    Args:
        scenario (str): Scenario to process ('bau' or 'high')
        
    Returns:
        pd.DataFrame: Processed EV data in standard format
    """
    # Load EV data
    ev_file = os.path.join('input_folder', 'decarb_LDV_EV_county_stock_0923.csv')
    ev_data = pd.read_csv(ev_file)
    
    # Load hierarchy for county-to-state mapping
    hierarchy_file = os.path.join('input_folder', 'hierarchy_utf8.csv')
    hierarchy_data = pd.read_csv(hierarchy_file)
    
    print(f"EV data shape: {ev_data.shape}")
    print(f"Hierarchy data shape: {hierarchy_data.shape}")
    
    # EV data is already in long format with Year, Scenario, county_fips, Class, Tech, Vehicles
    # Map scenario names - the data uses lowercase scenario names
    scenario_mapping = {
        'bau': 'bau',        # Direct mapping - no change needed
        'high': 'high',      # Direct mapping - no change needed  
        'mid': 'mid',        # Direct mapping - no change needed
        'Reference': 'bau',  # Legacy mapping for backward compatibility
        'High': 'high'       # Legacy mapping for backward compatibility
    }
    
    # Filter for the requested scenario
    if scenario in scenario_mapping:
        scenario_filter = scenario_mapping[scenario]
    else:
        scenario_filter = scenario
    
    filtered_data = ev_data[ev_data['Scenario'] == scenario_filter].copy()
    
    if filtered_data.empty:
        print(f"Warning: No data found for scenario '{scenario_filter}'. Available scenarios: {ev_data['Scenario'].unique()}")
        # Use first available scenario as fallback
        scenario_filter = ev_data['Scenario'].iloc[0]
        filtered_data = ev_data[ev_data['Scenario'] == scenario_filter].copy()
        print(f"Using fallback scenario: {scenario_filter}")
    
    # Filter for BEV (Battery Electric Vehicle) technology only
    bev_data = filtered_data[filtered_data['Tech'].str.contains('BEV', case=False, na=False)].copy()
    
    if bev_data.empty:
        print(f"Warning: No BEV data found. Available technologies: {filtered_data['Tech'].unique()}")
        return pd.DataFrame()  # Return empty DataFrame if no BEV data
    
    print(f"Filtered to BEV only: {len(bev_data)} records from {len(filtered_data)} total")
    print(f"Available vehicle technologies: {sorted(filtered_data['Tech'].unique())}")
    
    # Add 'p' prefix to county_fips to match hierarchy format (ensure 5-digit FIPS with leading zeros)
    bev_data['county_fips_p'] = 'p' + bev_data['county_fips'].astype(str).str.zfill(5)
    
    # Merge with hierarchy to get state information
    merged_data = pd.merge(
        bev_data, 
        hierarchy_data[['*county', 'st']], 
        left_on='county_fips_p', 
        right_on='*county', 
        how='inner'
    )
    
    if merged_data.empty:
        raise ValueError("No matching counties found between EV data and hierarchy after adding 'p' prefix")
    
    print(f"Merged EV data shape: {merged_data.shape}")
    
    # Aggregate to state level (sum vehicles by state, year, tech, class)
    state_aggregated = merged_data.groupby(['st', 'Year', 'Tech', 'Class'])['Vehicles'].sum().reset_index()
    
    # Create the standard format
    result_data = []
    for _, row in state_aggregated.iterrows():
        result_data.append({
            'tech_id': 10,  # EV tech_id
            'tech_name': 'ev',
            'sector': 'transportation',
            'year': int(row['Year']),
            'scenario': scenario,
            'state': row['st'],
            'stock_projection': row['Vehicles'],
            'projection_units': 'vehicles'
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Assign unique IDs
    result_df['id'] = range(len(result_df))
    
    print(f"Final EV data shape: {result_df.shape}")
    print(f"Year range: {result_df['year'].min()} - {result_df['year'].max()}")
    print(f"States: {result_df['state'].nunique()}")
    
    return result_df


def pv_forecast_local(scenario='mid'):
    """
    Process PV data locally from CSV file and aggregate to state level.
    
    Args:
        scenario (str): Scenario name (typically 'mid' for PV data)
        
    Returns:
        pd.DataFrame: Formatted PV data ready for _get_tech_projections()
    """
    base_path = '/Users/faye994/code/BICEP/input_folder'
    
    # Load PV data
    pv_file = os.path.join(base_path, 'distpvcap_stscen2023_mid_case_utf8.csv')
    pv_data = pd.read_csv(pv_file)
    
    # Load hierarchy mapping
    hierarchy_file = os.path.join(base_path, 'hierarchy_utf8.csv')
    hierarchy_data = pd.read_csv(hierarchy_file)
    
    # Handle column name mismatch - PV data uses 'r' column with 'p' prefix
    # Hierarchy uses '*county' column with 'p' prefix
    # Create matching column names
    pv_data = pv_data.rename(columns={'r': '*county'})
    
    # Merge with hierarchy to get state mapping
    merged_data = pd.merge(pv_data, hierarchy_data[['*county', 'st']], 
                          on='*county', how='inner')
    
    # Get year columns
    year_cols = [col for col in merged_data.columns 
                if str(col).isdigit() and 2010 <= int(col) <= 2050]
    
    # Convert to long format
    long_data = merged_data.melt(
        id_vars=['*county', 'st'],
        value_vars=year_cols,
        var_name='year',
        value_name='stock_projection'
    )
    
    # Aggregate by state and year - sum all county data within each state
    aggregated_data = long_data.groupby(['st', 'year'])['stock_projection'].sum().reset_index()
    
    # Format for _get_tech_projections
    result = pd.DataFrame({
        'id': range(len(aggregated_data)),
        'tech_id': 11,  # PV tech_id
        'tech_name': 'pv',
        'sector': 'pv',
        'year': aggregated_data['year'].astype(int),
        'scenario': scenario,
        'state': aggregated_data['st'],
        'stock_projection': aggregated_data['stock_projection'],
        'projection_units': 'MW'
    })
    
    return result


# Note: Data combining is handled by _get_tech_projections() in tech_adoption.py
# These functions only process individual technology types


if __name__ == '__main__':
    # Test local processing functions
    print("Testing local data processing...")
    
    # Example usage - process individual technologies
    print("Processing Scout data locally...")
    scout_data = scout_forecast_local(scenario='bau')
    print(f"Scout data shape: {scout_data.shape}")
    
    print("\nProcessing EV data locally...")
    ev_data = ev_forecast_local(scenario='bau')
    print(f"EV data shape: {ev_data.shape}")
    
    print("\nProcessing PV data locally...")
    pv_data = pv_forecast_local(scenario='mid')
    print(f"PV data shape: {pv_data.shape}")
    
    print("\nLocal processing complete!")
    print("Note: Data combining is handled by _get_tech_projections() in tech_adoption.py")
    
    # Legacy code (commented out for local processing)
    # eda(file=None)
    # bau_forecast = scout_forecast(bau_building_blob_name, scenario='bau')
    # high_forecast = scout_forecast(high_building_blob_name, scenario='high')
    # high_forecast.to_csv('scout_stock_forecast_high.csv', index=False)
    # bau_forecast.to_csv('scout_stock_forecast_bau.csv', index=False)
