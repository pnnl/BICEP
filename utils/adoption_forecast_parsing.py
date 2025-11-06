"""
Module to parse, align, and combined the multi-sector technology adoption forecasts.
"""

from io import BytesIO
import json
from pathlib import Path
import logging

import pandas as pd

from azure.storage.blob import BlobServiceClient

from utils.sensitive_config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
from utils.config import SCOUT_BAU_FILE, SCOUT_HIGH_FILE, EV_PROJECTIONS_FILE, HIERARCHY_FILE, PV_PROJECTIONS_FILE

logger = logging.getLogger(__name__)

# Legacy Azure blob configuration
container_name = 'bicep'
bau_building_blob_name = 'scout-outputs/uec_sdshr_gcam_AEO2023Ref.json'
high_building_blob_name = 'scout-outputs/uec_sdshr_gcam_alt-High.json'

BLOB_URL = account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
service_client = BlobServiceClient(BLOB_URL, credential=AZURE_STORAGE_KEY)


# ============= BICEP v1 STANDARDIZATION =============

STATE_ABBREVIATIONS = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'District of Columbia': 'DC'
}

SECTOR_STANDARDIZATION = {
    'resid': 'residential', 'comm': 'commercial', 'res': 'residential', 'com': 'commercial',
    'residential': 'residential', 'commercial': 'commercial', 'transportation': 'transportation', 'pv': 'pv'
}

def standardize_state_name(state_name):
    if pd.isna(state_name) or state_name is None:
        return None
    state_name = str(state_name).strip()
    if len(state_name) == 2 and state_name.upper() in STATE_ABBREVIATIONS.values():
        return state_name.upper()
    if state_name in STATE_ABBREVIATIONS:
        return STATE_ABBREVIATIONS[state_name]
    for full_name, abbrev in STATE_ABBREVIATIONS.items():
        if state_name.lower() == full_name.lower():
            return abbrev
    logger.warning(f"Unknown state name: {state_name}")
    return state_name

def standardize_sector_name(sector_name):
    if pd.isna(sector_name) or sector_name is None:
        return None
    sector_name = str(sector_name).strip().lower()
    if sector_name in SECTOR_STANDARDIZATION:
        return SECTOR_STANDARDIZATION[sector_name]
    logger.warning(f"Unknown sector name: {sector_name}")
    return sector_name

def standardize_scenario_name(scenario_name):
    if pd.isna(scenario_name) or scenario_name is None:
        return None
    return str(scenario_name).strip().lower()

def standardize_dataframe_for_bicep_v1(df):
    df_standardized = df.copy()
    if 'state' in df_standardized.columns:
        df_standardized['state'] = df_standardized['state'].apply(standardize_state_name)
    if 'sector' in df_standardized.columns:
        df_standardized['sector'] = df_standardized['sector'].apply(standardize_sector_name)
    if 'scenario' in df_standardized.columns:
        df_standardized['scenario'] = df_standardized['scenario'].apply(standardize_scenario_name)
    
    azure_columns = ['tech_id', 'tech_name', 'sector', 'year', 'scenario', 'state', 'stock_projection', 'projection_units']
    available_columns = [col for col in azure_columns if col in df_standardized.columns]
    df_standardized = df_standardized[available_columns]
    
    if 'stock_projection' in df_standardized.columns:
        df_standardized.loc[df_standardized['stock_projection'] < 0, 'stock_projection'] = 0
    
    return df_standardized


# ============= LEGACY AZURE BLOB FUNCTION =============

def scout_forecast(forecast_blob_name, scenario, metric='stock'):
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


# ============= CURRENT ACTIVE FUNCTIONS =============

def scout_forecast_local(scenario):
    """Process Scout data locally from input files."""
    if scenario == 'bau':
        file_path = SCOUT_BAU_FILE
    elif scenario == 'high':
        file_path = SCOUT_HIGH_FILE
    else:
        raise ValueError("Scenario must be 'bau' or 'high'")
    
    with open(file_path, 'r') as f:
        json_data = json.load(f)
    
    tech_mapping = {
        'heat pump': (1, 'heat pump'),
        'electric heat pump': (1, 'heat pump'),
        'electric furnace': (2, 'electric furnace'),
        'fuel furnace': (3, 'fuel furnace'),
        'gas furnace': (4, 'gas furnace'),
        'wood furnace': (5, 'wood furnace'),
        'heat pump water heater': (6, 'hpwh'),
        'electric heat pump water heater': (6, 'hpwh'),
        'electric resistance water heater': (7, 'electric wh'),
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
                            tech_lower = tech.lower()
                            tech_id = None
                            db_tech_name = None
                            
                            if tech_lower in tech_mapping:
                                tech_id, db_tech_name = tech_mapping[tech_lower]
                            else:
                                for mapped_name, (t_id, db_name) in tech_mapping.items():
                                    if mapped_name in tech_lower:
                                        tech_id, db_tech_name = t_id, db_name
                                        break
                            
                            if tech_id is None:
                                continue
                            
                            stock_projections = pd.DataFrame(
                                list(json_data[state][sector][fuel][end_use][tech]['stock'].items()),
                                columns=['year', 'stock_projection']
                            )
                            stock_projections['id'] = range(len(stock_projections))
                            stock_projections['tech_id'] = tech_id
                            stock_projections['tech_name'] = db_tech_name
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

    return standardize_dataframe_for_bicep_v1(building_forecast)


def ev_forecast_local(scenario):
    """Process EV data locally from CSV files."""
    ev_data = pd.read_csv(EV_PROJECTIONS_FILE)
    hierarchy_data = pd.read_csv(HIERARCHY_FILE)
    
    scenario_mapping = {
        'bau': 'bau', 'high': 'high', 'mid': 'mid',
        'Reference': 'bau', 'High': 'high'
    }
    
    scenario_filter = scenario_mapping.get(scenario, scenario)
    filtered_data = ev_data[ev_data['Scenario'] == scenario_filter].copy()
    
    if filtered_data.empty:
        logger.warning(f"No data found for scenario '{scenario_filter}'. Available scenarios: {ev_data['Scenario'].unique()}")
        scenario_filter = ev_data['Scenario'].iloc[0]
        filtered_data = ev_data[ev_data['Scenario'] == scenario_filter].copy()
        logger.info(f"Using fallback scenario: {scenario_filter}")
    
    bev_data = filtered_data[filtered_data['Tech'].str.contains('BEV', case=False, na=False)].copy()
    
    if bev_data.empty:
        logger.warning(f"No BEV data found. Available technologies: {filtered_data['Tech'].unique()}")
        return pd.DataFrame()
    
    bev_data['county_fips_p'] = 'p' + bev_data['county_fips'].astype(str).str.zfill(5)
    
    merged_data = pd.merge(
        bev_data, 
        hierarchy_data[['*county', 'st']], 
        left_on='county_fips_p', 
        right_on='*county', 
        how='inner'
    )
    
    if merged_data.empty:
        raise ValueError("No matching counties found between EV data and hierarchy after adding 'p' prefix")
    
    state_aggregated = merged_data.groupby(['st', 'Year'])['Vehicles'].sum().reset_index()
    
    result_data = []
    for _, row in state_aggregated.iterrows():
        result_data.append({
            'tech_id': 10,
            'tech_name': 'ev',
            'sector': 'transportation',
            'year': int(row['Year']),
            'scenario': scenario,
            'state': row['st'],
            'stock_projection': row['Vehicles'],
            'projection_units': 'vehicles'
        })
    
    result_df = pd.DataFrame(result_data)
    result_df['id'] = range(len(result_df))
    
    return standardize_dataframe_for_bicep_v1(result_df)


def pv_forecast_local(scenario):
    """Process PV data locally from ReEDS CSV file and aggregate to state level."""
    pv_data = pd.read_csv(PV_PROJECTIONS_FILE)
    
    pv_data = pv_data[pv_data['scen'] == scenario].copy()
    
    if pv_data.empty:
        logger.warning(f"No PV data found for scenario '{scenario}'. Available scenarios: {pv_data['scen'].unique()}")
        return pd.DataFrame()
    
    hierarchy_data = pd.read_csv(HIERARCHY_FILE)
    reeds_to_state = hierarchy_data[['ba', 'st']].drop_duplicates()
    reeds_to_state = reeds_to_state.rename(columns={'ba': 'r'})
    
    merged_data = pd.merge(pv_data, reeds_to_state, on='r', how='inner')
    aggregated_data = merged_data.groupby(['st', 't'])['Value'].sum().reset_index()
    
    result = pd.DataFrame({
        'id': range(len(aggregated_data)),
        'tech_id': 11,
        'tech_name': 'pv',
        'sector': 'pv',
        'year': aggregated_data['t'].astype(int),
        'scenario': scenario,
        'state': aggregated_data['st'],
        'stock_projection': aggregated_data['Value'],
        'projection_units': 'MW'
    })
    
    return standardize_dataframe_for_bicep_v1(result)


def generate_adoption_forecasts(scenarios=None, output_file=None):
    """Generate complete adoption forecasts dataset by processing all technologies and scenarios."""
    if scenarios is None:
        scenarios = ['bau', 'high']
    
    if output_file is None:
        output_file = Path('data/parsed_inputs/adoption_forecasts.parquet')
    else:
        output_file = Path(output_file)
    
    print(f"Generating adoption forecasts for scenarios: {scenarios}")
    
    all_data = []
    
    for scenario in scenarios:
        print(f"\nProcessing scenario: {scenario.upper()}")
        
        print(f"  Processing Scout data for {scenario}...")
        scout_data = scout_forecast_local(scenario=scenario)
        print(f"    Scout {scenario} data shape: {scout_data.shape}")
        all_data.append(scout_data)
        
        print(f"  Processing EV data for {scenario}...")
        ev_data = ev_forecast_local(scenario=scenario)
        print(f"    EV {scenario} data shape: {ev_data.shape}")
        all_data.append(ev_data)
        
        print(f"  Processing PV data for {scenario}...")
        pv_data = pv_forecast_local(scenario=scenario)
        print(f"    PV {scenario} data shape: {pv_data.shape}")
        all_data.append(pv_data)
    
    print("\nCombining all adoption forecast data...")
    combined_data = pd.concat(all_data, ignore_index=True)
    combined_data['id'] = range(len(combined_data))
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined_data.to_parquet(output_file, index=False)
    
    print(f"\nSaved combined adoption forecasts: {output_file}")
    print(f"Total records: {len(combined_data):,}")
    print(f"States: {sorted(combined_data['state'].unique())}")
    print(f"Scenarios: {sorted(combined_data['scenario'].unique())}")
    print(f"Technologies: {sorted(combined_data['tech_name'].unique())}")
    
    return combined_data


if __name__ == '__main__':
    # Generate complete adoption forecasts dataset
    print("GENERATING COMPLETE ADOPTION FORECASTS DATASET")
    print("="*60)

    combined_data = generate_adoption_forecasts()
    
    print("\nProcessing complete!")
