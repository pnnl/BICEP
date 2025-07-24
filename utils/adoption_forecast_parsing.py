"""
Module to parse, align, and combined the multi-sector technology adoption forecasts.
"""

from io import BytesIO
import json

import pandas as pd

# from azure.storage.blob import BlobServiceClient

# from utils.sensitive_config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

# container_name = 'bicep'
# bau_building_blob_name = 'scout-outputs/uec_sdshr_gcam_AEO2023Ref.json'
# high_building_blob_name = 'scout-outputs/uec_sdshr_gcam_alt-High.json'

# BLOB_URL = account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"

# service_client = BlobServiceClient(BLOB_URL, credential=AZURE_STORAGE_KEY)


# def scout_forecast(forecast_blob_name, scenario, metric='stock',):
#     blob_client = service_client.get_blob_client(container=container_name,
#                                                  blob=forecast_blob_name)

#     # Download the blob's content as a stream
#     with BytesIO() as input_blob:
#         blob_client.download_blob().readinto(input_blob)
#         input_blob.seek(0)  # Seek to the start of the stream
#         json_data = json.load(input_blob)

#     building_forecast = pd.DataFrame()
#     for state in json_data.keys():
#         if len(json_data[state]) == 0:
#             continue
#         for sector in ['resid', 'comm']:
#             for fuel in ['electricity', 'gas', 'refined liquids', 'biomass']:
#                 try:
#                     for end_use in ['heating', 'hot water']:
#                         for tech in json_data[state][sector][fuel][end_use].keys():
#                             stock_projections = pd.DataFrame(
#                                 list(json_data[state][sector][fuel][end_use][tech]['stock'].items()),
#                                 columns=['year', 'stock']
#                             )
#                             stock_projections['sector'] = sector
#                             stock_projections['fuel'] = fuel
#                             stock_projections['end_use'] = end_use
#                             stock_projections['technology'] = tech
#                             stock_projections['year'] = stock_projections['year'].astype(int)
#                             stock_projections['state'] = state
#                             stock_projections['metric'] = metric
#                             stock_projections['scenario'] = scenario

#                             building_forecast = pd.concat([building_forecast, stock_projections])
#                 except KeyError:
#                     pass

#     return building_forecast


def phase2_scout_forecast(file):
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
        
        k=1


if __name__ == '__main__':
    phase2_scout_forecast(file=None)  # Replace with actual file path if needed FIRST SCENARIO
    # bau_forecast = scout_forecast(bau_building_blob_name, scenario='bau')
    # high_forecast = scout_forecast(high_building_blob_name, scenario='high')

    # high_forecast.to_csv('scout_stock_forecast_high.csv', index=False)
    # bau_forecast.to_csv('scout_stock_forecast_bau.csv', index=False)
