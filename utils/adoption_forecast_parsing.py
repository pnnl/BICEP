"""
Module to parse, align, and combined the multi-sector technology adoption forecasts.
"""

from io import BytesIO
import json

import pandas as pd

from azure.storage.blob import BlobServiceClient

from utils.sensitive_config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

container_name = 'biceb'
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


if __name__ == '__main__':
    bau_forecast = scout_forecast(bau_building_blob_name, scenario='bau')
    high_forecast = scout_forecast(high_building_blob_name, scenario='high')

    high_forecast.to_csv('scout_stock_forecast_high.csv', index=False)
    bau_forecast.to_csv('scout_stock_forecast_bau.csv', index=False)
