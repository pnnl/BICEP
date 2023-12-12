import pandas as pd

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from time import perf_counter
from loguru import logger

s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

oedi_s3_bucket = 'oedi-data-lake'
xstock_folder = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2023/comstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade=0/state=CA/'

t0 = perf_counter()
logger.info('getting first 1000 files')
# first 1000 files, aws limit
response = s3_client.list_objects_v2(
    Bucket=oedi_s3_bucket,
    Prefix=xstock_folder)

t1 = perf_counter()
logger.info(f'time elapsed: {t1-t0}')

# import awswrangler as wr
# df = wr.s3.read_parquet(response['Contents'][345]['Key'])

logger.info('getting presigned url')
# Generate a pre-signed URL for the Parquet file
presigned_url = s3_client.generate_presigned_url(
    ClientMethod="get_object",
    Params={"Bucket": oedi_s3_bucket, "Key": response['Contents'][345]['Key']},
    ExpiresIn=3600,  # Set expiry time in seconds (1 hour)
)
t2 = perf_counter()
logger.info(f'time elapsed: {t2-t1}')


logger.info('reading first parquet file')
df = pd.read_parquet(presigned_url)
# for content in response.get('Contents', []):
#     print(content['Key'])
t3 = perf_counter()
logger.info(f'time elapsed: {t3-t2}')


logger.info('getting all parquet file')
all_files = []

# Use pagination to retrieve all objects
paginator = s3_client.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=oedi_s3_bucket, Prefix=xstock_folder):
    for content in page.get('Contents', []):
        all_files.append(content['Key'])
t4 = perf_counter()
logger.info(f'time elapsed: {t4-t3}')


logger.info('get all unsigned')
presigned_files = []

for file in all_files:
    presigned_url = s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": oedi_s3_bucket, "Key": file},
        ExpiresIn=3600,  # Set expiry time in seconds (1 hour)
    )
    presigned_files.append(presigned_url)

t5 = perf_counter()
logger.info(f'time elapsed: {t5-t4}')

logger.info('reading another parquet file')
t6 = perf_counter()
df = pd.read_parquet(presigned_files[23525])
t7 = perf_counter()
logger.info(f'time elapsed: {t7-t6}')

"""
220.87 Determining Existing
Loads. "The calculation of feeder or service load for existing installations shall be permitted to use actual maximum 
demand to determine the existing load under all of the following conditions:

(1) The maximum demand data is available for a 1-year period. 

Exception: If the maximum demand data for a 1-year period is not available, the calculated load shall be permitted to be 
based on the maximum demand (the highest average kilowatts reached and maintained for a 15-minute interval) continuously 
recorded over a minimum 30-day period using a recording ammeter or power meter connected to the highest loaded phase of 
the feeder or service, based on the initial loading at the start of the recording. The recording shall reflect the 
maximum demand or the feeder or service by being taken when the building or space is occupied and shall include by 
measurement or calculation the larger of the healing or cooling equipment load, and other loads that might be periodic 
in nature due to seasonal or similar conditions. This exception shall not be permitted if the feeder or service has a 
renewable energy system (i.e., solar photovoltaic or wind electric) or employs any form of peak load shaving.

(2) The maximum demand at 125 percent plus the new load does not exceed the ampacity of the feeder or rating of the 
service.

(3) the feeder has overcurrent protection in accordance with 240.4, and the service has overload protection in 
accordance with 230.90.
"""


load_col = 'out.electricity.total.energy_consumption'
max_load = df.iloc[df[load_col].argmax()][['timestamp', load_col]]
building_id = presigned_url.split('/')[-1].split('.')[0]


