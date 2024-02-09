"""This module defines the distributions and sampling functions used throughout
BICEP, such as the distribution of panel utilization of the existing stock and
the distribution of upgrade costs.
"""

from io import BytesIO

import numpy as np
import pandas as pd
from scipy.stats import invweibull, lognorm, gaussian_kde
from azure.storage.blob import BlobServiceClient

from utils.db_models import query_to_df
from utils.sensitive_config import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

container_name = 'bicep'
panel_capacity_file = 'panel_capacity.csv'
BLOB_URL = account_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net"
service_client = BlobServiceClient(BLOB_URL, credential=AZURE_STORAGE_KEY)


def sample_xstock(sample_size, residential):
    """Generate a random sample of building IDs from the xstock models"""
    if residential in (1, 0):
        where_clause = f'where residential = {residential} '
    else:
        where_clause = ''
    query = (f'SELECT TOP {sample_size} building_id, residential FROM [stock-meta] '
             f'{where_clause}'
             f'ORDER BY NEWID()')

    return query_to_df(query)


def get_panel_data():
    blob_client = service_client.get_blob_client(container=container_name,
                                                 blob=panel_capacity_file)

    # Download the blob's content as a stream
    with BytesIO() as input_blob:
        blob_client.download_blob().readinto(input_blob)
        input_blob.seek(0)  # Seek to the start of the stream
        panel_data = pd.read_csv(input_blob)
    return panel_data


def utilization_distribution():
    """Distribution of panel utilization based on empirical data from HEA"""
    panel_data = get_panel_data()
    panel_data['perc_utilize'] = panel_data['utilized'] / panel_data['panel size']
    return gaussian_kde(panel_data['perc_utilize'])


def utilization_samples(sample_size, min_value=0.02):
    """
    Sample the utilization_distribution and return n=sample_size samples.
    Constrain samples to be larger or equal to min_value.
    """
    util_dist = utilization_distribution()
    initial_samples = util_dist.resample(sample_size)

    valid_samples = initial_samples[initial_samples >= min_value]
    num_too_small = sample_size - len(valid_samples)

    while num_too_small > 0:
        new_samples = util_dist.resample(sample_size)
        all_samples = np.concatenate([valid_samples, new_samples.reshape(-1)])
        valid_samples = all_samples[all_samples >= min_value]
        num_too_small = sample_size - len(valid_samples)

    return valid_samples[:sample_size]


def residential_panel_distribution():
    """Distribution of residential panel sized based on empirical data from HEA"""
    panel_data = get_panel_data()

    panel_sizes = panel_data['panel size'].unique()
    # todo: binned panel capacity probability based on peak amp


def residential_panel_cost_distribution(distribution='lognormal'):
    if distribution == 'lognormal':
        shape = 1.2
        scale = 3000
        loc = 0
        dist = lognorm(s=shape, scale=scale, loc=loc)

    elif distribution == 'frechet':
        c = 1.5
        scale = 1500
        loc = 1000
        dist = invweibull(c=c, scale=scale, loc=loc)

    else:
        raise KeyError('Distribution choices are ("lognormal", "frechet")')

    # x = np.linspace(dist.ppf(0.01), dist.ppf(0.99), 100)
    samples = dist.rvs(size=100)

    return dist, samples


if __name__ == '__main__':
    # used for testing

    # sample = utilization_distribution()
    # sample = sample_xstock(12, -1)
    sample = utilization_samples(1580, 0.2)