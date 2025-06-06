"""
This module defines the distributions and sampling functions used throughout
BICEP, such as the distribution of panel utilization of the existing stock and
the distribution of upgrade costs.

Distributions are mix of kernel density estimates from empirical data and parameterize
distributions based on rules of thumb and/or common industry values sourced from
the relevant literature.
"""

from io import BytesIO

import numpy as np
import pandas as pd
from scipy.stats import norm, invweibull, lognorm, gaussian_kde
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
    """
    Pull the empirical sample data for electrical panel size and peak loads
    for the BICEP Azure blob storage
    """
    blob_client = service_client.get_blob_client(container=container_name,
                                                 blob=panel_capacity_file)

    # Download the blob's content as a stream
    with BytesIO() as input_blob:
        blob_client.download_blob().readinto(input_blob)
        input_blob.seek(0)  # Seek to the start of the stream
        panel_data = pd.read_csv(input_blob)
    return panel_data


class BaseDistribution:
    """
    Base class for the distributions. The class provides a method to sample the
    defined distribution and optionally constrain those values to a min and/or max
    value.

    """
    def __init__(self, kernel_fit=True):
        self.distribution = None
        self.kernel_fit = kernel_fit

    def _sample(self, sample_size):
        if self.kernel_fit:
            return self.distribution.resample(sample_size)
        else:
            return self.distribution.rvs(sample_size)

    def constrained_samples(self, sample_size, min_value=None, max_value=None):
        """
        Sample the distribution and return n=sample_size samples.
        Constrain samples to be greater than or equal to min_value and
        less than or equal to max_value.
        """
        try:
            assert self.distribution is not None
        except AssertionError:
            raise ValueError("The distribution must be initialized before sampling")

        samples = self._sample(sample_size)

        if min_value is not None:
            samples = samples[samples >= min_value]

        if max_value is not None:
            samples = samples[samples <= max_value]

        while len(samples) < sample_size:
            new_samples = self._sample(sample_size)
            samples = np.concatenate([samples, new_samples.reshape(-1)])
            if min_value is not None:
                samples = samples[samples >= min_value]

            if max_value is not None:
                samples = samples[samples <= max_value]

        return samples[:sample_size]

    def plot_distribution(self, min_value=None, max_value=None):
        import plotly.express as px
        samples = self.constrained_samples(sample_size=5000,
                                           min_value=min_value, max_value=max_value)
        histogram = px.histogram(x=samples)
        histogram.show()


class PanelUtilizationDistribution(BaseDistribution):
    """Distribution of panel utilization (peak load / panel capacity) sourced from empirical data"""
    def __init__(self):
        super().__init__(kernel_fit=True)
        self._init_distribution()

    def _init_distribution(self):
        panel_data = get_panel_data()
        panel_data['perc_utilize'] = panel_data['utilized'] / panel_data['panel size']
        self.distribution = gaussian_kde(panel_data['perc_utilize'])


class PvSizingDistribution(BaseDistribution):
    """
    Distribution of PV sizes relative to the building peak load based on a very small
    sample of data from NREL's Nationwide Analysis of U.S. Commercial Building Solar
    (NREL/TP-6A20-64793).
    """
    def __init__(self):
        super().__init__(kernel_fit=True)
        self._init_distribution()

    def _init_distribution(self):
        # from NREL/TP-6A20-64793
        # Nationwide Analysis of U.S. Commercial Building Solar
        # Photovoltaic (PV) Breakeven Conditions
        nrel_data = [0.16139, 0.17360, 0.19603, 0.19938, 0.26415, 0.30769, 0.38235,
                     0.38346, 0.54924, 0.75204, 1.13462, 1.13978, 1.36842, 1.38415, 1.95833]

        self.distribution = gaussian_kde(nrel_data)


class EvSpotsDistribution(BaseDistribution):
    """
    Distribution of the number of EV parking spots relative to the total number
    of parking spaces. The value varies widely based on local requirements but
    a general rule of thumb is 5-10%. Here a normal distribution is assumed with
    a mean of 7.5% and a standard deviation of 2.5%.
        """
    def __init__(self, mean_value=0.25, std=0.1):
        super().__init__(kernel_fit=False)
        self.mean_value = mean_value
        self.std = std

        self._init_distribution()

    def _init_distribution(self):
        self.distribution = norm(loc=self.mean_value, scale=self.std)


class ParkingSpotsDistribution(BaseDistribution):
    """
    Distribution of the number of total number parking spots based on building
    area. The value varies widely based on local requirements but  default values
    sourced from Institute of Transportation Engineers, Transportation Planning
    Handbook, 3rd edition. Here a normal distribution is assumed with a mean
    of 3.8 spots / ksf with a standard deviation of 0.5.
    """
    def __init__(self, mean_value=3.8, std=.5):
        super().__init__(kernel_fit=False)
        self.mean_value = mean_value
        self.std = std

        self._init_distribution()

    def _init_distribution(self):
        self.distribution = norm(loc=self.mean_value, scale=self.std)


class ResidentialEvDistribution(BaseDistribution):
    """
    Residential EV parking spaces are calculated from the total housing units
    and an EV/housing units factor drawn from a normal distribution with a mean
    of 1.5 and a standard deviation of 0.5.
    """
    def __init__(self, mean_value=1.5, std=.5):
        super().__init__(kernel_fit=False)
        self.mean_value = mean_value
        self.std = std

        self._init_distribution()

    def _init_distribution(self):
        self.distribution = norm(loc=self.mean_value, scale=self.std)


class PanelUpgradeCostDistribution(BaseDistribution):
    """
    Distribution of the costs to upgrade a panel at a building. Empirical data is
    limited and the costs vary widely depending on local electric code, building
    characteristics, and the local utility. A literature search provided some
    insight into the likely range of costs.

    Two skewed distributions (long right tail) are provided here.
    """
    def __init__(self, residential=True, distribution_type='lognormal'):
        super().__init__(kernel_fit=False)
        self.residential = residential
        self.distribution_type = distribution_type

        try:
            assert self.distribution_type in ['lognormal', 'frechet']
        except AssertionError:
            raise KeyError('Distribution choices are ("lognormal", "frechet')

        self._init_distribution()

    def _residential_cost_distribution(self):
        if self.distribution_type == 'lognormal':
            shape = 1.2
            scale = 3000
            loc = 0
            return lognorm(s=shape, scale=scale, loc=loc)

        elif self.distribution_type == 'frechet':
            c = 1.5
            scale = 1500
            loc = 1000
            return invweibull(c=c, scale=scale, loc=loc)

    def _commercial_cost_distribution(self):
        if self.distribution_type == 'lognormal':
            shape = 1.2
            scale = 30000
            loc = 10000
            return lognorm(s=shape, scale=scale, loc=loc)

        elif self.distribution_type == 'frechet':
            c = 1.5
            scale = 15000
            loc = 10000
            return invweibull(c=c, scale=scale, loc=loc)

    def _init_distribution(self):
        if self.residential:
            self.distribution = self._residential_cost_distribution()
        else:
            self.distribution = self._commercial_cost_distribution()


def residential_panel_distribution():
    """Distribution of residential panel sized based on empirical data from HEA"""
    panel_data = get_panel_data()
    panel_sizes = panel_data['panel size'].unique()
    # todo: binned panel capacity probability based on peak amp


if __name__ == '__main__':
    # used for testing

    # sample = utilization_distribution()
    # sample = sample_xstock(12, -1)
    panel_dist = PanelUtilizationDistribution()
    panel_samples = panel_dist.constrained_samples(1580, 0.2)

    pv_dist = PvSizingDistribution()
    pv_samples = pv_dist.constrained_samples(12345, 0.1, 1.25)

    res_cost_dist = PanelUpgradeCostDistribution()
    res_cost_samples = res_cost_dist.constrained_samples(sample_size=1000, min_value=0, max_value=35000)

    com_cost_dist = PanelUpgradeCostDistribution(residential=False)
    com_cost_samples = com_cost_dist.constrained_samples(sample_size=1000, min_value=0, max_value=250000)

    ev_dist = EvSpotsDistribution()
    ev_samples = ev_dist.constrained_samples(sample_size=1000, min_value=0)


