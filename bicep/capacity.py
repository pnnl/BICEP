"""
Capacity estimation for existing stock and future stock under given decarbonization
scenarios.

Technology adoption forecasts are provided by external models. Building technologies
(e.g., heat pumps, heat pump water heaters) are sourced from ComStock/Restock/Scout;
electric vehicle estimates come from TEMPO; and pv capacity estimates are given
by dGen/ReEDS.
"""

from loguru import logger
import pandas as pd
from sqlalchemy import select

from utils.db_models import PeakLoad, query_to_df, engines
from utils.sampling import utilization_samples

PANEL_SIZES = [30, 50, 60, 70, 100, 125, 150, 200, 250, 300, 400, 600, 800, 1000,
               1200, 2000, 3000, 4000]


def building_peak_loads(upgrade=0, residential=1):
    if residential in (0, 1):
        query = select(PeakLoad).where(PeakLoad.upgrade == upgrade,
                                       PeakLoad.residential == residential)
    else:
        query = select(PeakLoad).where(PeakLoad.upgrade == upgrade)
    return query_to_df(query)


def building_peak_load_diff(non_zero_upgrade, residential):
    logger.info(f'Retrieving peak loads for upgrade {non_zero_upgrade}, '
                f'residential={residential}')
    baseline = building_peak_loads(0, residential)
    upgrade = building_peak_loads(non_zero_upgrade, residential)

    logger.info(f'Calculating peak load differences')
    merged = pd.merge(left=baseline.drop(columns=['timestamp', 'file_path', 'upgrade']),
                      right=upgrade.drop(columns=['timestamp', 'file_path']),
                      on=['building_id', 'state', 'release', 'residential'],
                      suffixes=("_baseline", '_upgrade'))

    merged['peak_diff_kwh'] = merged['max_elec_consumption_kwh_upgrade'] - merged['max_elec_consumption_kwh_baseline']
    merged.drop(columns=['max_elec_consumption_kwh_upgrade',
                         'max_elec_consumption_kwh_baseline'],
                inplace=True)
    logger.info(f'Uploading peak load differences')
    with engines['x-stock'].connect() as connection:
        with connection.begin():
            merged.to_sql(name='load-diff', con=connection, chunksize=1000,
                          if_exists='append', index=False)
            logger.info('Peak loads uploaded for upgrade {non_zero_upgrade}, '
                        f'residential={residential}')


def calc_building_peak_loads():
    building_peak_load_diff(non_zero_upgrade=4, residential=1)
    building_peak_load_diff(non_zero_upgrade=6, residential=1)
    building_peak_load_diff(non_zero_upgrade=3, residential=0)


class CapacityEstimate:
    def __init__(self, upgrade, oversize_dist,
                 residential_voltage=240, commercial_voltage=480, medium_voltage=12470,
                 max_light_comm_amp=4000,
                 safety_factor=1.25):
        self.upgrade = upgrade
        self.buildings = None
        self.oversize_distribution = oversize_dist
        self.safety_factor = safety_factor

        self.resid_volt = residential_voltage
        self.comm_volt = commercial_voltage

        self.max_comm_amp = max_light_comm_amp
        self.med_volt = medium_voltage

        self.get_baseline_loads()

    def get_baseline_loads(self):
        self.buildings = building_peak_loads(self.upgrade, residential=-1)

    def calculate_existing_capacity(self):
        """Estimate the existing capacity of the baseline stock models"""
        bldg = self.buildings

        # calculate peak kw from max 15-min interval kwh data
        bldg['peak_kw'] = bldg['max_elec_consumption_kwh'] / 0.25

        # calculate associate peak amperage and capture assumed voltage
        bldg['peak_amp'] = 0.0
        bldg['assume_volt'] = 0
        bldg.loc[bldg['residential'] == 1, 'peak_amp'] = bldg['peak_kw'] / self.resid_volt * 1000
        bldg.loc[bldg['residential'] == 1, 'assume_volt'] = self.resid_volt

        bldg.loc[bldg['residential'] == 0, 'peak_amp'] = bldg['peak_kw'] / self.comm_volt * 1000
        bldg.loc[bldg['residential'] == 0, 'assume_volt'] = self.comm_volt

        # Assume medium voltage (e.g., 12.47 kV) service for large commercial
        bldg.loc[
            ((bldg['residential'] == 0) & (bldg['peak_amp'] > self.max_comm_amp)),
            'assume_volt'] = self.med_volt
        bldg.loc[bldg['assume_volt'] == self.med_volt, 'peak_amp'] = bldg['peak_kw'] / self.med_volt * 1000

        # calculate required capacity from peak load
        bldg['req_capacity'] = bldg['peak_amp'] * self.safety_factor

        # generate estimates for current utilization of installed capacity
        bldg['utilization'] = utilization_samples(sample_size=len(self.buildings))

        # estimate existing capacity based on utilization draws
        bldg['est_capacity'] = bldg['peak_amp'] / bldg['utilization']

        # round up to the next largest panel size
        def round_up_to_panel_size(x):
            return min(PANEL_SIZES, key=lambda val: (val - x) if val >= x else float('inf'))

        bldg['installed_capacity'] = bldg['est_capacity'].apply(round_up_to_panel_size)

        bldg['spare_capacity'] = bldg['installed_capacity'] - bldg['req_capacity']


if __name__ == '__main__':
    # building_peak_load_diff(non_zero_upgrade=4, residential=1)
    # building_peak_load_diff(non_zero_upgrade=6, residential=1)
    # building_peak_load_diff(non_zero_upgrade=3, residential=0)

    import time
    t0 = time.perf_counter()
    cap = CapacityEstimate(0, 0)
    cap.calculate_existing_capacity()
    t1 = time.perf_counter()

    print(t1 - t0)

    # import plotly.express as px
    # fig = px.histogram(cap.buildings, x='peak_amp')
    # fig.show()
