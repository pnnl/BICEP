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


def building_peak_loads(upgrade=0, residential=1):
    query = select(PeakLoad).where(PeakLoad.upgrade == upgrade,
                                   PeakLoad.residential == residential)
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


if __name__ == '__main__':
    building_peak_load_diff(non_zero_upgrade=4, residential=1)
    building_peak_load_diff(non_zero_upgrade=6, residential=1)
    building_peak_load_diff(non_zero_upgrade=3, residential=0)
