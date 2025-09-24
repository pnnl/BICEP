"""
This module parses one timeseries parquet file from the x_stock outputs and stores the relevant data in a BICEP db.


#### Relevant section of the National Electric Code:

NEC 220.87 Determining Existing Loads.

The calculation of feeder or service load for existing installations shall be permitted to use actual maximum
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

import os
import sys
import argparse

import pandas as pd

from loguru import logger

from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, IntegrityError

from utils.db_models import engines, PeakLoad


logger.add(sys.stdout)
cwd = os.getenv('AZ_BATCH_TASK_WORKING_DIR')
out_file = os.path.join(cwd, 'output.txt')
logger.add(out_file)


def write_to_db(peak_load_data):
    try:
        with Session(engines['x-stock']) as session, session.begin():
            building = peak_load_data.building_id
            session.add(peak_load_data)
        logger.debug(f'Peak load data inserted for {building}')
    except (OperationalError, IntegrityError) as error:
        logger.error(error)
        logger.error(f"Data not inserted in db for: {peak_load_data.building_id}")


def parse_parquet_file(pre_signed_url, state):
    building_id = pre_signed_url.split('/')[-1].split('.')[0]
    file_path = pre_signed_url.split('.com/')[1]
    release = file_path.split('timeseries_individual')[0].split('us-building-stock/')[1]
    residential = 'resstock' in release

    logger.debug(f'Parsing parquet for: {building_id}')

    building_timeseries = pd.read_parquet(pre_signed_url)

    load_col = 'out.electricity.total.energy_consumption'
    max_load = building_timeseries.iloc[building_timeseries[load_col].argmax()]

    peak_load_data = PeakLoad(building_id=int(building_id.split('-')[0]),
                              upgrade=int(building_id.split('-')[1]),
                              max_elec_consumption_kwh=float(max_load[load_col]),
                              timestamp=max_load['timestamp'],
                              state=state,
                              file_path=file_path,
                              release=release,
                              residential=residential)

    write_to_db(peak_load_data)

    with open(out_file, "w") as text_file:
        text_file.write('\n job completed')


def main():
    """
    This function will be run in the Docker container and accept the file path of the
    parquet file to be parsed as an argument.
    :return:
    """
    # Create argument arg_parser
    arg_parser = argparse.ArgumentParser(
        description='Parse xStock parquet files and upload to BICEP db')
    arg_parser.add_argument("--url", type=str,
                            help="URL of the parquet file to be parsed")

    arg_parser.add_argument("--state", type=str,
                            help="The state the building models represent")

    args = arg_parser.parse_args()
    file_path = args.url
    state = args.state

    parse_parquet_file(file_path, state)


if __name__ == '__main__':
    main()
