"""
Electrical service capacity estimation for existing building stock and required
additional capacity for the technologies in the decarbonization scenarios.

Existing installed electrical capacity of the building stock is estimated
based on peak load data from Com/ResStock (xStock) building energy models (BEMs).

Required capacity is also estimated for the different building technologies
in the DECARB scenarios. Currently included technologies are: heat pumps,
heat pump hot water heaters, PV, and light-duty EVs.

Existing capacity estimates are based on the National Electric Code (NEC) optional
method for determining required panel capacity based on load data (NEC 220.87
Determining Existing Loads). The peak load data is pulled from the xStock BEMs.
The required NEC safety factor of 25% is applied to the peak load. The peak power
value is converted to a peak current value using an assumed voltage based on the
building class (residential vs commercial) and the magnitude of the load. The panel
capacity is then estimated using an empirical distribution of panel utilization factors.

The HP and HPWH capacity requirements are estimated from the xStock
EUSS data which models each building assuming the technology upgrade. Peak load
differences are assumed to be the result of the technology upgrade.

PV system sizing is estimated from the buildings' peak load and a distribution
of PV system sizes relative to the buildings' peak load. The current requirement
is estimated from the PV system size and the same assumed voltage as the panel
capacity estimate.

EV capacity requirements are estimated from an estimated number of EV chargers
at the building. The total number parking spaces is based on a distribution of
parking spaces per 1000 sf from the Institute of Transportation Engineers with a
mean value of 3.8 spaces / ksf. An assumed percentage of EV spaces to regular stalls
is applied from a normal distribution of a rule of thumb estimate with a mean of
7.5% EV spaces. Each charger is assumed to be a Level 2 charger with a fixed
current requirement.
"""

from loguru import logger
import pandas as pd
import numpy as np
from sqlalchemy import select

from utils.db_models import LoadDifference, StockMeta, PeakLoad, query_to_df, engines
import utils.sampling as sampling

PANEL_SIZES = [
    # 30, 50, 60, 70,
    100, 125, 150, 200, 250, 300, 400, 600, 800, 1000,
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
    """
    Estimates the existing installed electrical capacity of the building stock
    based on peak load data from Com/ResStock (xStock) building energy models (BEMs).

    Required capacity is also estimated for the different building technologies
    in the DECARB scenarios. Currently included technologies are: heat pumps,
    heat pump hot water heaters, PV, and light-duty EVs.

    Existing capacity estimates are based on the National Electric Code (NEC) optional
    method for determining required panel capacity based on load data (NEC 220.87
    Determining Existing Loads). The peak load data is pulled from the xStock BEMs.
    The required NEC safety factor of 25% is applied to the peak load. The peak power
    value is converted to a peak current value using an assumed voltage based on the
    building class (residential vs commercial) and the magnitude of the load. The panel
    capacity is then estimated using an empirical distribution of panel utilization factors.

    The HP and HPWH capacity requirements are estimated from the xStock
    EUSS data which models each building assuming the technology upgrade. Peak load
    differences are assumed to be the result of the technology upgrade.

    PV system sizing is estimated from the buildings' peak load and a distribution
    of PV system sizes relative to the buildings' peak load. The current requirement
    is estimated from the PV system size and the same assumed voltage as the panel
    capacity estimate.

    EV capacity requirements are estimated from an estimated number of EV chargers
    at the building. The total number parking spaces is based on a distribution of
    parking spaces per 1000 sf from the Institute of Transportation Engineers with a
    mean value of 3.8 spaces / ksf. An assumed percentage of EV spaces to regular stalls
    is applied from a normal distribution of a rule of thumb estimate with a mean of
    7.5% EV spaces. Each charger is assumed to be a Level 2 charger with a fixed
    current requirement.
    """
    def __init__(self, residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):
        """

        :param residential_voltage: Assumed voltage for residential electrical service
        :param commercial_voltage: Assumed voltage for light commercial electrical service
        :param medium_voltage: Assumed voltage for large commercial services
        :param max_light_comm_amp: Current threshold for assuming medium voltage
        :param ev_charger_amp: Fixed current requirement for Level 2 EV charger
        :param panel_safety_factor: NEC panel safety of 25%
        """
        self.buildings = None
        self.building_meta = None
        self.safety_factor = panel_safety_factor

        self.ev_charger_amp = ev_charger_amp

        self.resid_volt = residential_voltage
        self.comm_volt = commercial_voltage

        self.max_comm_amp = max_light_comm_amp
        self.med_volt = medium_voltage

        self.get_baseline_loads()
        self.get_meta()

    def get_baseline_loads(self):
        logger.info('Getting baseline peak loads')
        self.buildings = building_peak_loads(upgrade=0, residential=-1)

    def get_meta(self):
        logger.info('Getting stock metadata')
        self.building_meta = query_to_df(select(StockMeta).where(StockMeta.state == 'CA'))

    def calculate_capacity(self):
        self.calculate_existing_capacity()
        self.building_req_capacity()
        self.pv_req_capacity()
        self.ev_req_capacity()

    def calculate_existing_capacity(self):
        """Estimate the existing capacity of the baseline stock models"""

        logger.info('Calculating existing stock capacity')
        # join building sqft to peak load data
        self.buildings.set_index(['building_id', 'residential'], inplace=True)
        self.building_meta.set_index(['building_id', 'residential'], inplace=True)
        self.buildings = self.buildings.join(self.building_meta[['sqft', 'weight', 'total_units', 'occupant_density_m_2']])
        self.buildings.reset_index(inplace=True)
        self.building_meta.reset_index(inplace=True)

        bldg = self.buildings  # less verbose

        # calculate peak kw from max 15-min interval kwh data
        bldg['peak_kw'] = bldg['max_elec_consumption_kwh'] / 0.25

        # calculate associate peak amperage and capture assumed voltage
        bldg['peak_amp'] = 0.0
        bldg['assumed_volt'] = 0
        bldg.loc[bldg['residential'] == 1, 'peak_amp'] = bldg['peak_kw'] / self.resid_volt * 1000
        bldg.loc[bldg['residential'] == 1, 'assumed_volt'] = self.resid_volt

        bldg.loc[bldg['residential'] == 0, 'peak_amp'] = bldg['peak_kw'] / self.comm_volt * 1000
        bldg.loc[bldg['residential'] == 0, 'assumed_volt'] = self.comm_volt

        # generate estimates for current utilization of installed capacity
        utilization_dist = sampling.PanelUtilizationDistribution()
        bldg['utilization'] = utilization_dist.constrained_samples(sample_size=len(self.buildings),
                                                                   min_value=0.1)

        # Assume medium voltage (e.g., 12.47 kV) service for large commercial
        bldg.loc[
            ((bldg['residential'] == 0) & (bldg['peak_amp'] > self.max_comm_amp)),
            'assumed_volt'] = self.med_volt
        bldg.loc[bldg['assumed_volt'] == self.med_volt, 'peak_amp'] = bldg['peak_kw'] / self.med_volt * 1000

        # estimate existing capacity based on utilization draws
        bldg['est_capacity'] = bldg['peak_amp'] / bldg['utilization']

        # apply medium voltage to the buildings with estimated capacity over max commercial amps
        bldg.loc[
            ((bldg['residential'] == 0) & (bldg['est_capacity'] > self.max_comm_amp)),
            'assumed_volt'] = self.med_volt
        bldg.loc[bldg['assumed_volt'] == self.med_volt, 'peak_amp'] = bldg['peak_kw'] / self.med_volt * 1000

        # re-estimate existing capacity based on updated peak_amps and utilization draws
        bldg['est_capacity'] = bldg['peak_amp'] / bldg['utilization']

        # calculate required capacity from peak load
        bldg['req_capacity'] = bldg['peak_amp'] * self.safety_factor

        # round up to the next largest panel size
        def round_up_to_panel_size(x):
            if x > max(PANEL_SIZES):
                return np.nan
            else:
                return min(PANEL_SIZES, key=lambda val: (val - x) if val >= x else float('inf'))

        bldg['installed_capacity'] = bldg['est_capacity'].apply(round_up_to_panel_size)

        bldg['spare_capacity'] = bldg['installed_capacity'] - bldg['req_capacity']

    def building_req_capacity(self, hp_upgrades=(3, 4,), hpwh_upgrades=(6, )):
        """Estimate required capacity for the building technologies: HP, HPWH"""

        logger.info('Estimating required capacity for building techs')

        # retrieve calculated load differences
        all_upgrades = query_to_df(select(LoadDifference))

        # separate the load difference values based on the upgrades
        hp_load_diff = all_upgrades[all_upgrades['upgrade'].isin(hp_upgrades)]
        hpwh_load_diff = all_upgrades[all_upgrades['upgrade'].isin(hpwh_upgrades)]

        # join the load difference to the main building dataframe
        hp_load_diff.set_index(['building_id', 'residential'], inplace=True)
        hpwh_load_diff.set_index(['building_id', 'residential'], inplace=True)

        hp_load_diff = hp_load_diff.rename(columns={'peak_diff_kwh': 'hp_peak_diff_kwh'})
        hpwh_load_diff = hpwh_load_diff.rename(columns={'peak_diff_kwh': 'hpwh_peak_diff_kwh'})

        self.buildings.set_index(['building_id', 'residential'], inplace=True)
        self.buildings = self.buildings.join(hp_load_diff['hp_peak_diff_kwh'], how='outer')
        self.buildings = self.buildings.join(hpwh_load_diff['hpwh_peak_diff_kwh'], how='outer')
        self.buildings.reset_index(inplace=True)

        bldg = self.buildings  # less verbose

        # convert peak load difference to kw and then amp
        bldg['hp_req_capacity_amp'] = bldg['hp_peak_diff_kwh'] / bldg['assumed_volt'] * 1000 / 0.25
        bldg['hpwh_req_capacity_amp'] = bldg['hpwh_peak_diff_kwh'] / bldg['assumed_volt'] * 1000 / 0.25

    def pv_req_capacity(self):
        """Estimate the required PV system size and required capacity"""

        logger.info('Estimating PV system size and capacity')

        # generate distribution of pv system sizes relative to the peak load of the building
        pv_size_dist = sampling.PvSizingDistribution()
        pv_sizes = pv_size_dist.constrained_samples(sample_size=len(self.buildings),
                                                    min_value=0.01, max_value=1)

        # calculate pv system size and associated spare capacity required
        self.buildings['pv_relative_size'] = pv_sizes
        self.buildings['pv_size_kw'] = self.buildings['pv_relative_size'] * self.buildings['peak_kw']
        self.buildings['pv_req_capacity_amp'] = self.buildings['pv_size_kw'] / self.buildings['assumed_volt'] * 1000

    def ev_req_capacity(self):
        """Estimate the number of EV charges (assumed to be Level 2) and the total required capacity"""

        logger.info('Estimating number of EV chargers and total required capacity')

        bldg = self.buildings  # less verbose
        num_commercial = len(bldg.loc[bldg['residential'] == 0])
        num_residential = len(bldg.loc[bldg['residential'] == 1])

        # generate distribution for total parking spots for the commercial buildings per 1000 SF
        comm_parking_dist = sampling.ParkingSpotsDistribution()
        parking_per_ksf = comm_parking_dist.constrained_samples(sample_size=num_commercial,
                                                                min_value=0.5)

        # generate distribution and samples for % EV spaces compared to total parking
        ev_parking_dist = sampling.EvSpotsDistribution()
        comm_percent_ev = ev_parking_dist.constrained_samples(sample_size=num_commercial,
                                                              min_value=0.01)

        # generate samples for residential EV spaces
        res_evs_dist = sampling.ResidentialEvDistribution(mean_value=1.25, std=.3)
        res_evs = res_evs_dist.constrained_samples(sample_size=num_residential, min_value=0.01)

        bldg['total_parking_spaces'] = 0.0
        bldg['perc_ev_spaces'] = 0.0
        bldg['ev_spaces'] = 0.0

        # commercial buildings total parking
        commercial_ksf = bldg.loc[bldg['residential'] == 0, 'sqft'] / 1000
        bldg.loc[bldg['residential'] == 0, 'total_parking_spaces'] = parking_per_ksf * commercial_ksf
        bldg.loc[bldg['residential'] == 1, 'total_parking_spaces'] = res_evs

        bldg.loc[bldg['residential'] == 0, 'perc_ev_spaces'] = comm_percent_ev
        bldg.loc[bldg['residential'] == 1, 'perc_ev_spaces'] = 1

        # calibrating to ~50M vehicles
        bldg['represented_vehicles'] = (bldg['total_units']/5).fillna(1) * bldg['total_parking_spaces']

        bldg['ev_spaces'] = bldg['total_parking_spaces'] * bldg['perc_ev_spaces']
        bldg['ev_spaces'] = np.ceil(bldg['ev_spaces'])
        bldg['ev_req_capacity_amp'] = bldg['ev_spaces'] * self.ev_charger_amp * self.resid_volt / bldg['assumed_volt']

    def calculate_parking_spaces_mhdv(self):
        """
        Calculate the number of parking spots for each building
        based on its type, floor area, or occupant density.
        """
        logger.info('Estimating the number of parking spots in commercial buildings and the EV space')

        building_sqft_per_spot = {
            'Outpatient': 200,
            'LargeOffice': 620,
            'SmallOffice': 250,
            'RetailStandalone': 285.7,
            'Warehouse': 1000,
            'RetailStripmall': 215,
            'QuickServiceRestaurant': 100,
            'MediumOffice': 250,
            'FullServiceRestaurant': 100
        } # Building area per parking spot by building type (ComStock Reference Documentation:Version 1, Table 61)

        bldg = self.buildings
        bldg = self.buildings[self.buildings['residential'] == 0].copy() # Filter only commercial buildings
        num_commercial = len(bldg.loc[bldg['residential'] == 0])
        bldg['total_parking_spaces'] = 0.0
        bldg['mhdv_ev_parking_space'] = 0.0

        # Estimating parking spaces in building based on area
        area_based_mask = bldg['in.comstock_building_type'].isin(building_sqft_per_spot.keys())
        bldg.loc[area_based_mask, 'total_parking_spaces'] = bldg.loc[area_based_mask].apply(
            lambda row: row['in.sqft'] / building_sqft_per_spot[row['in.comstock_building_type']], axis=1)

        # Estimating parking spaces in hotels based on occupancy
        hotel_mask = bldg['in.comstock_building_type'].isin(['SmallHotel', 'LargeHotel'])
        if hotel_mask.any():
            bldg.loc[hotel_mask, 'num_people'] = (
                    bldg.loc[hotel_mask, 'in.sqft'] *
                    bldg.loc[hotel_mask, 'out.params.occupant_density_ppl_per_m_2..people_per_m2'] * 0.0929) # ft² to m²: 1 ft² = 0.0929 m²

            bldg.loc[hotel_mask, 'num_hotel_units'] = bldg.loc[hotel_mask, 'num_people'] * 0.65 / 1.5 # hotel occupancy rate: 0.65 and occupants per hotel units: 1.5
            bldg.loc[hotel_mask, 'total_parking_spaces'] = bldg.loc[hotel_mask, 'num_hotel_units']

        # Estimate parking spaces in schools based on number of students
        primary_mask = bldg['in.comstock_building_type'] == 'PrimarySchool'
        secondary_mask = bldg['in.comstock_building_type'] == 'SecondarySchool'

        school_mask = primary_mask | secondary_mask
        if school_mask.any():
            bldg.loc[school_mask, 'num_people'] = (
                    bldg.loc[school_mask, 'in.sqft'] *
                    bldg.loc[school_mask, 'out.params.occupant_density_ppl_per_m_2..people_per_m2'] * 0.0929)
            bldg.loc[primary_mask, 'total_parking_spaces'] = bldg.loc[primary_mask, 'num_people'] / 17 # Students per parking space (ComStock Reference Documentation:Version 1, Table 61)
            bldg.loc[secondary_mask, 'total_parking_spaces'] = bldg.loc[secondary_mask, 'num_people'] / 8 # Students per parking space (ComStock Reference Documentation:Version 1, Table 61)

        # Estimate parking spaces in hospital based on area and number of beds
        hospital_mask = bldg['in.comstock_building_type'] == 'Hospital'
        bldg.loc[hospital_mask, 'total_parking_spaces'] = bldg.loc[hospital_mask, 'in.sqft'] * 0.000875 * 0.83 # 0.83 beds per parking space (Comstock Reference Doc. Table 61)  and 175 beds per 200,000 sq ft area (0.000875) ( https://www.definitivehc.com/resources/healthcare-insights/average-us-hospital-square-footage#:~:text=How%20does%20hospital%20facility%20size,of%201.2%20million%20square%20feet.)

        # Removing intermediate columns
        bldg.drop(columns=['num_people', 'num_hotel_units'], inplace=True, errors='ignore')

        # generate distribution and samples for % MHDV EV spaces compared to total parking
        mhdv_ev_parking_dist = sampling.EvSpotsDistribution()
        percent_mhdv_ev = mhdv_ev_parking_dist.constrained_samples(sample_size = num_commercial, min_value=0.01)
        bldg['mhdv_ev_parking_space'] = percent_mhdv_ev*bldg['total_parking_spaces']


    def mhdv_ev_req_capacity(self, total_counts, seed=42):
        """
        Estimates the required charging capacity for MHDVs in each commercial building,
        based on charger type distribution and number of parking spaces.
        """

        logger.info('Estimating the required capacity of MHDV chargers')

        # Charger power ratings (in kW)
        power_rating = {
            "L2": 19.2,
            "DC50": 50,
            "DC150": 150,
            "DC250": 250,
            "DC350": 350,
            "DC500": 500,
            "DC750": 750,
            "DC1000": 1000,
            "DC1500": 1500,
            "DC2000": 2000,
            "DC3750": 3750
        }

        # Normalize charger distribution
        charger_types = list(total_counts.keys())
        counts = np.array(list(total_counts.values()), dtype=float)
        probabilities = counts / counts.sum()

        # Filter only commercial buildings
        bldg = self.buildings[self.buildings['residential'] == 0].copy()

        np.random.seed(seed)
        charger_allocations = []

        for _, row in bldg.iterrows():
            num_chargers = int(row['mhdv_ev_parking_spaces'])
            sampled = np.random.choice(charger_types, size=num_chargers, p=probabilities)
            allocation = {ct: 0 for ct in charger_types}
            for ct in sampled:
                allocation[ct] += 1
            charger_allocations.append(allocation)

        # Compute total required capacity for each building
        total_capacity = []
        for alloc in charger_allocations:
            capacity = sum(alloc[ct] * power_rating[ct] for ct in charger_types)
            total_capacity.append(capacity)

        # Assign results back to the dataframe
        bldg['mhdv_amp'] = total_capacity
        bldg['mhdv_ev_req_capacity_amp'] = bldg['mhdv_amp'] * bldg['mhdv_ev_parking_spaces']

        self.buildings.loc[bldg.index, 'mhdv_amp'] = bldg['mhdv_amp']
        self.buildings.loc[bldg.index, 'mhdv_ev_req_capacity_amp'] = bldg['mhdv_ev_req_capacity_amp']

    # total_counts = {
    #     "L2": 1200,
    #     "DC50": 300,
    #     "DC150": 400,
    #     "DC250": 200,
    #     "DC350": 100,
    #     "DC500": 60,
    #     "DC750": 50,
    #     "DC1000": 30,
    #     "DC1500": 20,
    #     "DC2000": 10,
    #     "DC3750": 5
    # }
    #
    # cap.mhdv_ev_req_capacity(total_counts, seed=2025)


if __name__ == '__main__':
    # building_peak_load_diff(non_zero_upgrade=4, residential=1)
    # building_peak_load_diff(non_zero_upgrade=6, residential=1)
    # building_peak_load_diff(non_zero_upgrade=3, residential=0)

    import time

    t0 = time.perf_counter()
    cap = CapacityEstimate()
    cap.calculate_existing_capacity()
    cap.building_req_capacity()
    cap.pv_req_capacity()
    cap.ev_req_capacity()
    cap.calculate_parking_spaces_mhdv()  ### SA
    cap.mhdv_ev_req_capacity()  ### SA
    t1 = time.perf_counter()

    print(t1 - t0)

    res = cap.buildings[cap.buildings['residential'] == 1]
    com = cap.buildings[cap.buildings['residential'] == 0]

    res_ev = res['ev_spaces'] * res['weight']
    com_ev = com['ev_spaces'] * com['weight']

    print(f'residential evs: {int(res_ev.sum()):,}')
    print(f'commercial evs: {int(com_ev.sum()):,}')
    # import plotly.express as px
    # fig = px.histogram(cap.buildings, x='peak_amp')
    # fig.show()
