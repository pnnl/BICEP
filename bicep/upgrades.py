"""
Estimate the upgrades and associated costs for the modeled existing capacity and
required additional capacity resulting from the decarbonization technology scenarios.
"""

import numpy as np
import numpy_financial as npf

from loguru import logger

from utils.sampling import PanelUpgradeCostDistribution
from bicep.tech_adoption import TechnologyAdoption


class UpgradeEstimator(TechnologyAdoption):
    """
    Estimates the total upgrade costs for the modeled stock.

    Total required capacity is the sum of the adopted upgrades. Total required capacity is
    then compared to the available spare capacity (estimated capacity - utilized capacity).
    If the required capacity is larger than the spare capacity, an upgrade is deemed required.

    Upgrade costs are drawn from a distribution of residential and commercial likely costs.
    If the assumed inflation rate and discount rate are different, the costs are escalated to
    the future value at the year the upgrade is assigned (random draw between base year and
    end year) and then brought back to present value of the base year. If the rates are the same,
    the costs are reported in the base year dollars.

    :param nominal_inflation_rate: the nominal inflation rate used in future value calculation
    :param discount_rate: the discount rate used to bring future values to present (base year) value
    """
    def __init__(self, nominal_inflation_rate=0.02,
                 discount_rate=0.02,
                 cost_distribution=PanelUpgradeCostDistribution,
                 scenario='bau', base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):

        super().__init__(scenario=scenario, base_year=base_year, end_year=end_year, epsilon=epsilon,
                         residential_voltage=residential_voltage,
                         commercial_voltage=commercial_voltage,
                         medium_voltage=medium_voltage, max_light_comm_amp=max_light_comm_amp,
                         ev_charger_amp=ev_charger_amp,
                         panel_safety_factor=panel_safety_factor)
        self.cost_distribution = cost_distribution
        self.discount_rate = discount_rate
        self.inflation_rate = nominal_inflation_rate

        self.calculate_adoptions()

    def calculate_costs(self):
        self._required_upgrades()
        self._upgrade_costs()

    def _required_upgrades(self):
        logger.info('Calculating required upgrades')
        ev_capacity = (self.buildings['ev_adopted'] * self.buildings['ev_req_capacity_amp']).fillna(0)
        pv_capacity = (self.buildings['pv_adopted'] * self.buildings['pv_req_capacity_amp']).fillna(0)
        hp_capacity = (self.buildings['hp_adopted'] * self.buildings['hp_req_capacity_amp']).fillna(0)
        hpwh_capacity = (self.buildings['hpwh_adopted'] * self.buildings['hpwh_req_capacity_amp']).fillna(0)
        self.buildings['net_capacity_diff_amp'] = ev_capacity + pv_capacity + hp_capacity + hpwh_capacity

        self.buildings['required_add_capacity_amp'] = (self.buildings['net_capacity_diff_amp'] -
                                                       self.buildings['spare_capacity'])

        self.buildings['upgrade_required'] = 0
        self.buildings.loc[self.buildings['required_add_capacity_amp'] > 0, 'upgrade_required'] = 1

    def _upgrade_costs(self):
        logger.info('Calculating upgrade costs')
        residential_cost_dist = self.cost_distribution(residential=True)
        commercial_costs_dist = self.cost_distribution(residential=False)

        residential_upgrades = self.buildings.loc[((self.buildings['residential'] == 1) &
                                                   (self.buildings['upgrade_required'] == 1))]

        commercial_upgrades = self.buildings.loc[((self.buildings['residential'] == 0) &
                                                  (self.buildings['upgrade_required'] == 1))]

        num_residential = len(residential_upgrades)
        num_commercial = len(commercial_upgrades)

        residential_costs = residential_cost_dist.constrained_samples(sample_size=num_residential,
                                                                      min_value=0, max_value=35000)
        commercial_costs = commercial_costs_dist.constrained_samples(sample_size=num_commercial,
                                                                     min_value=0, max_value=350000)

        self.buildings['upgrade_costs'] = np.nan

        self.buildings.loc[
            ((self.buildings['residential'] == 1) &
             (self.buildings['upgrade_required'] == 1)),
            'upgrade_costs'] = residential_costs

        self.buildings.loc[
            ((self.buildings['residential'] == 0) &
             (self.buildings['upgrade_required'] == 1)),
            'upgrade_costs'] = commercial_costs

        if self.discount_rate != self.inflation_rate:
            possible_years = np.arange(start=self.base_year, stop=self.end_year + 1, dtype=int)

            num_upgrades = num_commercial + num_residential
            upgrade_year = np.random.choice(possible_years, num_upgrades, replace=True)
            self.buildings['upgrade_year'] = np.nan

            self.buildings.loc[self.buildings['upgrade_required'] == 1, 'upgrade_year'] = upgrade_year

            # future value
            self.buildings['fv_upgrade_cost'] = self.buildings.apply(
                lambda row: npf.fv(rate=self.inflation_rate,
                                   nper=self.base_year - row['upgrade_year'],
                                   pmt=0,
                                   pv=row['upgrade_costs']), axis=1)

            # future value
            self.buildings['pv_upgrade_cost'] = self.buildings.apply(
                lambda row: npf.pv(rate=self.discount_rate,
                                   nper=self.base_year - row['upgrade_year'],
                                   pmt=0,
                                   fv=row['fv_upgrade_cost']), axis=1)


if __name__ == '__main__':
    # used for testing
    up = UpgradeEstimator(nominal_inflation_rate=.03)
    up._required_upgrades()
    up._upgrade_costs()
