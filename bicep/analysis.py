"""
Aggregation of the total costs at a desired spatial resolution (e.g., state or national).
"""

from bicep.upgrades import UpgradeEstimator
from utils.sampling import PanelUpgradeCostDistribution


class BicepResults(UpgradeEstimator):
    def __init__(self, aggregation_level='state', annualized=True,
                 upgrade_lifespan=25,
                 nominal_inflation_rate=0.02,
                 discount_rate=0.02,
                 cost_distribution=PanelUpgradeCostDistribution,
                 scenario='bau', base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):
        super().__init__(aggregation_level=aggregation_level, annualized_costs=annualized,
                         upgrade_lifespan=upgrade_lifespan,
                         nominal_inflation_rate=nominal_inflation_rate, discount_rate=discount_rate,
                         cost_distribution=cost_distribution,
                         scenario=scenario, base_year=base_year, end_year=end_year, epsilon=epsilon,
                         residential_voltage=residential_voltage,
                         commercial_voltage=commercial_voltage,
                         medium_voltage=medium_voltage, max_light_comm_amp=max_light_comm_amp,
                         ev_charger_amp=ev_charger_amp,
                         panel_safety_factor=panel_safety_factor)

        self.calculate_costs()

    def requirements_by_tech(self, residential=1):
        techs_caps = ['ev_req_capacity_amp', 'pv_req_capacity_amp',
                      'hp_req_capacity_amp', 'hpwh_req_capacity_amp']

        if residential == 1:
            dataset = self.residential
        elif residential == 0:
            dataset = self.commercial
        else:
            dataset = self.buildings

        return dataset[techs_caps].describe()


if __name__ == '__main__':
    bau = BicepResults(scenario='bau')
    high = BicepResults(scenario='high')

    print(f'total cost for bau: ${bau.total_cost:,.0f}')
    print(f'total cost for high: ${high.total_cost:,.0f}')
