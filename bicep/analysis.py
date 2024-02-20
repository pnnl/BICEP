"""
Aggregation of the total costs at a desired spatial resolution (e.g., state or national).
"""

from bicep.upgrades import UpgradeEstimator
from utils.sampling import PanelUpgradeCostDistribution


class AggregateCosts(UpgradeEstimator):
    def __init__(self, level='state', nominal_inflation_rate=0.02,
                 discount_rate=0.02,
                 cost_distribution=PanelUpgradeCostDistribution,
                 scenario='bau', base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):
        super().__init__(nominal_inflation_rate=nominal_inflation_rate,
                         discount_rate=discount_rate, cost_distribution=cost_distribution,
                         scenario=scenario, base_year=base_year, end_year=end_year, epsilon=epsilon,
                         residential_voltage=residential_voltage,
                         commercial_voltage=commercial_voltage,
                         medium_voltage=medium_voltage, max_light_comm_amp=max_light_comm_amp,
                         ev_charger_amp=ev_charger_amp,
                         panel_safety_factor=panel_safety_factor)

        self.level = level
        self.total_cost = None
        self.state_costs = None

        self.calculate_costs()

    def calculate_total_costs(self):
        if self.discount_rate == self.inflation_rate:
            cost_col = 'upgrade_costs'
        else:
            cost_col = 'pv_upgrade_cost'

        self.state_costs = self.buildings.groupby('state')[cost_col].sum()

        self.total_cost = self.state_costs.sum()


if __name__ == '__main__':
    bau = AggregateCosts(scenario='bau')
    high = AggregateCosts(scenario='high')

    bau.calculate_total_costs()
    high.calculate_total_costs()

    print(f'total cost for bau: ${bau.total_cost:,.0f}')
    print(f'total cost for high: ${high.total_cost:,.0f}')

