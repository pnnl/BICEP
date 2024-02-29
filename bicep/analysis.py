"""
Analysis of the BICEP model run results.
"""

import plotly.express as px
import plotly.graph_objs as go

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
        self._capacity_requirement_cols = ['ev_req_capacity_amp', 'pv_req_capacity_amp',
                                           'hp_req_capacity_amp', 'hpwh_req_capacity_amp']

    def requirements_by_tech(self, residential=1):
        dataset = self._filter_dataset(residential)
        return dataset[self._capacity_requirement_cols].describe()

    def _filter_dataset(self, residential=1):
        if residential == 1:
            return self.residential
        elif residential == 0:
            return self.commercial
        else:
            return self.buildings

    def plot_drivers(self, residential=1, cdf=True):
        dataset = self._filter_dataset(residential=residential)
        plot_df = dataset.rename(columns={
                              "pv_req_capacity_amp": "PV",
                              "ev_req_capacity_amp": "EV",
                              "hp_req_capacity_amp": "HP",
                              "hpwh_req_capacity_amp": "HP WH"})
        plot_cols = ['PV', 'EV', 'HP', 'HP WH']
        if cdf:
            cdf = px.ecdf(data_frame=plot_df, x=plot_cols)
            cdf.update_layout(title='Additional capacity requirements by tech',
                              xaxis_title="Required Capacity [amps]",
                              yaxis_title="Percentile of Tech",
                              legend_title="Technologies",)
            cdf.show()
        else:
            histo = go.Figure()
            pv_cap = dataset['pv_req_capacity_amp']
            ev_cap = dataset['ev_req_capacity_amp']
            hp_cap = dataset['hp_req_capacity_amp']
            hpwh_cap = dataset['hpwh_req_capacity_amp']

            histo.add_trace(go.Histogram(x=pv_cap, name="PV",
                                         histnorm='percent', nbinsx=100))
            histo.add_trace(go.Histogram(x=ev_cap, name="EV",
                                         histnorm='percent', nbinsx=100))
            histo.add_trace(go.Histogram(x=hp_cap, name="HP",
                                         histnorm='percent', nbinsx=100))
            histo.add_trace(go.Histogram(x=hpwh_cap, name="HP WH",
                                         histnorm='percent', nbinsx=100))

            histo.update_layout(title='Additional capacity requirements by tech',
                                xaxis_title="Required Capacity [amps]",
                                yaxis_title="Percentile of Tech",
                                legend_title="Technologies",
                                barmode='overlay')
            histo.update_traces(opacity=0.75)
            histo.show()

    def plot_peak_amp_distribution(self, residential=1):
        dataset = self._filter_dataset(residential=residential)
        histo = px.histogram(dataset, x='peak_amp', title='Estimated Peak Amp Distribution')
        histo.update_layout(xaxis_title="Peak Capacity [amps]",
                            yaxis_title="Stock Count")
        histo.show()

    def plot_spare_capacity(self, residential=1):
        dataset = self._filter_dataset(residential=residential)
        histo = px.histogram(dataset, x='spare_capacity', title='Estimated Spare Capacity')
        histo.update_layout(xaxis_title="Spare Capacity [amps]",
                            yaxis_title="Stock Count")
        histo.show()

    def plot_panel_capacity(self, residential=1, log_y=True):
        dataset = self._filter_dataset(residential=residential)
        dataset = dataset[['installed_capacity', 'peak_amp']].sort_values(by=['installed_capacity', 'peak_amp'],
                                                                          ascending=[True, False])
        capacity = go.Figure()
        installed_cap = dataset['installed_capacity']
        peak_load = dataset['peak_amp']
        x = list(range(len(installed_cap)))

        capacity.add_trace(go.Scatter(x=x, y=installed_cap, name="Panel Size",
                                      mode='lines', fill='tonexty'))
        capacity.add_trace(go.Scatter(x=x, y=peak_load, name="Peak Load",
                                      mode='lines', fill='tozeroy'))
        if log_y:
            capacity.update_yaxes(type="log")

        capacity.update_layout(title='Panel Utilization and Capacity',
                               xaxis_title="Building",
                               yaxis_title="Amp")
        capacity.show()


if __name__ == '__main__':
    bau = BicepResults(scenario='bau')
    high = BicepResults(scenario='high')

    print(f'total cost for bau: ${bau.total_cost:,.0f}')
    print(f'total cost for high: ${high.total_cost:,.0f}')

    print(f'total residential cost for bau: ${bau.total_residential_costs:,.0f}')
    print(f'total residential cost for high: ${high.total_residential_costs:,.0f}')

    print(f'total commercial cost for bau: ${bau.total_commercial_costs:,.0f}')
    print(f'total commercial cost for high: ${high.total_commercial_costs:,.0f}')

