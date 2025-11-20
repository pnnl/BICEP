"""
Analysis of the BICEP model run results.
"""

import plotly.express as px
import plotly.graph_objs as go
from loguru import logger

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
                 panel_safety_factor=1.25, target_states='all', mode='local', 
                 save_results=False):
        
        self.mode = mode
        self.scenario = scenario
        
        super().__init__(aggregation_level=aggregation_level, annualized_costs=annualized,
                         upgrade_lifespan=upgrade_lifespan,
                         nominal_inflation_rate=nominal_inflation_rate, discount_rate=discount_rate,
                         cost_distribution=cost_distribution,
                         scenario=scenario, base_year=base_year, end_year=end_year, epsilon=epsilon,
                         residential_voltage=residential_voltage,
                         commercial_voltage=commercial_voltage,
                         medium_voltage=medium_voltage, max_light_comm_amp=max_light_comm_amp,
                         ev_charger_amp=ev_charger_amp,
                         panel_safety_factor=panel_safety_factor, target_states=target_states, mode=mode)

        self.calculate_costs()
        
        if save_results:
            self._save_results()
        
        self._capacity_requirement_cols = ['ev_req_capacity_amp', 'pv_req_capacity_amp',
                                           'hp_req_capacity_amp', 'hpwh_req_capacity_amp']

    def _save_results(self):
        """Save results to CSV with state abbreviation."""
        from utils.config import PARSED_INPUTS_PATH
        import pandas as pd
        
        PARSED_INPUTS_PATH.mkdir(parents=True, exist_ok=True)
        
        # Determine state identifier
        if isinstance(self.target_states, list):
            if len(self.target_states) == 1:
                state = self.target_states[0]
            else:
                state = "ALL"
        elif isinstance(self.target_states, str):
            state = self.target_states
        else:
            state = self.buildings['state'].iloc[0]
        
        # Save individual state or giant file
        if state == "ALL":
            # Giant file with all results
            filename = f"bicep_results_{self.scenario}_ALL_STATES.csv"
            output_path = PARSED_INPUTS_PATH / filename
            self.buildings.to_csv(output_path, index=False)
            logger.info(f"Saved {len(self.buildings):,} records from all states to {output_path}")
            
            # Summary file with costs by state and building type
            summary = self._create_cost_summary()
            summary_filename = f"bicep_cost_summary_{self.scenario}.csv"
            summary_path = PARSED_INPUTS_PATH / summary_filename
            summary.to_csv(summary_path, index=False)
            logger.info(f"Saved cost summary to {summary_path}")
        else:
            # Individual state file
            filename = f"bicep_results_{self.scenario}_{state}.csv"
            output_path = PARSED_INPUTS_PATH / filename
            self.buildings.to_csv(output_path, index=False)
            logger.info(f"Saved {len(self.buildings):,} records to {output_path}")
    
    def _create_cost_summary(self):
        """Create cost summary by state and building type."""
        import pandas as pd
        
        # Group by state and residential/commercial
        summary_data = []
        
        for state in sorted(self.buildings['state'].unique()):
            state_data = self.buildings[self.buildings['state'] == state]
            
            # Residential costs
            residential = state_data[state_data['residential'] == 1]
            res_total_cost = residential['weighted_cost'].sum()
            res_upgrade_cost = residential['upgrade_costs'].sum()
            res_buildings = len(residential)
            res_upgrades = residential['upgrade_required'].sum()
            
            # Commercial costs  
            commercial = state_data[state_data['residential'] == 0]
            com_total_cost = commercial['weighted_cost'].sum()
            com_upgrade_cost = commercial['upgrade_costs'].sum()
            com_buildings = len(commercial)
            com_upgrades = commercial['upgrade_required'].sum()
            
            # Add rows for this state
            summary_data.extend([
                {
                    'state': state,
                    'building_type': 'residential',
                    'total_buildings': res_buildings,
                    'buildings_needing_upgrades': res_upgrades,
                    'upgrade_rate_percent': (res_upgrades / res_buildings * 100) if res_buildings > 0 else 0,
                    'total_upgrade_costs': res_upgrade_cost,
                    'total_weighted_costs': res_total_cost
                },
                {
                    'state': state,
                    'building_type': 'commercial',
                    'total_buildings': com_buildings,
                    'buildings_needing_upgrades': com_upgrades,
                    'upgrade_rate_percent': (com_upgrades / com_buildings * 100) if com_buildings > 0 else 0,
                    'total_upgrade_costs': com_upgrade_cost,
                    'total_weighted_costs': com_total_cost
                }
            ])
        
        return pd.DataFrame(summary_data)

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


class BicepAllStates:
    """
    BICEP Individual State Analysis 
    Implements state-by-state processing to avoid national competition effects.
    Uses exact same variable names and structure as all_states.py for easy identification.
    """
    
    def __init__(self, scenario='bau', mode='local', save_results=True):
        self.scenario = scenario
        self.mode = mode
        self.save_results = save_results
        
        # list of all unique states
        self.all_states = [
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
            'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
            'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
            'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
            'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
        ]
        
        # Run the analysis
        self._run_individual_state_analysis()
    
    def _run_individual_state_analysis(self):
        """Run individual state analysis with state-by-state processing"""
        import pandas as pd
        
        logger.info("=== BICEP Individual State Analysis ===")
        logger.info("Running each state separately to avoid national competition effects")
        
        # object to store results
        final_results = {}

        for state in self.all_states:
            logger.info(f"Processing {state}...")
            
            try:
                # calculate state results 
                state_results = BicepResults(scenario=self.scenario, mode=self.mode, target_states=[state])
                
                # store results in dictionary 
                final_results[state] = state_results
                
                logger.info(f"Completed {state}: ${state_results.total_cost:,.0f}")
                
            except Exception as e:
                logger.error(f"Failed to process {state}: {e}")
                continue

        logger.info("All individual state analyses complete. Combining results...")

        # after all states have completed, combine results 
        # list for aggregate cost data results
        aggregate_data = []
        # list for buildings df data
        buildings_list = []
        # list for meta df data
        meta_list = []

        for state in self.all_states:
            if state in final_results:
                # combined aggregate data 
                aggregate_data.append({
                    'state': state,
                    'total_cost': final_results[state].total_cost,
                    'total_residential_costs': final_results[state].total_residential_costs,
                    'total_commercial_costs': final_results[state].total_commercial_costs
                })

                # collect DataFrames for concatenation 
                buildings_list.append(final_results[state].buildings)
                meta_list.append(final_results[state].building_meta)

        # Create final DataFrames
        self.all_states_results = pd.DataFrame(aggregate_data)
        
        # Filter out empty DataFrames to avoid FutureWarning
        non_empty_buildings = [df for df in buildings_list if not df.empty]
        non_empty_meta = [df for df in meta_list if not df.empty]
        
        self.all_states_buildings = pd.concat(non_empty_buildings, ignore_index=True) if non_empty_buildings else pd.DataFrame()
        self.all_states_meta = pd.concat(non_empty_meta, ignore_index=True) if non_empty_meta else pd.DataFrame()
        
        # Calculate and display national totals
        self.total_cost = self.all_states_results['total_cost'].sum()
        self.total_residential_costs = self.all_states_results['total_residential_costs'].sum()
        self.total_commercial_costs = self.all_states_results['total_commercial_costs'].sum()
        
        logger.info(f"=== FINAL NATIONAL TOTALS (Individual State Method) ===")
        logger.info(f"Total Cost: ${self.total_cost:,.0f}")
        logger.info(f"Residential Cost: ${self.total_residential_costs:,.0f}")
        logger.info(f"Commercial Cost: ${self.total_commercial_costs:,.0f}")
        
        # Save results if requested
        if self.save_results:
            self._save_individual_state_results()
        
        logger.info("=== Individual State Analysis Complete ===")
    
    def _save_individual_state_results(self):
        """Save results from individual state analysis with standard naming convention"""
        from utils.config import PARSED_INPUTS_PATH
        
        PARSED_INPUTS_PATH.mkdir(parents=True, exist_ok=True)
        
        # Save aggregate cost summary results
        self.all_states_results.to_csv(f"data/parsed_inputs/bicep_cost_summary_{self.scenario}.csv", index=False)
        
        # Save detailed buildings results  
        self.all_states_buildings.to_csv(f"data/parsed_inputs/bicep_results_{self.scenario}_all_states.csv", index=False)
        
        # Save meta data
        self.all_states_meta.to_csv(f"data/parsed_inputs/bicep_meta_{self.scenario}_all_states.csv", index=False)
        
        logger.info(f"Saved results to data/parsed_inputs/")


if __name__ == '__main__':
    print("=== BICEP Analysis ===")
    
    # Run individual state analysis to avoid national competition effects
    bau_results = BicepAllStates(scenario='bau', mode='local', save_results=True)
    high_results = BicepAllStates(scenario='high', mode='local', save_results=True)
    
    print(f'BAU scenario - total cost: ${bau_results.total_cost:,.0f}')
    print(f'HIGH scenario - total cost: ${high_results.total_cost:,.0f}')
    print(f'BAU scenario - residential cost: ${bau_results.total_residential_costs:,.0f}')
    print(f'HIGH scenario - residential cost: ${high_results.total_residential_costs:,.0f}')
    print(f'BAU scenario - commercial cost: ${bau_results.total_commercial_costs:,.0f}')
    print(f'HIGH scenario - commercial cost: ${high_results.total_commercial_costs:,.0f}')
    
    print(f'\nTotal buildings analyzed: {len(bau_results.all_states_buildings):,}')
    print(f'States included: {sorted(bau_results.all_states_buildings["state"].unique())}')
    
    