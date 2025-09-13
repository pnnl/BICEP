"""
Estimate the technology adoptions and match to specific buildings

Technology adoption forecasts are provided by external models. Building technologies
(e.g., heat pumps, heat pump water heaters) are sourced from ComStock/Restock/Scout;
electric vehicle estimates come from TEMPO; and pv capacity estimates are given
by dGen/ReEDS.
"""

from loguru import logger

from sqlalchemy import select

import pandas as pd
import numpy as np

from utils.db_models import AdoptionForecasts, Technologies, TechMapping,  query_to_df
from utils.db_models import get_new_pv_data
from utils.adoption_forecast_parsing import scout_forecast_local, ev_forecast_local, pv_forecast_local
from utils.sampling import sample_xstock
from bicep.capacity import CapacityEstimate


class TechnologyAdoption(CapacityEstimate):

    def __init__(self, scenario, base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25, target_states=None):
        # Validate required parameters
        if target_states is None:
            raise ValueError("target_states parameter is required. Please specify the states to analyze, e.g., target_states=['CA']")
        
        super().__init__(residential_voltage, commercial_voltage, medium_voltage,
                         max_light_comm_amp, ev_charger_amp, panel_safety_factor, target_states)
        self.calculate_capacity()

        self.scenario = scenario
        try:
            assert scenario in ('bau', 'high')
        except AssertionError:
            raise KeyError('Scenario must be in ["bau", "high"]')

        self.base_year = base_year
        self.end_year = end_year
        self.epsilon = epsilon

        self.all_techs = query_to_df(select(Technologies))
        self.tech_mapping = query_to_df(select(TechMapping))
        
        # Create state name mapping for technology forecast data
        self.state_name_mapping = self._create_state_name_mapping()

    def _create_state_name_mapping(self):
        """Create mapping from various state name formats to standard abbreviations."""
        # Common state name mappings that appear in forecast data
        state_mappings = {
            'CA': ['CA', 'California'],
            'TX': ['TX', 'Texas'], 
            'FL': ['FL', 'Florida'],
            'NY': ['NY', 'New York'],
            'PA': ['PA', 'Pennsylvania'],
            'IL': ['IL', 'Illinois'],
            'OH': ['OH', 'Ohio'],
            'GA': ['GA', 'Georgia'],
            'NC': ['NC', 'North Carolina'],
            'MI': ['MI', 'Michigan'],
            'WA': ['WA', 'Washington'],
            'AZ': ['AZ', 'Arizona'],
            'MA': ['MA', 'Massachusetts'],
            'TN': ['TN', 'Tennessee'],
            'IN': ['IN', 'Indiana'],
            'MO': ['MO', 'Missouri'],
            'MD': ['MD', 'Maryland'],
            'WI': ['WI', 'Wisconsin'],
            'CO': ['CO', 'Colorado'],
            'MN': ['MN', 'Minnesota'],
            'SC': ['SC', 'South Carolina'],
            'AL': ['AL', 'Alabama'],
            'LA': ['LA', 'Louisiana'],
            'KY': ['KY', 'Kentucky'],
            'OR': ['OR', 'Oregon'],
            'OK': ['OK', 'Oklahoma'],
            'CT': ['CT', 'Connecticut'],
            'IA': ['IA', 'Iowa'],
            'MS': ['MS', 'Mississippi'],
            'AR': ['AR', 'Arkansas'],
            'UT': ['UT', 'Utah'],
            'KS': ['KS', 'Kansas'],
            'NV': ['NV', 'Nevada'],
            'NM': ['NM', 'New Mexico'],
            'NE': ['NE', 'Nebraska'],
            'WV': ['WV', 'West Virginia'],
            'ID': ['ID', 'Idaho'],
            'HI': ['HI', 'Hawaii'],
            'NH': ['NH', 'New Hampshire'],
            'ME': ['ME', 'Maine'],
            'MT': ['MT', 'Montana'],
            'RI': ['RI', 'Rhode Island'],
            'DE': ['DE', 'Delaware'],
            'SD': ['SD', 'South Dakota'],
            'ND': ['ND', 'North Dakota'],
            'AK': ['AK', 'Alaska'],
            'VT': ['VT', 'Vermont'],
            'WY': ['WY', 'Wyoming'],
            'DC': ['DC', 'District of Columbia']
        }
        return state_mappings
    
    def _filter_for_target_states(self, data):
        """Filter dataset for target states, handling multiple state name formats."""
        if data.empty:
            return data
            
        # Create a list of all possible state names for our target states
        target_state_names = []
        for state_abbrev in self.target_states:
            if state_abbrev in self.state_name_mapping:
                target_state_names.extend(self.state_name_mapping[state_abbrev])
            else:
                # If not in mapping, just use the abbreviation itself
                target_state_names.append(state_abbrev)
        
        # Filter data for any of the target state names
        filtered_data = data[data['state'].isin(target_state_names)].copy()
        
        if filtered_data.empty:
            logger.warning(f"No data found for target states {self.target_states}. "
                          f"Available states in data: {sorted(data['state'].unique())}")
        else:
            logger.info(f"Filtered data for states: {self.target_states}. "
                       f"Found {len(filtered_data)} records matching target states.")
        
        return filtered_data

    def calculate_adoptions(self):
        logger.info('Calculating adoption rate for EV')
        self._iterative_adoption(tech='ev', tech_project_col='represented_vehicles')
        logger.info('Calculating adoption rate for PV')
        self._iterative_adoption(tech='pv', tech_project_col='pv_size_kw')
        logger.info('Calculating adoption rate for HPs')
        self._building_adoption(end_use='heating')
        logger.info('Calculating adoption rate for HPWHs')
        self._building_adoption(end_use='water heating')

    def _get_tech_projections(self, tech, return_difference=True, sector=None):
        try:
            assert tech in self.all_techs['tech_name'].to_list()
        except AssertionError:
            raise KeyError(f"Technology must be in {self.all_techs['tech_name'].to_list()}")

        if sector is not None:
            projection = query_to_df(select(AdoptionForecasts).where(
                (AdoptionForecasts.tech_name == tech) &
                (AdoptionForecasts.sector == sector)
            ))
        else:
            projection = query_to_df(select(AdoptionForecasts).where(
                (AdoptionForecasts.tech_name == tech)
            ))
        if projection.empty:
            return None, None

        base_year_projection = projection.loc[(
                (projection['year'] == self.base_year) &
                (projection['scenario'] == self.scenario)), 'stock_projection'].iloc[0]

        end_year_projection = projection.loc[(
                (projection['year'] == self.end_year) &
                (projection['scenario'] == self.scenario)), 'stock_projection'].iloc[0]

        tech_growth = end_year_projection - base_year_projection

        if not projection.empty and projection['tech_name'].iloc[0] == 'pv':
            new_pv_df = self._get_new_pv_projections()
            if new_pv_df is not None and not new_pv_df.empty:
                #Concatenate with existing pv data once pv data processing step is triggered
                projection = pd.concat([projection, new_pv_df], ignore_index=True)

        if return_difference:
            return tech_growth
        else:
            return base_year_projection, end_year_projection

    def get_combined_tech_projections(self, scenario='bau', use_local=False):
        """
        Get combined technology projections from all sources.
        
        Args:
            scenario (str): Scenario to process ('bau' or 'high')
            use_local (bool): If True, use local file processing instead of database
            
        Returns:
            pd.DataFrame: Combined dataset with all technologies
        """
        if use_local:
            logger.info(f"Processing {scenario} scenario data locally...")
            
            # Process Scout data (tech_id 1-9)
            logger.info("Processing Scout data...")
            scout_data = scout_forecast_local(scenario=scenario)
            
            # Process EV data (tech_id 10)  
            logger.info("Processing EV data...")
            ev_data = ev_forecast_local(scenario=scenario)
            
            # Process PV data (tech_id 11) - always use 'mid' scenario for PV
            logger.info("Processing PV data...")
            pv_data = pv_forecast_local(scenario='mid')
            # Update PV scenario to match requested scenario
            pv_data['scenario'] = scenario
            
            # Combine all datasets
            combined_data = pd.concat([scout_data, ev_data, pv_data], ignore_index=True)
            
            # Assign unique IDs
            combined_data['id'] = range(len(combined_data))
            
            logger.info(f"Processing complete. Total records: {len(combined_data)}")
            logger.info(f"Technologies processed: {sorted(combined_data['tech_id'].unique())}")
            logger.info(f"States covered: {sorted(combined_data['state'].unique())}")
            logger.info(f"Year range: {combined_data['year'].min()} - {combined_data['year'].max()}")
            
            return combined_data
        else:
            # Use existing database-based approach
            logger.info("Using database-based technology projections")
            # This would be the existing logic for database queries
            # For now, return None to indicate database mode
            return None

    def _get_new_pv_projections(self):
        """Get additional PV projections from new data source."""
        try:
            pv_data, hierarchy_data = get_new_pv_data()
            processed_data = self.process_new_pv_data(pv_data, hierarchy_data)
            
            # Return all processed data (no filtering needed since we want all new PV data)
            return processed_data
        except Exception:
            return None

    def process_new_pv_data(self, pv_data, hierarchy_data):
        """Process new PV data to match the FORMAT of old PV data."""
        year_cols = [col for col in pv_data.columns 
                    if str(col).isdigit() and 2010 <= int(col) <= 2050]
        
        merged_data = pd.merge(pv_data, hierarchy_data, on='county_id', how='inner')
        
        long_data = merged_data.melt(
            id_vars=['county_id', 'state'],
            value_vars=year_cols,
            var_name='year',
            value_name='stock_projection'
        )
        
        long_data['year'] = long_data['year'].astype(int)
        
        # Aggregate by state and year - sum all county data within each state
        aggregated_data = long_data.groupby(['state', 'year'])['stock_projection'].sum().reset_index()
        
        result = pd.DataFrame({
            'id': range(1001, 1001 + len(aggregated_data)),
            'tech_id': 11,
            'tech_name': 'pv',
            'sector': '',
            'year': aggregated_data['year'],
            'scenario': 'mid',
            'state': aggregated_data['state'],
            'stock_projection': aggregated_data['stock_projection'],
            'projection_units': 'MW'
        })
        
        return result

    def _building_adoption(self, end_use):
        # get base and target year totals for each type
        # calculate percent conversion for tech
        # get stock models that have that tech
        # use conversion prob to estimate which ones have adopted tech
        # not sure what to do about the weights - maybe have to iteratively add like pv/ev
        if end_use == 'water heating':
            adoption_col = f'hpwh_adopted'
        elif end_use == 'heating':
            adoption_col = 'hp_adopted'
        else:
            raise KeyError(f"End uses must be either 'water heating' or 'heating'")

        self.buildings[adoption_col] = 0

        # get all techs in that end use (e.g., water heating => hpwh, gas wh, fuel wh, etc.)
        end_use_techs = self.all_techs[self.all_techs['end_use'] == end_use]
        end_use_tech_ids = end_use_techs['tech_id'].to_list()

        # get mapping from xstock fuel / end use type to Scout tech types
        end_use_tech_mapping = self.tech_mapping[self.tech_mapping['tech_id'].isin(end_use_tech_ids)]
        fuel_meta_col = end_use_tech_mapping['fuel_col'].iloc[0]
        type_meta_col = end_use_tech_mapping['type_col'].iloc[0]

        # loop through each tech in the end use and calculate percent converted
        for tech_id in end_use_tech_ids:
            tech_mapping = end_use_tech_mapping[end_use_tech_mapping['tech_id'] == tech_id]
            xstock_fuels = tech_mapping['xstock_fuel'].unique().tolist()
            xstock_types = tech_mapping['xstock_type'].unique().tolist()
            stock_with_tech = self.building_meta.loc[((self.building_meta[fuel_meta_col].isin(xstock_fuels)) &
                                                      (self.building_meta[type_meta_col].isin(xstock_types)))]
            res_stock = stock_with_tech[stock_with_tech['residential'] == 1]
            com_stock = stock_with_tech[stock_with_tech['residential'] == 0]

            residential_converted = self._building_tech_conversion(tech_id=tech_id, sector='residential')
            commercial_converted = self._building_tech_conversion(tech_id=tech_id, sector='commercial')

            if residential_converted is not None:
                res_stock_converted = res_stock['building_id'].sample(frac=residential_converted).to_list()
                self.buildings.loc[
                    ((self.buildings['residential'] == 1) & (self.buildings['building_id'].isin(res_stock_converted))),
                    adoption_col] = 1
            if commercial_converted is not None:
                com_stock_converted = com_stock['building_id'].sample(frac=commercial_converted).to_list()
                self.buildings.loc[
                    ((self.buildings['residential'] == 0) & (self.buildings['building_id'].isin(com_stock_converted))),
                    adoption_col] = 1

    def _building_tech_conversion(self, tech_id, sector):
        """Calculate the percentage of stock converted based on the Scout adoption forecasts"""
        tech = self.all_techs.loc[self.all_techs['tech_id'] == tech_id, 'tech_name'].iloc[0]
        base_year_stock, end_year_stock = self._get_tech_projections_local(tech=tech, return_difference=False, sector=sector)
        if (base_year_stock is None) or (base_year_stock == 0):  # No tech in sector
            return None
        percent_converted = min(1 - (end_year_stock/base_year_stock), 1)  # rounding can result in percent > 1
        return max(percent_converted, 0)  # percent_convert < 0 implies tech / growth
        percent_converted = min(1 - (end_year_stock/base_year_stock), 1)  # rounding can result in percent > 1
        return max(percent_converted, 0)  # percent_convert < 0 implies tech / growth

    def _iterative_adoption(self, tech, tech_project_col):

        tech_adopted_col = f'{tech}_adopted'

        tech_growth = self._get_tech_projections_local(tech=tech)

        if tech == 'pv':
            tech_growth = tech_growth * 1000  # MW to kW

        tech_estimate = 0
        eps = tech_growth * self.epsilon

        estimate_error = tech_growth - tech_estimate

        rand_buildings = self.buildings[['building_id', 'residential']].sample(frac=1)
        rand_buildings = list(rand_buildings.itertuples(index=False, name=None))

        self.buildings[tech_adopted_col] = 0
        self.buildings.set_index(['building_id', 'residential'], inplace=True)

        bldg = self.buildings  # less verbose

        while estimate_error >= eps:
            added_building = rand_buildings.pop()

            bldg.loc[added_building, tech_adopted_col] = 1

            bldg['temp_sum'] = bldg[tech_adopted_col] * bldg['weight'] * bldg[tech_project_col]

            tech_estimate = bldg['temp_sum'].sum()
            estimate_error = tech_growth - tech_estimate
            # logger.debug(f'num buildings {bldg[tech_adopted_col].sum()}', estimate_error)

        bldg.drop(columns=['temp_sum'], inplace=True)
        self.buildings = bldg.reset_index()

    # NEW LOCAL PROCESSING METHODS
    def calculate_adoptions_local(self):
        """Main BICEP pipeline using local file processing instead of database."""
        logger.info('Calculating adoption rate for EV using local data')
        self._iterative_adoption_local(tech='ev', tech_project_col='represented_vehicles')
        logger.info('Calculating adoption rate for PV using local data')
        self._iterative_adoption_local(tech='pv', tech_project_col='pv_size_kw')
        logger.info('Calculating adoption rate for HPs using local data')
        self._building_adoption_local(end_use='heating')
        logger.info('Calculating adoption rate for HPWHs using local data')
        self._building_adoption_local(end_use='water heating')

    def _iterative_adoption_local(self, tech, tech_project_col):
        """Iterative adoption using local file processing."""
        tech_adopted_col = f'{tech}_adopted'

        # Get tech growth from local processing instead of database
        tech_growth = self._get_tech_projections_local(tech=tech)

        if tech == 'pv':
            tech_growth = tech_growth * 1000  # MW to kW

        tech_estimate = 0
        eps = tech_growth * self.epsilon

        estimate_error = tech_growth - tech_estimate

        rand_buildings = self.buildings[['building_id', 'residential']].sample(frac=1)
        rand_buildings = list(rand_buildings.itertuples(index=False, name=None))

        self.buildings[tech_adopted_col] = 0
        self.buildings.set_index(['building_id', 'residential'], inplace=True)

        bldg = self.buildings  # less verbose

        while estimate_error >= eps:
            added_building = rand_buildings.pop()

            bldg.loc[added_building, tech_adopted_col] = 1

            bldg['temp_sum'] = bldg[tech_adopted_col] * bldg['weight'] * bldg[tech_project_col]

            tech_estimate = bldg['temp_sum'].sum()
            estimate_error = tech_growth - tech_estimate

        bldg.drop(columns=['temp_sum'], inplace=True)
        self.buildings = bldg.reset_index()

    def _building_adoption_local(self, end_use):
        """Building adoption using local file processing."""
        if end_use == 'water heating':
            adoption_col = f'hpwh_adopted'
        elif end_use == 'heating':
            adoption_col = 'hp_adopted'
        else:
            raise KeyError(f"End uses must be either 'water heating' or 'heating'")

        self.buildings[adoption_col] = 0

        # get all techs in that end use (e.g., water heating => hpwh, gas wh, fuel wh, etc.)
        end_use_techs = self.all_techs[self.all_techs['end_use'] == end_use]
        end_use_tech_ids = end_use_techs['tech_id'].to_list()

        # get mapping from xstock fuel / end use type to Scout tech types
        end_use_tech_mapping = self.tech_mapping[self.tech_mapping['tech_id'].isin(end_use_tech_ids)]
        fuel_meta_col = end_use_tech_mapping['fuel_col'].iloc[0]
        type_meta_col = end_use_tech_mapping['type_col'].iloc[0]

        # loop through each tech in the end use and calculate percent converted
        for tech_id in end_use_tech_ids:
            tech_mapping = end_use_tech_mapping[end_use_tech_mapping['tech_id'] == tech_id]
            xstock_fuels = tech_mapping['xstock_fuel'].unique().tolist()
            xstock_types = tech_mapping['xstock_type'].unique().tolist()
            stock_with_tech = self.building_meta.loc[((self.building_meta[fuel_meta_col].isin(xstock_fuels)) &
                                                      (self.building_meta[type_meta_col].isin(xstock_types)))]
            res_stock = stock_with_tech[stock_with_tech['residential'] == 1]
            com_stock = stock_with_tech[stock_with_tech['residential'] == 0]

            residential_converted = self._building_tech_conversion_local(tech_id=tech_id, sector='residential')
            commercial_converted = self._building_tech_conversion_local(tech_id=tech_id, sector='commercial')

            if residential_converted is not None:
                res_stock_converted = res_stock['building_id'].sample(frac=residential_converted).to_list()
                self.buildings.loc[
                    ((self.buildings['residential'] == 1) & (self.buildings['building_id'].isin(res_stock_converted))),
                    adoption_col] = 1
            if commercial_converted is not None:
                com_stock_converted = com_stock['building_id'].sample(frac=commercial_converted).to_list()
                self.buildings.loc[
                    ((self.buildings['residential'] == 0) & (self.buildings['building_id'].isin(com_stock_converted))),
                    adoption_col] = 1

    def _building_tech_conversion_local(self, tech_id, sector):
        """Calculate the percentage of stock converted using local file processing."""
        tech = self.all_techs.loc[self.all_techs['tech_id'] == tech_id, 'tech_name'].iloc[0]
        base_year_stock, end_year_stock = self._get_tech_projections_local(tech=tech, return_difference=False, sector=sector)
        if (base_year_stock is None) or (base_year_stock == 0):  # No tech in sector
            return None
        percent_converted = min(1 - (end_year_stock/base_year_stock), 1)  # rounding can result in percent > 1
        return max(percent_converted, 0)  # percent_convert < 0 implies tech / growth

    def _get_tech_projections_local(self, tech, return_difference=True, sector=None):
        """Get technology projections using local file processing instead of database."""
        # Get combined data from local processing
        combined_data = self.get_combined_tech_projections(scenario=self.scenario, use_local=True)
        
        # Filter for target states to match building stock scope
        target_data = self._filter_for_target_states(combined_data)
        
        if target_data.empty:
            logger.warning(f"No data found for target states {self.target_states} for technology {tech}")
            return None, None
        
        # Filter for specific technology
        if sector is not None:
            projection = target_data[
                (target_data['tech_name'] == tech) & 
                (target_data['sector'] == sector)
            ].copy()
        else:
            projection = target_data[target_data['tech_name'] == tech].copy()
            
        if projection.empty:
            logger.warning(f"No data found for technology {tech} in target states {self.target_states}")
            return None, None

        # Sum across target states for this tech/sector/year/scenario
        base_year_projection = projection[
            (projection['year'] == self.base_year) & 
            (projection['scenario'] == self.scenario)
        ]['stock_projection'].sum()

        end_year_projection = projection[
            (projection['year'] == self.end_year) & 
            (projection['scenario'] == self.scenario)
        ]['stock_projection'].sum()

        tech_growth = end_year_projection - base_year_projection

        # Create state list string for logging
        state_str = ', '.join(self.target_states)
        logger.info(f"{state_str} {tech} projections - Base year ({self.base_year}): {base_year_projection:,.0f}, "
                   f"End year ({self.end_year}): {end_year_projection:,.0f}, Growth: {tech_growth:,.0f}")

        if return_difference:
            return tech_growth
        else:
            return base_year_projection, end_year_projection


if __name__ == '__main__':
    # used for testing
    tec = TechnologyAdoption(scenario='high', target_states=['CA'])
    tec.calculate_adoptions()
