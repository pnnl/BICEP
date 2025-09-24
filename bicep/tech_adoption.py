"""
Estimate the technology adoptions and match to specific buildings

Technology adoption forecasts are provided by external models. Building technologies
(e.g., heat pumps, heat pump water heaters) are sourced from ComStock/Restock/Scout;
electric vehicle estimates come from TEMPO; and pv capacity estimates are given
by dGen/ReEDS.
"""

from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")

from sqlalchemy import select

import pandas as pd
import numpy as np
from utils.local_db_mirror import save_db_query_results

from utils.db_models import AdoptionForecasts, Technologies, TechMapping,  query_to_df
from utils.db_models import get_new_pv_data
from utils.adoption_forecast_parsing import get_scout_forecast, get_ev_forecast, get_pv_forecast
from utils.local_db_mirror import save_db_query_results
from utils.sampling import sample_xstock
from bicep.capacity import CapacityEstimate


class TechnologyAdoption(CapacityEstimate):

    def __init__(self, scenario, base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25, target_states=None, mode='local'):
        # Validate required parameters
        if target_states is None:
            raise ValueError("target_states parameter is required. Please specify the states to analyze, e.g., target_states=['CA']")
        
        # Store mode for tech projections
        self.mode = mode
        
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
        
        # Cache for combined tech projections to avoid reloading data for each technology
        self._combined_data_cache = {}
        
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
            logger.debug(f"Filtered data for states: {self.target_states}. "
                       f"Found {len(filtered_data)} records matching target states.")
        
        return filtered_data

    def calculate_adoptions(self):
        """Calculate adoption rates for all technologies using configured mode."""
        if self.mode == 'local':
            logger.info('Using local file processing for technology adoptions')
        else:
            logger.info('Using database processing for technology adoptions')
        
        logger.info('Calculating adoption rate for EV')
        self._iterative_adoption(tech='ev', tech_project_col='represented_vehicles')
        logger.info('Calculating adoption rate for PV')
        self._iterative_adoption(tech='pv', tech_project_col='pv_size_kw')
        logger.info('Calculating adoption rate for HPs')
        self._building_adoption(end_use='heating')
        logger.info('Calculating adoption rate for HPWHs')
        self._building_adoption(end_use='water heating')

    def _get_tech_projections(self, tech, return_difference=True, sector=None):
        """Get technology projections using configured mode (local or database)."""
        try:
            assert tech in self.all_techs['tech_name'].to_list()
        except AssertionError:
            raise KeyError(f"Technology must be in {self.all_techs['tech_name'].to_list()}")

        if self.mode == 'local':
            # Local mode: use combined data from files (with caching)
            cache_key = f"{self.scenario}_{self.mode}"
            if cache_key not in self._combined_data_cache:
                logger.info(f"Loading combined tech projections for {self.scenario} scenario (will be cached)")
                self._combined_data_cache[cache_key] = self.get_combined_tech_projections(scenario=self.scenario, mode=self.mode)
            else:
                logger.debug(f"Using cached combined tech projections for {self.scenario} scenario")
                
            combined_data = self._combined_data_cache[cache_key]
            
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
        else:
            # Database mode: use original database queries
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
                logger.warning(f"No data found for technology {tech} in database")
                if return_difference:
                    return None
                else:
                    return None, None

            # Filter for base year data - ALWAYS use 'bau' scenario for base year like original BICEP v1
            base_year_data = projection.loc[
                (projection['year'] == self.base_year) & 
                (projection['scenario'] == 'bau')  # Always use 'bau' for base year
            ]
            
            if base_year_data.empty:
                logger.warning(f"No bau scenario data found for {tech} in {self.base_year}")
                if return_difference:
                    return None
                else:
                    return None, None
                
            base_year_projection = base_year_data['stock_projection'].iloc[0]

            # Filter for end year data
            end_year_data = projection.loc[
                (projection['year'] == self.end_year) & 
                (projection['scenario'] == self.scenario)
            ]
            
            if end_year_data.empty:
                logger.warning(f"No {self.scenario} scenario data found for {tech} in {self.end_year}")
                if return_difference:
                    return None
                else:
                    return None, None
                
            end_year_projection = end_year_data['stock_projection'].iloc[0]

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

    def get_combined_tech_projections(self, scenario='bau', mode='local'):
        """
        Get combined technology projections from all sources.
        
        Args:
            scenario (str): Scenario to process ('bau' or 'high')
            mode (str): 'local' for file processing or 'database' for Azure operations
            
        Returns:
            pd.DataFrame: Combined dataset with all technologies
        """
        if mode == 'local':
            logger.info(f"Processing {scenario} scenario data locally...")
            
            # Process Scout data (tech_id 1-9)
            logger.info("Processing Scout data...")
            scout_data = get_scout_forecast(scenario=scenario, mode='local')
            
            # Process EV data (tech_id 10)  
            logger.info("Processing EV data...")
            ev_data = get_ev_forecast(scenario=scenario, mode='local')
            
            # Process PV data (tech_id 11) - always use 'mid' scenario for PV
            logger.info("Processing PV data...")
            pv_data = get_pv_forecast(scenario='mid', mode='local')
            # Update PV scenario to match requested scenario
            pv_data['scenario'] = scenario
            
            # Combine all datasets
            combined_data = pd.concat([scout_data, ev_data, pv_data], ignore_index=True)
            
            # Assign unique IDs
            combined_data['id'] = range(len(combined_data))
            
            logger.info(f"Processing complete. Total records: {len(combined_data)}")
            logger.debug(f"Technologies processed: {sorted(combined_data['tech_id'].unique())}")
            logger.debug(f"States covered: {sorted(combined_data['state'].unique())}")
            logger.debug(f"Year range: {combined_data['year'].min()} - {combined_data['year'].max()}")
            
            # Save local mode results to parsed_inputs for comparison
            save_db_query_results(f'combined_tech_projections_local_{scenario}', combined_data)
            
            # Save individual adoption forecasts in database table format
            save_db_query_results(f'adoption_forecasts_{scenario}', combined_data)
            
            # Save all equivalent database tables for local mode
            from utils.local_db_mirror import save_database_tables
            save_database_tables(mode='local', scenario=scenario)
            
            return combined_data
        elif mode == 'database':
            # Use database-based approach and save results for local mode
            logger.info("Using database-based technology projections")
            
            try:
                # Process Scout data using database
                logger.info("Fetching Scout data from database...")
                scout_data = get_scout_forecast(scenario=scenario, mode='database')
                logger.info(f"Successfully retrieved {len(scout_data)} Scout records from database")
                save_db_query_results(f'scout_data_{scenario}', scout_data)
                
                # Use database for both PV and EV data
                logger.info("Processing PV data from database...")
                pv_data = get_pv_forecast(scenario='mid', mode='database')
                pv_data['scenario'] = scenario
                
                logger.info("Processing EV data from database...")
                ev_data = get_ev_forecast(scenario=scenario, mode='database')
                
                # Save EV and PV data as well
                save_db_query_results(f'ev_data_{scenario}', ev_data)
                save_db_query_results(f'pv_data_{scenario}', pv_data)
                
                combined_data = pd.concat([scout_data, ev_data, pv_data], ignore_index=True)
                combined_data['id'] = range(len(combined_data))
                
                # Save final combined dataset
                save_db_query_results(f'combined_tech_projections_{scenario}', combined_data)
                
                # Save all database tables for comparison
                from utils.local_db_mirror import save_database_tables
                save_database_tables(mode='database', scenario=scenario)
                
                logger.info(f"Database mode results saved to parsed_inputs for scenario: {scenario}")
                return combined_data
                
            except Exception as e:
                logger.error(f"Database mode failed with error: {str(e)}")
                logger.error(f"Error type: {type(e).__name__}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                logger.info("Falling back to local mode...")
                return self.get_combined_tech_projections(scenario=scenario, mode='local')
        else:
            raise ValueError("Mode must be 'local' or 'database'")

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
        """Calculate the percentage of stock converted based on the adoption forecasts"""
        tech = self.all_techs.loc[self.all_techs['tech_id'] == tech_id, 'tech_name'].iloc[0]
        base_year_stock, end_year_stock = self._get_tech_projections(tech=tech, return_difference=False, sector=sector)
        if (base_year_stock is None) or (base_year_stock == 0):  # No tech in sector
            return None
        percent_converted = min(1 - (end_year_stock/base_year_stock), 1)  # rounding can result in percent > 1
        return max(percent_converted, 0)  # percent_convert < 0 implies tech / growth

    def _iterative_adoption(self, tech, tech_project_col):

        tech_adopted_col = f'{tech}_adopted'

        tech_growth = self._get_tech_projections(tech=tech)
        
        # Handle case where no data is available
        if tech_growth is None:
            logger.warning(f"No growth data available for {tech}, skipping adoption calculation")
            # Set default column with zero adoption
            self.buildings[tech_adopted_col] = 0
            return

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


if __name__ == '__main__':
    # used for testing
    tec = TechnologyAdoption(scenario='high', target_states=['CA'], mode='local')
    tec.calculate_adoptions()
