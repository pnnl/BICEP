"""
Estimate the technology adoptions and match to specific buildings

Technology adoption forecasts are provided by external models. Building technologies
(e.g., heat pumps, heat pump water heaters) are sourced from ComStock/Restock/Scout;
electric vehicle estimates come from TEMPO; and pv capacity estimates are given
by dGen/ReEDS.
"""

from loguru import logger

from sqlalchemy import select

from utils.db_models import AdoptionForecasts, Technologies, TechMapping,  query_to_df
from utils.sampling import sample_xstock
from bicep.capacity import CapacityEstimate


class TechnologyAdoption(CapacityEstimate):

    def __init__(self, scenario, base_year=2020, end_year=2050, epsilon=0.0001,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):
        super().__init__(residential_voltage, commercial_voltage, medium_voltage,
                         max_light_comm_amp, ev_charger_amp, panel_safety_factor)
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
                (projection['scenario'] == 'bau')), 'stock_projection'].iloc[0]

        end_year_projection = projection.loc[(
                (projection['year'] == self.end_year) &
                (projection['scenario'] == self.scenario)), 'stock_projection'].iloc[0]

        tech_growth = end_year_projection - base_year_projection

        if return_difference:
            return tech_growth
        else:
            return base_year_projection, end_year_projection

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
        base_year_stock, end_year_stock = self._get_tech_projections(tech=tech, return_difference=False, sector=sector)
        if (base_year_stock is None) or (base_year_stock == 0):  # No tech in sector
            return None
        percent_converted = min(1 - (end_year_stock/base_year_stock), 1)  # rounding can result in percent > 1
        return max(percent_converted, 0)  # percent_convert < 0 implies tech / growth

    def _iterative_adoption(self, tech, tech_project_col):

        tech_adopted_col = f'{tech}_adopted'

        tech_growth = self._get_tech_projections(tech=tech)

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
    tec = TechnologyAdoption(scenario='high')
    tec.calculate_adoptions()
