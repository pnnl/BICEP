"""
Estimate the technology adoptions and match to specific buildings

Technology adoption forecasts are provided by external models. Building technologies
(e.g., heat pumps, heat pump water heaters) are sourced from ComStock/Restock/Scout;
electric vehicle estimates come from TEMPO; and pv capacity estimates are given
by dGen/ReEDS.
"""

from loguru import logger

from sqlalchemy import select

from utils.db_models import AdoptionForecasts, Technologies, query_to_df
from utils.sampling import sample_xstock
from bicep.capacity import CapacityEstimate


class TechnologyAdoption(CapacityEstimate):

    def __init__(self, scenario, base_year=2020, end_year=2050, epsilon=0.01,
                 residential_voltage=240, commercial_voltage=480,
                 medium_voltage=12470, max_light_comm_amp=1000, ev_charger_amp=50,
                 panel_safety_factor=1.25):
        super().__init__(residential_voltage, commercial_voltage, medium_voltage,
                         max_light_comm_amp, ev_charger_amp, panel_safety_factor)
        self.calculate_capacity()

        self.scenario = scenario
        self.base_year = base_year
        self.end_year = end_year
        self.epsilon = epsilon

        self.all_techs = query_to_df(select(Technologies))

    def get_tech_projections(self, tech):
        try:
            assert tech in self.all_techs['tech_name']
        except AssertionError:
            raise KeyError(f"Technology must be in {self.all_techs['tech_name'].to_list()}")

        projection = query_to_df(select(AdoptionForecasts).where(AdoptionForecasts.tech_name == tech))
        base_year_projection = projection.loc[(
                (projection['year'] == self.base_year) &
                (projection['scenario'] == 'bau')), 'stock_projection'].iloc[0]

        end_year_projection = projection.loc[(
                (projection['year'] == self.end_year) &
                (projection['scenario'] == self.scenario)), 'stock_projection'].iloc[0]

        tech_growth = end_year_projection - base_year_projection

        return tech_growth

