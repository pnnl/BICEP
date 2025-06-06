"""
This module defines the access to the bicep database.

SQLAlchemy is the ORM used to interact with the database.

User credentials are stored in ./utils/sensitive_config.py. 
"""

import datetime
import pandas as pd


from loguru import logger

import sqlalchemy
from sqlalchemy.pool import NullPool

from sqlalchemy.types import Integer, String, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy.dialects.mssql import DATETIME2

from utils.sensitive_config import sql_server_admin, sql_server_pass

ENABLE_TIMING = False
LOG_LEVEL = 'INFO'

dialect_driver = 'mssql+pymssql'
user_creds = f'{sql_server_admin}:{sql_server_pass}'
host_port = 'bicep-sql-server.database.windows.net:1433'
DATABASES = ['x-stock', ]


def create_engine(database):
    database_url = f'{dialect_driver}://{user_creds}@{host_port}/{database}'
    return sqlalchemy.create_engine(database_url)


def validate_database(database):
    if database not in DATABASES:
        raise KeyError(f'{database} not in {DATABASES}')


engines = {database: create_engine(database) for database in DATABASES}


# Base class for ORM x-stock tables
class Base(DeclarativeBase):
    pass


class PeakLoad(Base):
    __tablename__ = 'peak-load'

    building_id: Mapped[int] = mapped_column(Integer,
                                             primary_key=True)
    max_elec_consumption_kwh: Mapped[float]
    timestamp: Mapped[datetime.datetime] = mapped_column(DATETIME2())
    upgrade: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    state: Mapped[str]
    file_path: Mapped[str]
    release: Mapped[str]
    residential: Mapped[int] = mapped_column(Integer, primary_key=True)

    def __repr__(self):
        return f"PeakLoad(building={self.building_id!r})"


class LoadDifference(Base):
    __tablename__ = 'load-diff'

    building_id: Mapped[int] = mapped_column(Integer,
                                             primary_key=True)
    peak_diff_kwh: Mapped[float]
    upgrade: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    state: Mapped[str]
    release: Mapped[str]
    residential: Mapped[int] = mapped_column(Integer, primary_key=True)

    def __repr__(self):
        return f"PeakLoad(building={self.building_id!r})"


class StockMeta(Base):
    __tablename__ = 'stock-meta'

    building_id: Mapped[int] = mapped_column(Integer,
                                             primary_key=True)
    metadata_index: Mapped[int]
    weight: Mapped[float]
    residential: Mapped[int] = mapped_column(Integer, primary_key=True)

    heating_fuel: Mapped[str]
    hvac_cool_type: Mapped[str]
    hvac_heat_type: Mapped[str]
    water_heating_fuel: Mapped[str]
    water_heating_type: Mapped[str]

    building_type: Mapped[str]
    vintage: Mapped[str]
    year_built: Mapped[int] = mapped_column(Integer, nullable=True)
    sqft: Mapped[float]

    income: Mapped[str] = mapped_column(String, nullable=True)
    total_units: Mapped[int] = mapped_column(Integer, nullable=True)
    census_division: Mapped[str]
    census_region: Mapped[str]
    iso_rto_region: Mapped[str]
    nhgis_county: Mapped[str]
    nhgis_puma: Mapped[str]
    nhgis_tract: Mapped[str] = mapped_column(String, nullable=True)
    reeds_balancing_area: Mapped[int]
    state: Mapped[str]
    ashrae_iecc_climate_zone: Mapped[str]


class Technologies(Base):
    __tablename__ = 'technologies'

    tech_id: Mapped[int] = mapped_column(Integer, primary_key=True,
                                         autoincrement=True)
    tech_name: Mapped[str]
    sector: Mapped[str]  # buildings, pv, ev
    end_use: Mapped[str]  # water heating, hp
    required_capacity: Mapped[float] = mapped_column(Float, nullable=True)


class TechMapping(Base):
    __tablename__ = 'scout-xstock-tech-mapping'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tech_id: Mapped[int] = mapped_column(sqlalchemy.ForeignKey("technologies.tech_id"),
                                         nullable=True)
    scout_tech: Mapped[str] = mapped_column(String, nullable=True)
    xstock_fuel: Mapped[str]
    xstock_type: Mapped[str]
    fuel_col: Mapped[str]
    type_col: Mapped[str]


class AdoptionForecasts(Base):
    __tablename__ = 'adoption-forecasts'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tech_id: Mapped[int] = mapped_column(sqlalchemy.ForeignKey("technologies.tech_id"))
    tech_name: Mapped[str]
    sector: Mapped[str] = mapped_column(String, nullable=True)
    year: Mapped[int]
    scenario: Mapped[str]
    state: Mapped[str]
    stock_projection: Mapped[float]
    projection_units: Mapped[str]


class Upgrades(Base):
    __tablename__ = 'upgrades'

    upgrade_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    upgrade_name: Mapped[str]
    sector: Mapped[str]  # commercial/residential

    cost_max: Mapped[float]
    cost_min: Mapped[float]
    cost_avg: Mapped[float]


def create_lookup_tables(database='x-stock'):
    Base.metadata.create_all(engines[database], checkfirst=True)
    logger.info('Created lookup tables')


def query_to_df(query, database='x-stock', params=None):
    """Run a raw sql query and return the result as a dataframe"""
    validate_database(database)
    try:
        sql, params = query.sql()
    except AttributeError:
        sql = query
        params = params
    try:
        data = pd.read_sql_query(sql=sql, con=engines[database], params=params)
        return data
    except sqlalchemy.exc.OperationalError:
        import time
        attempts = 10
        for attempt in range(attempts):
            logger.error('Unable to reach the DB. Attempting db connection again. '
                         f'Attempt: {attempt} of {attempts}')
            time.sleep(6)
            try:
                data = pd.read_sql_query(sql=sql,
                                         con=engines[database],
                                         params=params)
                return data
            except sqlalchemy.exc.OperationalError:
                continue

        raise ConnectionError("Connection to the database cannot be established. "
                              "Please try refreshing the page.")


def get_state_cost_factors():
    """
    Retrieve state location cost factors from the database.
    """
    query = """
    SELECT State, Factor 
    FROM dbo.state_cost_factors
    """
    return query_to_df(query)


if __name__ == '__main__':
    create_lookup_tables()
