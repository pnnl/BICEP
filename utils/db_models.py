"""
This module defines the access to the bicep database.

SQLAlchemy is the ORM used to interact with the database.

User credentials are stored in ./utils/sensitive_config.py. 
"""

import datetime
import pandas as pd
from contextlib import contextmanager

from loguru import logger

import sqlalchemy
from sqlalchemy.pool import NullPool

from sqlalchemy.types import Integer, String, Float
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

from utils.sensitive_config import sql_server_admin, sql_server_pass
from utils.config import DATA_ROOT, ensure_data_assets

DATABASES = ['x-stock', ]


class DatabaseContext:
    """
    Manages database connections and engines for BICEP.
    Supports both local SQLite and remote MSSQL on PNNL Azure Cloud configurations.
    """

    def __init__(self, mode='local'):
        """
        Initialize database context.

        Args:
            mode: Either 'local' for SQLite or 'PNNL database' for MSSQL host on PNNL Azure cloud
        """
        self.mode = mode
        self._engines = {}

    def get_engine(self, database='x-stock'):
        """Get or create engine for the specified database."""
        if database not in self._engines:
            self._engines[database] = self._create_engine(database)
        return self._engines[database]

    def _create_engine(self, database):
        """Create database engine based on mode configuration."""
        if self.mode == 'local':
            ensure_data_assets()
            sqlite_file = DATA_ROOT / 'bicep.x-stock.db'
            if not sqlite_file.exists():
                raise FileNotFoundError(f"SQLite database not found: {sqlite_file}")
            database_url = f'sqlite:///{sqlite_file}'
            logger.info(f'Connecting to database: {database_url}')
            return sqlalchemy.create_engine(database_url)

        elif self.mode == 'PNNL database':
            dialect_driver = 'mssql+pymssql'
            user_creds = f'{sql_server_admin}:{sql_server_pass}'
            host_port = 'bicep-sql-server.database.windows.net:1433'
            database_url = f'{dialect_driver}://{user_creds}@{host_port}/{database}'
            logger.info(f'Connecting to database: {database_url}')
            return sqlalchemy.create_engine(database_url, poolclass=NullPool)

        else:
            raise ValueError(f'Invalid mode: {self.mode}. Must be "local" or "PNNL database"')

    @contextmanager
    def connection(self, database='x-stock'):
        """Context manager for database connections."""
        engine = self.get_engine(database)
        with engine.connect() as conn:
            yield conn

    def close_all(self):
        """Close all engine connections."""
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()


def validate_database(database):
    if database not in DATABASES:
        raise KeyError(f'{database} not in {DATABASES}')


# Base class for ORM x-stock tables
class Base(DeclarativeBase):
    pass


class PeakLoad(Base):
    __tablename__ = 'peak-load'

    building_id: Mapped[int] = mapped_column(Integer,
                                             primary_key=True)
    max_elec_consumption_kwh: Mapped[float]
    timestamp: Mapped[datetime.datetime] = mapped_column(sqlalchemy.DateTime())
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
    occupant_density_m_2 = mapped_column(Float, nullable=True)


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


class StateCostFactors(Base):
    __tablename__ = 'state_cost_factors'

    State: Mapped[int] = mapped_column(String, primary_key=True)
    Factor: Mapped[float]


def create_lookup_tables(db_context, database='x-stock'):
    """Create lookup tables using provided database context."""
    engine = db_context.get_engine(database)
    Base.metadata.create_all(engine, checkfirst=True)
    logger.info('Created lookup tables')


def query_to_df(query, engine, params=None):
    """
    Run a raw sql query and return the result as a dataframe
    
    Args:
        query: SQL query string or SQLAlchemy query object
        engine: SQLAlchemy engine instance (required)
        params: Query parameters
    """
    try:
        sql, params = query.sql()
    except AttributeError:
        sql = query
        params = params
    try:
        data = pd.read_sql_query(sql=sql, con=engine, params=params)
        return data
    except sqlalchemy.exc.OperationalError as e:
        import time
        attempts = 10
        for attempt in range(attempts):
            logger.error(e)
            logger.error('Unable to reach the DB. It may be paused. Attempting db connection again. '
                         f'Attempt: {attempt} of {attempts}')
            time.sleep(6)
            try:
                data = pd.read_sql_query(sql=sql,
                                         con=engine,
                                         params=params)
                return data
            except sqlalchemy.exc.OperationalError as error:
                logger.debug(error)
                continue

        raise ConnectionError("Connection to the database cannot be established. "
                              "Please try refreshing the page.")


if __name__ == '__main__':
    # create_lookup_tables()
    from sqlalchemy import select

    local_db_context = DatabaseContext(mode='local')
    pnnl_db_context = DatabaseContext(mode='PNNL database')

    q = select(PeakLoad).where(PeakLoad.upgrade == 0,
                                   PeakLoad.residential == 1,
                                   PeakLoad.state == 'PA')

    df_local = query_to_df(q, engine=local_db_context.get_engine())
    df_pnnl = query_to_df(q, engine=pnnl_db_context.get_engine())

    df_pnnl.sort_values('building_id', inplace=True)
    df_local.sort_values('building_id', inplace=True)

    df_pnnl.reset_index(drop=True, inplace=True)
    df_local.reset_index(drop=True, inplace=True)

    assert df_pnnl.equals(df_local)

