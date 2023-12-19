"""
This module defines the access to the bicep database.

SQLAlchemy is the ORM used to interact with the database.

User credentials are stored in ./utils/sensitive_config.py. 
"""

import datetime

from loguru import logger

import sqlalchemy
from sqlalchemy.pool import NullPool

from sqlalchemy.types import Integer
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


def create_lookup_tables(database='x-stock'):
    Base.metadata.create_all(engines[database], checkfirst=True)
    logger.info('Created lookup tables')


if __name__ == '__main__':
    create_lookup_tables()
