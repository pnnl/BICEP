"""
Migration script to export MS SQL database tables to local files (SQLite or parquet).
"""
import sqlite3
import pandas as pd
from pathlib import Path
from loguru import logger
import sqlalchemy

from utils.sensitive_config import sql_server_admin, sql_server_pass
from utils.config import DATA_DIR

# Configuration
SQLITE_OUTPUT_PATH = DATA_DIR / 'bicep.x-stock.db'
dialect_driver = 'mssql+pymssql'
user_creds = f'{sql_server_admin}:{sql_server_pass}'
host_port = 'bicep-sql-server.database.windows.net:1433'
database = 'x-stock'

# Tables to migrate
TABLES_TO_MIGRATE = [
    'peak-load',
    'load-diff',
    'stock-meta',
    'technologies',
    'scout-xstock-tech-mapping',
    'adoption-forecasts',
    'upgrades',
    'state_cost_factors',  # from get_state_cost_factors query
    'distpvcap_stscen2023_mid_case',  # from get_new_pv_data
    'CountyHierarchy'  # from get_new_pv_data
]


def create_mssql_engine():
    """Create MS SQL engine"""
    database_url = f'{dialect_driver}://{user_creds}@{host_port}/{database}'
    return sqlalchemy.create_engine(database_url)


def create_sqlite_engine():
    """Create SQLite engine"""
    database_url = f'sqlite:///{SQLITE_OUTPUT_PATH}'
    return sqlalchemy.create_engine(database_url)


def get_table_schema(table_name, mssql_engine):
    """Get column info for the table"""
    schema_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    try:
        return pd.read_sql(schema_query, mssql_engine)
    except:
        logger.warning(f"Could not get schema for {table_name}")
        return None


def migrate_table(table_name, mssql_engine, sqlite_engine):
    """Migrate a single table from MS SQL to SQLite"""
    try:
        logger.info(f"Migrating table: {table_name}")

        query = f"SELECT * FROM [{table_name}]"  # Square brackets for hyphenated names

        # Read from MS SQL
        df = pd.read_sql(query, mssql_engine)
        logger.info(f"  - Read {len(df)} rows from {table_name}")

        # Write to SQLite
        df.to_sql(table_name, sqlite_engine, if_exists='replace', index=False)
        logger.info(f"  - Wrote to SQLite as {table_name.replace('-', '_')}")

        return True

    except Exception as e:
        logger.error(f"Failed to migrate {table_name}: {e}")
        return False


def save_table_as_parquet(table_name, mssql_engine):
    """Migrate a single table from MS SQL to Parquet"""
    try:
        logger.info(f"Migrating table: {table_name}")

        query = f"SELECT * FROM [{table_name}]"  # Square brackets for hyphenated names

        # Read from MS SQL
        df = pd.read_sql(query, mssql_engine)
        logger.info(f"  - Read {len(df)} rows from {table_name}")

        file_path = DATA_DIR / f'{table_name}.parquet'

        # Write to SQLite
        df.to_parquet(file_path)
        logger.info(f"  - Wrote to parquet to: {file_path}")

        return True

    except Exception as e:
        logger.error(f"Failed to migrate {table_name}: {e}")
        return False


def main(migrate_target='sqlite'):
    """Main migration function"""
    logger.info(f"Starting database migration from MS SQL to {migrate_target}")

    # Create engines
    mssql_engine = create_mssql_engine()

    # Test connections
    try:
        with mssql_engine.connect() as conn:
            logger.info("✓ Connected to MS SQL Server")
    except Exception as e:
        logger.error(f"✗ Failed to connect to MS SQL: {e}")
        return

    # Migrate each table
    successful_migrations = 0

    if migrate_target == 'sqlite':
        logger.info(f"Output SQLite file: {SQLITE_OUTPUT_PATH}")
        sqlite_engine = create_sqlite_engine()
        try:
            with sqlite_engine.connect() as conn:
                logger.info("✓ Connected to SQLite")
        except Exception as e:
            logger.error(f"✗ Failed to connect to SQLite: {e}")
            return

        for table_name in TABLES_TO_MIGRATE:
            if migrate_table(table_name, mssql_engine, sqlite_engine):
                successful_migrations += 1

        logger.info(f"Migration complete! {successful_migrations}/{len(TABLES_TO_MIGRATE)} tables migrated successfully")

        # Show final file size
        if Path(SQLITE_OUTPUT_PATH).exists():
            file_size = Path(SQLITE_OUTPUT_PATH).stat().st_size / (1024 * 1024)  # MB
            logger.info(f"SQLite file size: {file_size:.1f} MB")

    elif migrate_target == 'parquet':
        logger.info(f"Output Parquet files saved to: {DATA_DIR}")

        for table_name in TABLES_TO_MIGRATE:
            if save_table_as_parquet(table_name, mssql_engine):
                successful_migrations += 1

        logger.info(
            f"Migration complete! {successful_migrations}/{len(TABLES_TO_MIGRATE)} tables migrated successfully")

    else:
        raise ValueError(f"Unknown migration target: {migrate_target}")


if __name__ == '__main__':
    main(migrate_target='parquet')