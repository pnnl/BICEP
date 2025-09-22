"""
Simple local database table mirroring utilities.

This module provides basic functionality to save and load data locally in CSV format
for BICEP to operate in local mode without requiring database connectivity.
"""

import pandas as pd
from pathlib import Path
import logging
from utils.config import PARSED_INPUTS_PATH

logger = logging.getLogger(__name__)


def save_db_query_results(query_name, data):
    """
    Save database query results to local CSV for later use in local mode.
    
    Args:
        query_name (str): Name for the saved file
        data (pd.DataFrame): Query results from database
    """
    output_path = PARSED_INPUTS_PATH / f'{query_name}.csv'
    
    data.to_csv(output_path, index=False)
    logger.info(f"Saved {len(data)} records to {output_path}")
    return output_path


def load_db_mirror_data(query_name):
    """
    Load previously saved database query results for local mode processing.
    
    Args:
        query_name (str): Name of the saved file to load
        
    Returns:
        pd.DataFrame: Previously saved database results
    """
    csv_path = PARSED_INPUTS_PATH / f'{query_name}.csv'
    
    if not csv_path.exists():
        logger.warning(f"Local mirror file not found: {csv_path}")
        return pd.DataFrame()
    
    data = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(data)} records from {csv_path}")
    return data


def clear_parsed_inputs():
    """Clear all files in the parsed_inputs directory."""
    if PARSED_INPUTS_PATH.exists():
        for file in PARSED_INPUTS_PATH.glob('*.csv'):
            file.unlink()
        for file in PARSED_INPUTS_PATH.glob('*.json'):
            file.unlink()
        logger.info("Cleared all parsed input files")
    else:
        logger.info("Parsed inputs directory does not exist")


def save_all_db_tables(mode='local', scenario=None):
    """
    Save all database tables to local CSV files for comparison.
    
    Args:
        mode (str): 'local' or 'database' - determines data source
        scenario (str): scenario name for adoption forecasts (bau/high)
    """
    
    if mode == 'database':
        # Save tables from actual database
        logger.info("Saving database tables to local files...")
        save_database_tables(scenario)
    else:
        # Save equivalent tables from local processing
        logger.info("Saving local processing results as database table equivalents...")
        save_local_tables_equivalent(scenario)


def save_database_tables(scenario, mode='local'):
    """Save actual database tables to CSV files with mode-specific naming.
    
    Args:
        scenario: The scenario to save (e.g., 'bau', 'high')
        mode: The mode being used ('local' or 'database')
            - 'local': creates files with 'local_' prefix
            - 'database': creates files without prefix (direct database exports)
    """
    try:
        from utils.db_models import (
            AdoptionForecasts, Technologies, TechMapping, 
            PeakLoad, LoadDifference, StockMeta, Upgrades,
            query_to_df
        )
        from sqlalchemy import select
        logger.info(f"Database modules imported successfully for {mode} mode")
    except ImportError as e:
        logger.error(f"Failed to import database modules: {e}")
        return
    
    # Determine file prefix based on mode
    mode_prefix = f'{mode}_' if mode == 'local' else ''
    
    try:
        logger.info(f"Saving {mode} mode database tables for scenario: {scenario}")
        
        # Save adoption forecasts for specific scenario
        if scenario:
            logger.info(f"Querying adoption forecasts for scenario: {scenario}")
            adoption_data = query_to_df(
                select(AdoptionForecasts).where(AdoptionForecasts.scenario == scenario)
            )
            filename = f'{mode_prefix}adoption_forecasts_{scenario}'
            save_db_query_results(filename, adoption_data)
            logger.info(f"✓ Saved {len(adoption_data)} rows to {filename}.csv")
        
        # Save reference tables (scenario-independent)
        reference_tables = [
            ('Technologies', Technologies, 'technologies'),
            ('TechMapping', TechMapping, 'scout_xstock_tech_mapping'),
            ('PeakLoad', PeakLoad, 'peak_load'),
            ('LoadDifference', LoadDifference, 'load_diff'),
            ('StockMeta', StockMeta, 'stock_meta'),
            ('Upgrades', Upgrades, 'upgrades')
        ]
        
        for table_name, model_class, file_suffix in reference_tables:
            logger.info(f"Querying {table_name} table")
            try:
                table_data = query_to_df(select(model_class))
                filename = f'{mode_prefix}{file_suffix}'
                save_db_query_results(filename, table_data)
                logger.info(f"✓ Saved {len(table_data)} rows to {filename}.csv")
            except Exception as table_error:
                logger.error(f"✗ Failed to save {table_name}: {table_error}")
                # Create empty file as placeholder
                filename = f'{mode_prefix}{file_suffix}'
                save_db_query_results(filename, pd.DataFrame())
                logger.warning(f"Created empty placeholder: {filename}.csv")
        
        logger.info(f"✓ {mode.title()} mode database tables saved successfully for scenario: {scenario}")
        
    except Exception as e:
        logger.error(f"✗ Failed to save {mode} mode database tables: {e}")
        logger.error(f"Error details: {str(e)}")
        
        # Create empty template files so pipeline doesn't fail
        if scenario:
            save_db_query_results(f'{mode_prefix}adoption_forecasts_{scenario}', pd.DataFrame())
        
        template_files = ['technologies', 'scout_xstock_tech_mapping', 'peak_load', 
                         'load_diff', 'stock_meta', 'upgrades']
        for file_suffix in template_files:
            save_db_query_results(f'{mode_prefix}{file_suffix}', pd.DataFrame())
        
        logger.warning(f"Created empty template files for {mode} mode")


def save_local_tables_equivalent(scenario):
    """Save local processing results in database table equivalent format."""
    # This will create equivalent table files from local processing
    # For now, we'll save what we have and expand as needed
    
    try:
        # Save state cost factors (static reference data)
        cost_factors = get_state_cost_factors_local()
        if not cost_factors.empty:
            save_db_query_results('state_cost_factors', cost_factors)
        
        logger.info(f"Local table equivalents saved for scenario: {scenario}")
        
    except Exception as e:
        logger.error(f"Failed to save local table equivalents: {e}")


def get_state_cost_factors_local():
    """Load state cost factors from local file."""
    cost_factor_path = Path(__file__).parent.parent / 'data' / 'required_input' / 'cost_factor.csv'
    if cost_factor_path.exists():
        logger.info(f"Loaded state cost factors from local file: {cost_factor_path}")
        return pd.read_csv(cost_factor_path)
    else:
        logger.warning(f"State cost factors file not found: {cost_factor_path}")
        return pd.DataFrame()